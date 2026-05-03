#include "sdm/service/table_service.h"

#include <fmt/format.h>

#include "common/define.h"
#include "common/log.h"
#include "common/status.h"
#include "sdm/model/store.h"
#include "storage.pb.h"

namespace adviskv::sdm {

TableService::TableService(SdmStore* sdm_store, StorageClient* storage_client,
                           NodeSelector* node_selector)
    : sdm_store_(sdm_store),
      storage_client_(storage_client),
      node_selector_(node_selector) {}

Status TableService::place_table(const PlaceTableParam& param) {
    RETURN_IF_INVALID_PARAM(param)

    Table table{
        .table_id = param.table_id,
        .spec{
            .table_name = param.table_name,
            .db_name = param.db_name,
            .db_id = param.db_id,
            .replica_count = param.replica_count,
            .shard_count = param.shard_count,
            .resource_pool = param.resource_pool,
        },
        .state{.status = TableStatus::CREATEING},
    };
    Status status = sdm_store_->put_table(table);
    RETURN_IF_INVALID_STATUS(status)

    std::vector<NodePtr> pool_nodes;
    status = sdm_store_->list_nodes_by_resource_pool(param.resource_pool,
                                                     pool_nodes);
    RETURN_IF_INVALID_STATUS(status)

    RETURN_IF_INVALID_CONDITION(
        static_cast<int32_t>(pool_nodes.size()) >= param.replica_count,
        fmt::format(
            "not enough nodes in resource_pool '{}', need {} but have {}",
            param.resource_pool, param.replica_count, pool_nodes.size()))

    for (int32_t shard_idx = 0; shard_idx < param.shard_count; ++shard_idx) {
        std::vector<NodePtr> selected_nodes;
        status = node_selector_->select_nodes(pool_nodes, param.replica_count,
                                              selected_nodes);
        RETURN_IF_INVALID_STATUS(status)

        std::vector<Replica> shard_replicas;
        shard_replicas.reserve(selected_nodes.size());
        for (int32_t rep_idx = 0;
             rep_idx < static_cast<int32_t>(selected_nodes.size()); ++rep_idx) {
            const NodePtr& node = selected_nodes[rep_idx];
            Replica replica{
                .replica_id{.table_id = param.table_id,
                            .shard_index = shard_idx,
                            .replica_index = rep_idx},
                .spec{
                    .dc = node->spec.dc,
                    .assign_node_id = node->id,
                    .role = (rep_idx == 0) ? ReplicaRole::LEADER
                                           : ReplicaRole::FOLLOWER,
                    .status = ReplicaStatus::PENDING,
                },
                .state{
                    .endpoint = node->state.endpoint,
                    .role = ReplicaRole::FOLLOWER,
                },
            };
            shard_replicas.push_back(replica);
        }

        for (const Replica& replica : shard_replicas) {
            status = sdm_store_->put_replica(replica);
            RETURN_IF_INVALID_STATUS(status)
        }

        rpc::CreateReplicaRequest cr_req;
        cr_req.set_table_id(param.table_id);
        cr_req.set_shard_index(shard_idx);
        cr_req.set_engine_type(static_cast<int32_t>(EngineType::MAP));

        for (const Replica& replica : shard_replicas) {
            auto* member = cr_req.add_members();
            member->set_node_id(replica.spec.assign_node_id);
            auto* rid = member->mutable_replica_id();
            rid->set_table_id(replica.replica_id.table_id);
            rid->set_shard_index(replica.replica_id.shard_index);
            rid->set_replica_index(replica.replica_id.replica_index);
            auto* ep = member->mutable_endpoint();
            ep->set_ip(replica.state.endpoint.ip);
            ep->set_port(replica.state.endpoint.port);
        }

        for (const Replica& replica : shard_replicas) {
            cr_req.set_replica_index(replica.replica_id.replica_index);
            rpc::CreateReplicaResponse cr_resp;
            NodePtr node;
            status = sdm_store_->get_node(replica.spec.assign_node_id, node);
            RETURN_IF_INVALID_STATUS(status)
            RETURN_IF_INVALID_CONDITION(node != nullptr, "node not found")

            status = storage_client_->create_replica(node->state.endpoint.ip,
                                                     node->state.endpoint.port,
                                                     cr_req, cr_resp);
            if (status.fail()) {
                LOG_WARN(
                    "CreateReplica failed for node={}, table={}, shard={}, "
                    "replica={}, msg={}",
                    replica.spec.assign_node_id, param.table_id, shard_idx,
                    replica.replica_id.replica_index, status.msg());
                continue;
            }

            Replica updating = replica;
            updating.spec.status = ReplicaStatus::ADDING;
            status = sdm_store_->put_replica(updating);
            RETURN_IF_INVALID_STATUS(status)
        }
    }

    return Status::OK();
}

}  // namespace adviskv::sdm