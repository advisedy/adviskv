#include "sdm/background/table_reconciler.h"

#include <fmt/format.h>

#include <algorithm>
#include <cassert>

#include "common/defer.h"
#include "common/define.h"
#include "common/func.h"
#include "common/log.h"
#include "common/status.h"
#include "common/type.h"
#include "sdm/model/store.h"
#include "sdm/utility/enum_convert.h"

namespace adviskv::sdm {

std::vector<PeerMember> TableReconciler::build_members(
    const std::vector<NodePtr>& nodes, TableID table_id,
    ShardIndex shard_index) {
    std::vector<PeerMember> members;
    members.reserve(nodes.size());
    for (ReplicaIndex replica_index = 0;
         replica_index < static_cast<ReplicaIndex>(nodes.size());
         ++replica_index) {
        const NodePtr& node = nodes[replica_index];
        members.push_back(PeerMember{
            .node_id = node->id,
            .replica_id = ReplicaID{.table_id = table_id,
                                    .shard_index = shard_index,
                                    .replica_index = replica_index},
            .endpoint = node->state.endpoint,
        });
    }
    return members;
}

Status TableReconciler::build_replicas(Table& table, ShardIndex shard_index,
                                       const std::vector<NodePtr>& nodes,
                                       const std::vector<PeerMember>& members) {
    for (ReplicaIndex replica_index = 0;
         replica_index < table.spec.replica_count; ++replica_index) {
        const NodePtr& node = nodes[replica_index];
        Replica replica{
            .replica_id = ReplicaID{.table_id = table.table_id,
                                    .shard_index = shard_index,
                                    .replica_index = replica_index},
            .spec =
                ReplicaSpec{
                    .dc = node->spec.dc,
                    .assign_node_id = node->id,
                    .engine_type = EngineType::MAP,
                    .members = members,
                },
            .state = ReplicaState{.desired = ReplicaDesired::PRESENT,
                                  .phase = ReplicaPhase::PENDING,
                                  .observed_role = ReplicaRole::FOLLOWER,
                                  .observed_endpoint = node->state.endpoint,
                                  .update_ts = func::get_current_ts_ms(),
                                  .term = 0},
        };
        RETURN_IF_INVALID_STATUS(store_->put_replica(replica))
    }
    return Status::OK();
}

Status TableReconciler::get_assigned_node_endpoint(const Replica& replica,
                                                   Endpoint& endpoint) const {
    NodePtr node;
    RETURN_IF_INVALID_STATUS(
        store_->get_node(replica.spec.assign_node_id, node))

    RETURN_IF_INVALID_CONDITION(
        node != nullptr, fmt::format("assigned node not found, node_id={}F",
                                     replica.spec.assign_node_id))

    endpoint = node->state.endpoint;
    return Status::OK();
}

TableReconciler::TableReconciler(SdmStore* store,
                                 IStorageClient* storage_client,
                                 NodeSelector* selector)
    : store_(store), storage_client_(storage_client), selector_(selector) {}

void TableReconciler::run() {
    Status status = reconcile_once();
    if (status.fail()) {
        LOG_WARN("table reconciler run failed, msg={}", status.msg());
    }
}

// 列出来当前store的每一个table，然后去进行处理
Status TableReconciler::reconcile_once() {
    RETURN_IF_INVALID_CONDITION(store_ != nullptr, "store is nullptr")
    std::vector<TablePtr> tables;
    RETURN_IF_INVALID_STATUS(store_->list_tables(tables))
    for (const TablePtr& table : tables) {
        if (!table) continue;
        Status status = reconcile_table(*table);
        if (status.fail()) {
            LOG_WARN("reconcile table failed, table_id={}, msg={}",
                     table->table_id, status.msg());
        }
    }
    return Status::OK();
}

Status TableReconciler::reconcile_table(Table& table) {
    switch (table.state.desired) {
        case TableDesired::PRESENT:
            return reconcile_present(table);
        case TableDesired::ABSENT:
            return reconcile_absent(table);
    }
    LOG_WARN("unknown table desired state");
    return mark_table_error(table,
                            Status::ERROR("unknown table desired state"));
}

Status TableReconciler::reconcile_present(Table& table) {
    Status status = ensure_replica_metadata(table);
    if (status.fail()) return mark_table_error(table, status);
    status = ensure_storage_replicas(table);
    if (status.fail()) return mark_table_error(table, status);

    status = refresh_storage_replica_info(table);
    if (status.fail()) {
        LOG_DEBUG("refresh storage replica info skipped for table={}, msg={}",
                  table.table_id, status.msg());
    }

    if (!all_replicas_ready(table) or !all_routes_ready(table)) {
        table.state.phase = TablePhase::CREATING;
        table.state.update_ts = func::get_current_ts_ms();
        return store_->put_table(table);
    }

    table.state.phase = TablePhase::READY;
    table.state.last_error_msg.clear();
    table.state.update_ts = func::get_current_ts_ms();
    return store_->put_table(table);
}

Status TableReconciler::reconcile_absent(Table& table) {
    Status status = ensure_routes_absent(table);
    if (status.fail()) return mark_table_error(table, status);
    status = ensure_storage_replicas_absent(table);
    if (status.fail()) return mark_table_error(table, status);
    status = ensure_replica_metadata_absent(table);
    if (status.fail()) return mark_table_error(table, status);

    table.state.phase = TablePhase::DELETED;
    table.state.last_error_msg.clear();
    table.state.update_ts = func::get_current_ts_ms();
    return store_->put_table(table);
}

Status TableReconciler::ensure_replica_metadata(Table& table) {
    RETURN_IF_INVALID_CONDITION(selector_ != nullptr, "selector is nullptr")
    for (ShardIndex shard_index = 0; shard_index < table.spec.shard_count;
         ++shard_index) {
        ShardID shard_id{.table_id = table.table_id,
                         .shard_index = shard_index};
        {
            std::vector<ReplicaPtr> replicas;
            RETURN_IF_INVALID_STATUS(
                store_->list_replicas_by_shard(shard_id, replicas))

            if (!replicas.empty()) {
                RETURN_IF_INVALID_CONDITION(
                    (int32)replicas.size() == table.spec.replica_count,
                    "replica put error: size is not enough");
                continue;
            }
        }

        //   选出来nodes ， 然后构造出来members， 然后放到replicas
        TablePlacementResult placement;
        RETURN_IF_INVALID_STATUS(selector_->select_table_nodes(
            PlaceNodesParam{.resource_pool = table.spec.resource_pool,
                            .shard_count = 1,
                            .replica_count = table.spec.replica_count},
            placement))

        const std::vector<NodePtr>& nodes = placement.shards.front().nodes;

        std::vector<PeerMember> members =
            build_members(nodes, table.table_id, shard_index);

        RETURN_IF_INVALID_STATUS(
            build_replicas(table, shard_index, nodes, members))

        std::vector<ReplicaPtr> replicas;
        RETURN_IF_INVALID_STATUS(
            store_->list_replicas_by_shard(shard_id, replicas))

        RETURN_IF_INVALID_CONDITION(
            (int32)replicas.size() == table.spec.replica_count,
            "replica put error: size is not enough");
    }
    return Status::OK();
}

Status TableReconciler::ensure_storage_replicas(Table& table) {
    RETURN_IF_INVALID_CONDITION(storage_client_ != nullptr,
                                "storage_client is nullptr")
    for (ShardIndex shard_index = 0; shard_index < table.spec.shard_count;
         ++shard_index) {
        std::vector<ReplicaPtr> replicas;
        RETURN_IF_INVALID_STATUS(store_->list_replicas_by_shard(
            ShardID{.table_id = table.table_id, .shard_index = shard_index},
            replicas))
        for (const ReplicaPtr& replica : replicas) {
            if (!replica or replica->state.desired != ReplicaDesired::PRESENT) {
                continue;
            }
            if (replica->state.phase == ReplicaPhase::READY) {
                continue;
            }
            // 关于这里如果是CREATING的话，
            // 说不定是storage那边发过来了创建好了， 但是我们这里没有收到，
            // 所以就再发送一遍，在storage那边保持好幂等应该就可以了。
            Endpoint endpoint;
            RETURN_IF_INVALID_STATUS(
                get_assigned_node_endpoint(*replica, endpoint))
            Status status = storage_client_->create_replica(CreateReplicaParam{
                .replica_id = replica->replica_id,
                .engine_type = replica->spec.engine_type,
                .members = replica->spec.members,
                .endpoint = endpoint,
            });
            if (status.fail()) {
                replica->state.last_error_msg = status.to_string();
                replica->state.update_ts = func::get_current_ts_ms();
                IGNORE_RESULT(store_->put_replica(*replica));
                return status;
            }
            replica->state.phase = ReplicaPhase::CREATING;
            replica->state.last_error_msg.clear();
            replica->state.update_ts = func::get_current_ts_ms();
            RETURN_IF_INVALID_STATUS(store_->put_replica(*replica))
        }
    }
    return Status::OK();
}

Status TableReconciler::refresh_storage_replica_info(Table& table) {
    RETURN_IF_INVALID_CONDITION(storage_client_ != nullptr,
                                "storage_client is nullptr")
    for (ShardIndex shard_index = 0; shard_index < table.spec.shard_count;
         ++shard_index) {
        std::vector<ReplicaPtr> replicas;
        RETURN_IF_INVALID_STATUS(store_->list_replicas_by_shard(
            ShardID{.table_id = table.table_id, .shard_index = shard_index},
            replicas))
        for (const ReplicaPtr& replica : replicas) {
            if (!replica || replica->state.desired != ReplicaDesired::PRESENT) {
                continue;
            }
            StorageReplicaInfo info;
            bool exists{false};
            Endpoint endpoint;
            RETURN_IF_INVALID_STATUS(
                get_assigned_node_endpoint(*replica, endpoint))
            RETURN_IF_INVALID_STATUS(storage_client_->get_replica_info(
                GetReplicaInfoParam{
                    .replica_id = replica->replica_id,
                    .endpoint = endpoint,
                },
                info, exists))
            if (!exists) {
                continue;
            }
            /*
                ReplicaDesired desired{ReplicaDesired::PRESENT};
                ReplicaPhase phase{ReplicaPhase::PENDING};
                ReplicaRole observed_role{ReplicaRole::FOLLOWER};
                Endpoint observed_endpoint;
                std::string last_error_msg;
                int64_t update_ts{0};
            */
            replica->state.observed_role = info.role;
            replica->state.observed_endpoint = info.endpoint;
            RETURN_IF_INVALID_CONDITION(convert_replica_status_to_phase(
                                            info.status, replica->state.phase),
                                        "replica status is not valid")
            replica->state.update_ts = func::get_current_ts_ms();
            replica->state.last_error_msg.clear();
            replica->state.term = info.term;
            RETURN_IF_INVALID_STATUS(store_->put_replica(*replica))
        }
    }
    return Status::OK();
}

Status TableReconciler::ensure_routes_absent(const Table& table) {
    for (ShardIndex shard_index = 0; shard_index < table.spec.shard_count;
         ++shard_index) {
        RETURN_IF_INVALID_STATUS(store_->delete_shard_route(
            ShardID{.table_id = table.table_id, .shard_index = shard_index}))
    }
    return Status::OK();
}

Status TableReconciler::ensure_storage_replicas_absent(const Table& table) {
    RETURN_IF_INVALID_CONDITION(storage_client_ != nullptr,
                                "storage_client is nullptr")
    for (ShardIndex shard_index = 0; shard_index < table.spec.shard_count;
         ++shard_index) {
        std::vector<ReplicaPtr> replicas;
        RETURN_IF_INVALID_STATUS(store_->list_replicas_by_shard(
            ShardID{.table_id = table.table_id, .shard_index = shard_index},
            replicas))
        for (const ReplicaPtr& replica : replicas) {
            if (!replica) continue;
            Endpoint endpoint;
            RETURN_IF_INVALID_STATUS(
                get_assigned_node_endpoint(*replica, endpoint))
            Status status = storage_client_->delete_replica(DeleteReplicaParam{
                .replica_id = replica->replica_id,
                .endpoint = endpoint,
            });
            if (status.fail()) {
                replica->state.phase = ReplicaPhase::DELETING;
                replica->state.last_error_msg = status.to_string();
                replica->state.update_ts = func::get_current_ts_ms();
                IGNORE_RESULT(store_->put_replica(*replica));
                return status;
            }
            replica->state.phase = ReplicaPhase::DELETED;
            replica->state.update_ts = func::get_current_ts_ms();
            RETURN_IF_INVALID_STATUS(store_->put_replica(*replica))
        }
    }
    return Status::OK();
}

Status TableReconciler::ensure_replica_metadata_absent(const Table& table) {
    for (ShardIndex shard_index = 0; shard_index < table.spec.shard_count;
         ++shard_index) {
        std::vector<ReplicaPtr> replicas;
        RETURN_IF_INVALID_STATUS(store_->list_replicas_by_shard(
            ShardID{.table_id = table.table_id, .shard_index = shard_index},
            replicas))
        for (const ReplicaPtr& replica : replicas) {
            if (!replica) continue;
            RETURN_IF_INVALID_STATUS(store_->del_replica(replica->replica_id))
        }
    }
    return Status::OK();
}

bool TableReconciler::all_replicas_ready(const Table& table) {
    for (ShardIndex shard_index = 0; shard_index < table.spec.shard_count;
         ++shard_index) {
        std::vector<ReplicaPtr> replicas;
        Status status = store_->list_replicas_by_shard(
            ShardID{.table_id = table.table_id, .shard_index = shard_index},
            replicas);
        if (status.fail() ||
            static_cast<int32_t>(replicas.size()) != table.spec.replica_count) {
            return false;
        }
        for (const ReplicaPtr& replica : replicas) {
            if (!replica || replica->state.phase != ReplicaPhase::READY) {
                return false;
            }
        }
    }
    return true;
}

bool TableReconciler::all_routes_ready(const Table& table) {
    for (ShardIndex shard_index = 0; shard_index < table.spec.shard_count;
         ++shard_index) {
        ShardRoutePtr route;
        Status status = store_->get_shard_route(
            ShardID{.table_id = table.table_id, .shard_index = shard_index},
            route);
        if (status.fail() || !route || route->replicas.empty()) return false;
        int leader_count = 0;
        for (const RouteEntry& entry : route->replicas) {
            leader_count += (entry.role == ReplicaRole::LEADER);
        }
        if (leader_count < 1) return false;
        // 对于大于等于1的情况，我们在route_updater那边判断，这里只是判断一下是否准备好了
    }
    return true;
}

Status TableReconciler::mark_table_error(Table& table, const Status& status) {
    table.state.phase = TablePhase::FAILED;
    table.state.last_error_msg = status.to_string();
    table.state.update_ts = func::get_current_ts_ms();
    return store_->put_table(table);
}

}  // namespace adviskv::sdm
