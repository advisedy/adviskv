#include "sdm/workflow/placetable_workflow.h"

#include <fmt/format.h>

#include "common/define.h"
#include "common/func.h"
#include "common/log.h"
#include "common/status.h"
#include "common/type.h"
#include "sdm/model/service_param.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {

PlaceTableWorkflow::PlaceTableWorkflow(SdmStore* store, StorageClient* client,
                                       NodeSelector* selector)
    : store_(store), client_(client), selector_(selector) {}

Status PlaceTableWorkflow::step(Table& table) {
    switch (table.state.lifecycle) {
        case TableLifecycle::CREATING:
            return step_creating(table);
        case TableLifecycle::PLACING:
            return step_placing(table);
        case TableLifecycle::CREATING_REPLICAS:
            return step_creating_replicas(table);
        case TableLifecycle::WAITING_READY:
            return step_waiting_ready(table);
        case TableLifecycle::WAITING_ROUTE_READY:
            return step_waiting_route_ready(table);
        case TableLifecycle::ROLLING_BACK:
            return step_rolling_back(table);
        case TableLifecycle::READY:
        case TableLifecycle::FAILED:
            return Status::OK();
    }
    return Status::OK();
}

Status PlaceTableWorkflow::step_creating(Table& table) {
    TablePtr existing;
    Status status = store_->get_table_by_name(table.spec.db_name,
                                              table.spec.table_name, existing);
    if (status.fail() && status.code() != StatusCode::TABLE_NOT_FOUND) {
        return status;
    }
    if (existing != nullptr && existing->table_id != table.table_id) {
        switch (existing->state.lifecycle) {
            case TableLifecycle::READY:
                return Status::ALREADY_EXIST(
                    fmt::format("table {}.{} already exists",
                                table.spec.db_name, table.spec.table_name));
            case TableLifecycle::ROLLING_BACK:
                return Status::ERROR(fmt::format("table {}.{} is rolling back",
                                                 table.spec.db_name,
                                                 table.spec.table_name));
            case TableLifecycle::FAILED:
                return Status::ERROR(
                    fmt::format("table {}.{} is in failed state",
                                table.spec.db_name, table.spec.table_name));
            default:
                return Status::ALREADY_EXIST(
                    fmt::format("table {}.{} is being created",
                                table.spec.db_name, table.spec.table_name));
        }
    }

    status = store_->put_table(table);
    RETURN_IF_INVALID_STATUS(status)
    return transition(table, TableLifecycle::PLACING);
}

Status PlaceTableWorkflow::step_placing(Table& table) {
    std::vector<NodePtr> pool_nodes;
    Status status = store_->list_nodes_by_resource_pool(
        table.spec.resource_pool, pool_nodes);
    if (status.fail()) {
        return transition(table, TableLifecycle::ROLLING_BACK, status.msg());
    }

    if ((int32)pool_nodes.size() < table.spec.replica_count) {
        return transition(
            table, TableLifecycle::ROLLING_BACK,
            fmt::format(
                "not enough nodes in resource_pool '{}', need {} but have {}",
                table.spec.resource_pool, table.spec.replica_count,
                pool_nodes.size()));
    }

    for (int32_t shard_idx = 0; shard_idx < table.spec.shard_count;
         ++shard_idx) {
        std::vector<NodePtr> selected_nodes;
        status = selector_->select_nodes(pool_nodes, table.spec.replica_count,
                                         selected_nodes);
        if (status.fail()) {
            return transition(table, TableLifecycle::ROLLING_BACK,
                              status.msg());
        }

        for (int32 rep_idx = 0, selected_nodes_size = selected_nodes.size();
             rep_idx < selected_nodes_size; ++rep_idx) {
            const NodePtr& node = selected_nodes[rep_idx];
            Replica replica{
                .replica_id{.table_id = table.table_id,
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
            status = store_->put_replica(replica);
            if (status.fail()) {
                return transition(table, TableLifecycle::ROLLING_BACK,
                                  status.msg());
            }
        }
    }

    return transition(table, TableLifecycle::CREATING_REPLICAS);
}

Status PlaceTableWorkflow::step_creating_replicas(Table& table) {
    bool all_done = true;

    for (int32 shard_idx = 0; shard_idx < table.spec.shard_count; ++shard_idx) {
        ShardID shard_id{.table_id = table.table_id, .shard_index = shard_idx};
        std::vector<ReplicaPtr> replicas;
        Status status = store_->list_replicas_by_shard(shard_id, replicas);
        RETURN_IF_INVALID_STATUS(status)

        std::vector<PeerMember> members;
        {
            members.reserve(replicas.size());
            for (const ReplicaPtr& rep : replicas) {
                if (rep->spec.status == ReplicaStatus::PENDING) {
                    all_done = false;
                }
                if (rep == nullptr) {
                    continue;
                }
                members.push_back(PeerMember{
                    .node_id = rep->spec.assign_node_id,
                    .replica_id = rep->replica_id,
                    .endpoint = rep->state.endpoint,
                });
            }
        }

        for (const ReplicaPtr& rep : replicas) {
            if (rep->spec.status != ReplicaStatus::PENDING) {
                continue;
            }

            NodePtr node;
            status = store_->get_node(rep->spec.assign_node_id, node);
            if (status.fail() || !node) {
                if (is_retriable(status)) {
                    LOG_WARN(
                        "get_node retriable error for node={}, table={}, "
                        "msg={}",
                        rep->spec.assign_node_id, table.table_id, status.msg());
                    continue;
                }
                return transition(
                    table, TableLifecycle::ROLLING_BACK,
                    fmt::format("node {} not found", rep->spec.assign_node_id));
            }

            CreateReplicaParam param{
                .replica_id = rep->replica_id,
                .engine_type = EngineType::MAP,
                .members = members,
                .endpoint = node->state.endpoint,
            };
            status = client_->create_replica(param);
            if (status.fail()) {
                if (is_retriable(status)) {
                    LOG_WARN(
                        "CreateReplica retriable for node={}, table={}, "
                        "shard={}, rep={}, msg={}",
                        rep->spec.assign_node_id, table.table_id, shard_idx,
                        rep->replica_id.replica_index, status.msg());
                    continue;
                }
                return transition(
                    table, TableLifecycle::ROLLING_BACK,
                    fmt::format("CreateReplica fatal for table={}, msg={}",
                                table.table_id, status.msg()));
            }

            Replica updating = *rep;
            updating.spec.status = ReplicaStatus::ADDING;
            status = store_->put_replica(updating);
            RETURN_IF_INVALID_STATUS(status)
        }

        std::vector<ReplicaPtr> updated_replicas;
        status = store_->list_replicas_by_shard(shard_id, updated_replicas);
        RETURN_IF_INVALID_STATUS(status)
        for (const ReplicaPtr& r : updated_replicas) {
            if (r->spec.status == ReplicaStatus::PENDING) {
                all_done = false;
                break;
            }
        }
    }

    if (all_done) {
        return transition(table, TableLifecycle::WAITING_READY);
    }
    return Status::OK();
}

Status PlaceTableWorkflow::step_waiting_ready(Table& table) {
    for (int32_t shard_idx = 0; shard_idx < table.spec.shard_count;
         ++shard_idx) {
        ShardID shard_id{.table_id = table.table_id, .shard_index = shard_idx};
        std::vector<ReplicaPtr> replicas;
        Status status = store_->list_replicas_by_shard(shard_id, replicas);
        RETURN_IF_INVALID_STATUS(status)

        for (const ReplicaPtr& rep : replicas) {
            if (rep->spec.status != ReplicaStatus::READY) {
                return Status::OK();
            }
        }
    }

    return transition(table, TableLifecycle::WAITING_ROUTE_READY);
}

Status PlaceTableWorkflow::step_waiting_route_ready(Table& table) {
    for (int32_t shard_idx = 0; shard_idx < table.spec.shard_count;
         ++shard_idx) {
        ShardID shard_id{.table_id = table.table_id, .shard_index = shard_idx};
        ShardRoutePtr route;
        Status status = store_->get_shard_route(shard_id, route);
        RETURN_IF_INVALID_STATUS(status)
        if (route == nullptr || route->replicas.empty()) {
            return Status::OK();
        }
    }

    return transition(table, TableLifecycle::READY);
}

Status PlaceTableWorkflow::step_rolling_back(Table& table) {
    for (int32 shard_idx = 0; shard_idx < table.spec.shard_count; ++shard_idx) {
        ShardID shard_id{.table_id = table.table_id, .shard_index = shard_idx};
        Status status = store_->delete_shard_route(shard_id);
        if (status.fail()) {
            return transition(table, TableLifecycle::FAILED, status.msg());
        }

        std::vector<ReplicaPtr> replicas;
        status = store_->list_replicas_by_shard(shard_id, replicas);
        if (status.fail()) {
            return transition(table, TableLifecycle::FAILED, status.msg());
        }
        for (const ReplicaPtr& replica : replicas) {
            if (!replica) {
                continue;
            }
            status = store_->del_replica(replica->replica_id);
            if (status.fail()) {
                return transition(table, TableLifecycle::FAILED, status.msg());
            }
        }
    }

    return store_->delete_table(table.table_id);
}

Status PlaceTableWorkflow::transition(Table& table, TableLifecycle next,
                                      const std::string& error_msg) {
    table.state.lifecycle = next;
    table.state.last_transition_ts = get_current_ts_ms();
    if ((next == TableLifecycle::ROLLING_BACK ||
         next == TableLifecycle::FAILED) &&
        !error_msg.empty()) {
        table.state.last_error_msg = error_msg;
    }
    if (next == TableLifecycle::FAILED) {
        table.state.status = TableStatus::FAILED;
    } else if (next == TableLifecycle::READY) {
        table.state.status = TableStatus::READY;
        table.state.last_error_msg.clear();
    } else {
        table.state.status = TableStatus::CREATEING;
    }
    return store_->put_table(table);
}

bool PlaceTableWorkflow::is_retriable(const Status& status) const {
    return status.code() == StatusCode::ERROR;
}

}  // namespace adviskv::sdm
