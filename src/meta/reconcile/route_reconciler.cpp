#include "meta/reconcile/route_reconciler.h"

#include <algorithm>
#include <fmt/format.h>

#include "common/define.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "common/model/model.h"
#include "meta/store/meta_store.h"
#include "meta/store/meta_store_txn.h"

namespace adviskv::meta {

RouteReconciler::RouteReconciler(MetaStore* store) : store_(store) {}

Status RouteReconciler::run() {
    if (store_ == nullptr) {
        Status error{StatusCode::INVALID_ARGUMENT, "store is nullptr"};
        LOG_ERROR("[RouteReconciler] reconcile skipped, msg={}", error.msg());
        return error;
    }
    std::vector<Table> tables;
    Status status = store_->read_with([&](const MetaStoreTxn& txn) { return txn.list_tables(tables); });
    if (status.fail()) {
        LOG_WARN("[RouteReconciler] list tables failed, msg={}", status.msg());
        return status;
    }
    for (auto& table : tables) {
        for (const ShardRange& range : table.spec.shard_ranges) {
            ShardIndex i = range.shard_index;
            Status s = check_shard_route(table, to<ShardIndex>(i));
            if (s.fail()) {
                LOG_WARN(
                        "[RouteReconciler] route task update shard route failed, "
                        "table={}, shard={}, "
                        "msg={}",
                        table.table_id, i, s.msg());
            } else {
                LOG_DEBUG("[RouteReconciler] route ready, table_id={}, shard_index={}", table.table_id, i);
            }
        }
    }
    return Status::OK();
}

Status RouteReconciler::check_shard_route(const Table& table, ShardIndex shard_index) {
    ADVISKV_METRICS_TIMER("meta_route_check_shard_route");
    ADVISKV_METRICS_COUNTER("meta_route_check_shard_route_request");

    ShardID shard_id{table.table_id, shard_index};
    Status route_status{Status::OK()};

    Status status = store_->write_with([&](MetaStoreTxn& txn) -> Status {
        auto clear_route_and_return = [&](const std::string& msg) -> Status {
            route_status = Status::ROUTE_NOT_FOUND(msg);
            ADVISKV_METRICS_COUNTER("meta_route_delete_shard_route_request");
            RETURN_IF_INVALID_STATUS(txn.delete_shard_route(shard_id))
            LOG_WARN(
                    "[RouteReconciler] check_shard_route, clear_route_and_return, table_id={}, shard_index={}, msg={}",
                    shard_id.table_id, shard_id.shard_index, msg);
            return Status::OK();
        };

        TableOr current_table;
        RETURN_IF_INVALID_STATUS(txn.get_table(table.table_id, current_table))

        if (current_table.is_empty() || is_absent(current_table->state.phase) ||
            (current_table->state.phase != TablePhase::CREATING && current_table->state.phase != TablePhase::RESIZING &&
             current_table->state.phase != TablePhase::READY) ||
            std::none_of(current_table->spec.shard_ranges.begin(), current_table->spec.shard_ranges.end(),
                         [&](const ShardRange& range) { return range.shard_index == shard_index; })) {
            ADVISKV_METRICS_COUNTER("meta_route_delete_shard_route_request");
            RETURN_IF_INVALID_STATUS(txn.delete_shard_route(shard_id))
            return Status::OK();
        }

        std::vector<Replica> replicas;
        RETURN_IF_INVALID_STATUS(txn.list_replicas_by_shard(shard_id, replicas))

        struct RouteEntryLocal {
            RouteReplica replica;
            NodeID node_id;
            Term term{0};
        };
        std::vector<RouteEntryLocal> leader_entries;
        std::vector<RouteEntryLocal> follower_entries;

        for (const Replica& replica : replicas) {
            if (replica.state.phase != ReplicaPhase::READY) {
                LOG_INFO(
                        "[RouteReconciler] replica_id:{} not routable, "
                        "replica.state.phase:{}",
                        replica.replica_id.to_string(), to<int32_t>(replica.state.phase));
                continue;
            }

            if (replica.spec.assign_node_id.empty()) {
                LOG_INFO("[RouteReconciler] replica_id:{} replica.spec.assign_node_id.empty()",
                         replica.replica_id.to_string());
                continue;
            }

            NodeOr node;
            RETURN_IF_INVALID_STATUS(txn.get_node(replica.spec.assign_node_id, node))

            if (node.is_empty() || node->state.status != NodeStatus::ONLINE) {
                int32_t node_status = node.is_empty() ? -1 : to<int32_t>(node->state.status);
                LOG_INFO(
                        "[RouteReconciler] replica_id:{}, node.is_empty() or "
                        "node->state.status != NodeStatus::ONLINE, "
                        "node.is_empty():{}, node.status:{}",
                        replica.replica_id.to_string(), node.is_empty(), node_status);
                continue;
            }

            // 没有心跳观测的副本不进入路由。
            ReplicaObservedOr observed_or;
            RETURN_IF_INVALID_STATUS(txn.get_replica_observed(replica.replica_id, observed_or))
            if (observed_or.is_empty() || !is_raft_voter(observed_or->member_type)) {
                LOG_INFO("[RouteReconciler] replica_id:{} not routable, no observed facts or not voter",
                         replica.replica_id.to_string());
                continue;
            }
            const ReplicaObserved& observed = observed_or.value();

            RouteEntryLocal entry;
            entry.replica.replica_id = replica.replica_id;
            entry.replica.endpoint = observed.endpoint;
            entry.replica.role = observed.raft_role;
            entry.node_id = replica.spec.assign_node_id;
            entry.term = observed.term;
            if (entry.replica.role == ReplicaRole::LEADER) {
                leader_entries.push_back(std::move(entry));
            } else if (entry.replica.role == ReplicaRole::FOLLOWER ||
                       entry.replica.role == ReplicaRole::PRE_CANDIDATE) {
                entry.replica.role = ReplicaRole::FOLLOWER;
                follower_entries.push_back(std::move(entry));
            }
        }

        if (leader_entries.empty()) {
            return clear_route_and_return("writable leader route is not ready");
        }

        if (leader_entries.size() > 1U) {
            std::sort(leader_entries.begin(), leader_entries.end(),
                      [](const RouteEntryLocal& lhs, const RouteEntryLocal& rhs) { return lhs.term > rhs.term; });
            if (leader_entries[0].term == leader_entries[1].term) {
                return clear_route_and_return(
                        fmt::format("multiple leaders share max term, term={}", leader_entries[0].term));
            }
            leader_entries.resize(1);
        }

        std::sort(follower_entries.begin(), follower_entries.end(),
                  [](const RouteEntryLocal& lhs, const RouteEntryLocal& rhs) {
                      return lhs.replica.replica_id.replica_seq < rhs.replica.replica_id.replica_seq;
                  });

        ShardRoute route;
        route.shard_id = shard_id;
        route.replicas.push_back(std::move(leader_entries.front().replica));
        for (RouteEntryLocal& follower_entry : follower_entries) {
            route.replicas.push_back(std::move(follower_entry.replica));
        }

        ADVISKV_METRICS_COUNTER("meta_route_put_shard_route_request");
        return txn.put_shard_route(route);
    });
    RETURN_IF_INVALID_STATUS(status)

    return route_status;
}

}  // namespace adviskv::meta
