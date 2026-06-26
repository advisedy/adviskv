#include "sdm/service/route_service.h"

#include <fmt/format.h>

#include <algorithm>
#include <memory>

#include "common/define.h"
#include "common/log.h"
#include "common/stable_hash.h"
#include "common/status.h"
#include "common/type.h"
#include "sdm/model/sdm_store.h"
#include "sdm/model/sdm_store_txn.h"

namespace adviskv::sdm {

RouteService::RouteService(SdmStore* store) : store_(store) {}

Status RouteService::get_route(const GetRouteParam& param,
                               ShardRoute* out) const {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_NULLPTR(store_, "store is nullptr")

    TableOr table;
    Status status = store_->read_with([&](const SdmStoreTxn& txn) {
        return txn.get_table_by_name(param.db_name, param.table_name, table);
    });
    RETURN_IF_INVALID_STATUS(status)
    if (table.is_empty()) {
        return Status::TABLE_NOT_FOUND(fmt::format(
            "table {}.{} not found", param.db_name, param.table_name));
    }

    ShardID shard_id = calc_shard_id(*table, param.key);
    ShardRouteOr route;
    status = store_->read_with([&](const SdmStoreTxn& txn) {
        return txn.get_shard_route(shard_id, route);
    });
    RETURN_IF_INVALID_STATUS(status)
    if (route.is_empty()) {
        return Status::ROUTE_NOT_FOUND("route not found");
    }
    int leader_count =
        std::count_if(route->replicas.begin(), route->replicas.end(),
                      [](const RouteEntry& entry) {
                          return entry.role == ReplicaRole::LEADER &&
                                 !entry.ip.empty() && entry.port > 0;
                      });
    if (leader_count != 1) {
        return Status::ROUTE_NOT_FOUND(
            fmt::format("writable leader route is not ready。 leader_count={}",
                        leader_count));
    }
    if (out) {
        *out = *route;
        // 这边第一个就是leader的，
        // 这个是在route_updater那边就保证了的。
    }

    {  // 打一下日志
        std::string route_res{route->shard_id.to_string()};
        for (RouteEntry& one : route->replicas) {
            route_res.append(" replica: " + one.replica_id.to_string() + ", ");
            if (one.role == ReplicaRole::LEADER) {
                route_res.append("role: leader.");
            } else if (one.role == ReplicaRole::CANDIDATE) {
                route_res.append("role: candidate.");
            } else {
                route_res.append("role: follower.");
            }
        }
        LOG_DEBUG("route is ok, {}", route_res);
    }

    return status;
}

ShardID RouteService::calc_shard_id(const Table& table, Key key) const {
    // TODO 将来得搞range
    return ShardID{table.table_id,
                   stable_shard_index(key, table.spec.shard_count)};
}

// TODO：
// 现在RouteService，貌似和Table也还有依赖关系，我在想要不要以后改成Shard那种，让他主要是和Shard有关联呢？
// 倒不如说，replicaGroup本身就有点shard的意思在了。

Status RouteService::reconcile_all() {
    RETURN_IF_NULLPTR(store_, "store is nullptr")
    std::vector<Table> tables;
    Status status = store_->read_with(
        [&](const SdmStoreTxn& txn) { return txn.list_tables(tables); });
    RETURN_IF_INVALID_STATUS(status)
    for (auto& table : tables) {
        for (int i = 0; i < table.spec.shard_count; i++) {
            Status s = check_shard_route(table, to<ShardIndex>(i));
            if (s.fail()) {
                LOG_WARN(
                    "[RouteService] route task update shard route failed, "
                    "table={}, shard={}, "
                    "msg={}",
                    table.table_id, i, s.msg());
            } else {
                LOG_DEBUG(
                    "[RouteService] route ready, table_id={}, shard_index={}",
                    table.table_id, i);
            }
        }
    }
    return Status::OK();
}

Status RouteService::check_shard_route(const Table& table,
                                       ShardIndex shard_index) {
    ShardID shard_id{table.table_id, shard_index};
    Status route_status{Status::OK()};

    Status status = store_->write_with([&](SdmStoreTxn& txn) -> Status {
        auto clear_route_and_return = [&](const std::string& msg) -> Status {
            route_status = Status::ROUTE_NOT_FOUND(msg);
            RETURN_IF_INVALID_STATUS(txn.delete_shard_route(shard_id))
            LOG_WARN("route not ready, table_id={}, shard_index={}, msg={}",
                     shard_id.table_id, shard_id.shard_index, msg);
            return Status::OK();
        };

        TableOr current_table;
        RETURN_IF_INVALID_STATUS(txn.get_table(table.table_id, current_table))

        if (current_table.is_empty() ||
            current_table->state.desired != TableDesired::PRESENT ||
            // 从 table
            // 是creating的时候就得开始准备了，创建table的要求是route都ok了
            (current_table->state.phase != TablePhase::CREATING &&
             current_table->state.phase != TablePhase::READY) ||
            shard_index < 0 || shard_index >= current_table->spec.shard_count) {
            RETURN_IF_INVALID_STATUS(txn.delete_shard_route(shard_id))
            return Status::OK();
        }

        std::vector<Replica> replicas;
        RETURN_IF_INVALID_STATUS(txn.list_replicas_by_shard(shard_id, replicas))

        std::vector<RouteEntry> leader_entries;
        std::vector<RouteEntry> follower_entries;

        for (const Replica& replica : replicas) {
            if (replica.state.desired != ReplicaDesired::PRESENT ||
                replica.state.phase != ReplicaPhase::READY) {
                continue;
            }

            if (replica.spec.assign_node_id.empty()) {
                continue;
            }

            NodeOr node;
            RETURN_IF_INVALID_STATUS(
                txn.get_node(replica.spec.assign_node_id, node))

            if (node.is_empty() || node->state.status != NodeStatus::ONLINE) {
                continue;
            }

            RouteEntry entry;
            entry.replica_id = replica.replica_id;
            entry.node_id = replica.spec.assign_node_id;
            entry.ip = replica.state.observed_endpoint.ip;
            entry.port = replica.state.observed_endpoint.port;
            entry.role = replica.state.observed_raft_role;
            entry.term = replica.state.term;
            if (entry.role == ReplicaRole::LEADER) {
                leader_entries.push_back(std::move(entry));
            } else {
                follower_entries.push_back(std::move(entry));
            }
        }

        if (leader_entries.empty()) {
            return clear_route_and_return("writable leader route is not ready");
        }

        if (leader_entries.size() > 1U) {
            std::sort(leader_entries.begin(), leader_entries.end(),
                      [](const RouteEntry& lhs, const RouteEntry& rhs) {
                          return lhs.term > rhs.term;
                      });
            if (leader_entries[0].term == leader_entries[1].term) {
                return clear_route_and_return(
                    fmt::format("multiple leaders share max term, term={}",
                                leader_entries[0].term));
            }

            for (size_t i = 1; i < leader_entries.size(); i++) {
                leader_entries[i].role = ReplicaRole::FOLLOWER;
                follower_entries.push_back(std::move(leader_entries[i]));
            }
        }

        std::sort(follower_entries.begin(), follower_entries.end(),
                  [](const RouteEntry& lhs, const RouteEntry& rhs) {
                      return lhs.replica_id.replica_seq <
                             rhs.replica_id.replica_seq;
                  });

        ShardRoute route;
        route.shard_id = shard_id;
        route.replicas.push_back(std::move(leader_entries.front()));
        for (RouteEntry& follower_entry : follower_entries) {
            route.replicas.push_back(std::move(follower_entry));
        }

        return txn.put_shard_route(route);
    });
    RETURN_IF_INVALID_STATUS(status)

    return route_status;
}

}  // namespace adviskv::sdm