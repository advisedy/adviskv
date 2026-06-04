#include "sdm/background/routeupdate_check_task.h"

#include <fmt/format.h>

#include <algorithm>
#include <vector>

#include "common/define.h"
#include "common/log.h"
#include "common/status.h"
#include "common/type.h"
#include "sdm/model/i_sdm_metastore.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {

void RouteUpdateCheckTask::run() {
    Status status = update_once();
    if (status.fail()) {
        LOG_WARN("route update failed, msg={}", status.msg());
    }
}

Status RouteUpdateCheckTask::update_once() {
    // 拿到路由表，然后进行更新，把对应的shardroute里面的没有用的replica给去掉。
    // 并且选择leader
    Status status{Status::OK()};

    std::vector<Table> table_list;

    RETURN_IF_INVALID_STATUS(sdm_store_->list_tables(table_list))

    for (const Table& table : table_list) {
        if (table.state.desired != TableDesired::PRESENT ||
            (table.state.phase != TablePhase::CREATING &&
             table.state.phase != TablePhase::READY)) {
            continue;
        }
        for (int i = 0; i < table.spec.shard_count; i++) {
            status = check_shard_route(table, static_cast<ShardIndex>(i));
            if (status.fail()) {
                LOG_WARN(
                    "route task update shard route failed, table={}, shard={}, "
                    "msg={}",
                    table.table_id, i, status.msg());
            } else {
                LOG_DEBUG("route ready, table_id={}, shard_index={}",
                          table.table_id, i);
            }
        }
    }
    return Status::OK();
}

// TODO 这里返回的状态不一致 maybe fix
Status RouteUpdateCheckTask::check_shard_route(const Table& table,
                                               ShardIndex shard_index) {
    Status status{Status::OK()};
    ShardID shard_id{table.table_id, shard_index};

    auto clear_route_and_return = [&](const std::string& msg) -> Status {
        RETURN_IF_INVALID_STATUS(sdm_store_->delete_shard_route(shard_id))
        LOG_WARN("route not ready, table_id={}, shard_index={}, msg={}",
                 shard_id.table_id, shard_id.shard_index, msg);
        return Status::ROUTE_NOT_FOUND(msg);
    };

    // 获取store里面shard里的replicas
    std::vector<Replica> replicas;
    status = sdm_store_->list_replicas_by_shard(shard_id, replicas);
    RETURN_IF_INVALID_STATUS(status)

    std::vector<RouteEntry> leader_entries;
    std::vector<RouteEntry> follower_entries;

    // 获取里面状态是正常的replicas
    // 然后更新到我们的路由表里面
    for (const Replica& replica : replicas) {
        if (replica.state.desired != ReplicaDesired::PRESENT ||
            replica.state.phase != ReplicaPhase::READY) {
            continue;
        }

        if (replica.spec.assign_node_id.empty()) {
            continue;
        }

        NodeOr node;
        status = sdm_store_->get_node(replica.spec.assign_node_id, node);
        RETURN_IF_INVALID_STATUS(status)

        if (node.is_empty() || node->spec.status != NodeStatus::ONLINE) {
            continue;
        }

        RouteEntry entry;
        entry.replica_id = replica.replica_id;
        entry.node_id = replica.spec.assign_node_id;
        entry.ip = replica.state.observed_endpoint.ip;
        entry.port = replica.state.observed_endpoint.port;
        entry.role = replica.state.observed_role;
        entry.term = replica.state.term;
        if (replica.state.observed_role == ReplicaRole::LEADER) {
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

        // 要把leader只选出来一个term最大的
        for (int i = 1, siz = leader_entries.size(); i < siz; i++) {
            leader_entries[i].role = ReplicaRole::FOLLOWER;
        }
        follower_entries.insert(
            follower_entries.end(),
            std::make_move_iterator(leader_entries.begin() + 1),
            std::make_move_iterator(leader_entries.end()));
    }

    std::sort(follower_entries.begin(), follower_entries.end(),
              [](const RouteEntry& lhs, const RouteEntry& rhs) {
                  return lhs.replica_id.replica_index <
                         rhs.replica_id.replica_index;
              });

    ShardRoute route;
    route.shard_id = shard_id;
    route.replicas.push_back(std::move(leader_entries.front()));
    route.replicas.insert(route.replicas.end(),
                          std::make_move_iterator(follower_entries.begin()),
                          std::make_move_iterator(follower_entries.end()));

    return sdm_store_->put_shard_route(route);
}

}  // namespace adviskv::sdm