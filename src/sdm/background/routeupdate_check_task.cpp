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
#include "sdm/selector/leader_selector/leader_selector.h"

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
                return status;
            }
        }
    }
    return Status::OK();
}

Status RouteUpdateCheckTask::check_shard_route(const Table& table,
                                               ShardIndex shard_index) {
    Status status{Status::OK()};
    const ShardID shard_id{.table_id = table.table_id,
                           .shard_index = shard_index};

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
        status =
            sdm_store_->get_node(replica.spec.assign_node_id, node);
        RETURN_IF_INVALID_STATUS(status)

        if (node.is_empty() || node->spec.status != NodeStatus::ONLINE) {
            continue;
        }

        RouteEntry entry{
            .replica_id = replica.replica_id,
            .node_id = replica.spec.assign_node_id,
            .ip = replica.state.observed_endpoint.ip,
            .port = replica.state.observed_endpoint.port,
            .role = replica.state.observed_role,
            .term = replica.state.term,
        };
        if (replica.state.observed_role == ReplicaRole::LEADER) {
            leader_entries.push_back(std::move(entry));
        } else {
            follower_entries.push_back(std::move(entry));
        }
    }

    if (leader_entries.size() < 1U) {
        RETURN_IF_INVALID_STATUS(sdm_store_->delete_shard_route(shard_id))

        LOG_WARN(
            "route not ready, leader_count={}, table_id={}, shard_index={}",
            leader_entries.size(), shard_id.table_id, shard_id.shard_index);
        return Status::OK("leader count < 1");
    }

    if (leader_entries.size() > 1U) {
        // 这边也再check一下关于leader的term是否会有一样的（raft正确就应该没问题）
        for (int i = 1, siz = leader_entries.size(); i < siz; i++) {
            if (leader_entries[i].term == leader_entries[i - 1].term) {
                RETURN_IF_INVALID_STATUS(
                    sdm_store_->delete_shard_route(shard_id))
                return Status::ERROR("error : leader term same!!!!!!");
            }
        }

        // 要把leader只选出来一个term最大的
        std::sort(leader_entries.begin(), leader_entries.end(),
                  [](const RouteEntry& lhs, const RouteEntry& rhs) {
                      return lhs.term > rhs.term;
                  });
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

    ShardRoute route{
        .shard_id = shard_id,
    };
    route.replicas.push_back(std::move(leader_entries.front()));
    route.replicas.insert(route.replicas.end(),
                          std::make_move_iterator(follower_entries.begin()),
                          std::make_move_iterator(follower_entries.end()));

    return sdm_store_->put_shard_route(route);
}

}  // namespace adviskv::sdm
