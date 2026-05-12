#include "sdm/background/routeupdate_check_task.h"

#include <fmt/format.h>

#include <algorithm>
#include <vector>

#include "common/define.h"
#include "common/log.h"
#include "common/status.h"
#include "common/type.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {

void RouteUpdateCheckTask::run() {
    // 拿到路由表，然后进行更新，把对应的shardroute里面的没有用的replica给去掉。
    // 并且选择leader
    Status status{Status::OK()};

    std::vector<TablePtr> table_list;
    status = sdm_store_->list_tables(table_list);
    if (status.fail()) {
        LOG_WARN("11");
        return;
    }

    for (TablePtr& table_ptr : table_list) {
        if (!table_ptr) {
            continue;
        }
        if (table_ptr->state.lifecycle != TableLifecycle::WAITING_ROUTE_READY &&
            table_ptr->state.lifecycle != TableLifecycle::READY) {
            continue;
        }
        for (int i = 0; i < table_ptr->spec.shard_count; i++) {
            status = check_shard_route(*table_ptr, static_cast<ShardIndex>(i));
            if (status.fail()) {
                LOG_WARN(
                    "route task update shard route failed, table={}, shard={}, msg={}",
                    table_ptr->table_id, i, status.msg());
            }
        }
    }
}

Status RouteUpdateCheckTask::check_shard_route(const Table& table,
                                               ShardIndex shard_index) {
    Status status{Status::OK()};
    const ShardID shard_id{.table_id = table.table_id,
                           .shard_index = shard_index};

    // 获取store里面shard里的replicas
    std::vector<ReplicaPtr> replicas;
    status = sdm_store_->list_replicas_by_shard(shard_id, replicas);
    RETURN_IF_INVALID_STATUS(status)

    std::vector<RouteEntry> leader_entries;
    std::vector<RouteEntry> follower_entries;

    // 获取里面状态是正常的replicas
    // 然后更新到我们的路由表里面
    for (const ReplicaPtr& replica_ptr : replicas) {
        if (!replica_ptr) {
            continue;
        }

        if (replica_ptr->spec.status != ReplicaStatus::READY) {
            continue;
        }

        if (replica_ptr->spec.assign_node_id.empty()) {
            continue;
        }

        NodePtr node_ptr;
        status =
            sdm_store_->get_node(replica_ptr->spec.assign_node_id, node_ptr);
        RETURN_IF_INVALID_STATUS(status)

        if (!node_ptr || node_ptr->spec.status != NodeStatus::ONLINE) {
            continue;
        }

        RouteEntry entry{
            .replica_id = replica_ptr->replica_id,
            .node_id = replica_ptr->spec.assign_node_id,
            .ip = replica_ptr->state.endpoint.ip,
            .port = replica_ptr->state.endpoint.port,
            .role = replica_ptr->state.role,
        };
        if (replica_ptr->state.role == ReplicaRole::LEADER) {
            leader_entries.push_back(std::move(entry));
        } else {
            follower_entries.push_back(std::move(entry));
        }
    }

    if (leader_entries.size() != 1) {
        status = sdm_store_->delete_shard_route(shard_id);
        RETURN_IF_INVALID_STATUS(status)
        if (leader_entries.size() > 1) {
            return Status{StatusCode::ERROR,
                          fmt::format("leader count: {}",
                                      leader_entries.size())};
        }
        LOG_DEBUG("shard route not ready, table_id={}, shard_index={}",
                  shard_id.table_id, shard_id.shard_index);
        return Status::OK();
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
