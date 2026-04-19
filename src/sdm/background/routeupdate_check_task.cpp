#include "sdm/background/routeupdate_check_task.h"

#include <fmt/format.h>

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
        WARN("11");
        return;
    }

    for (TablePtr& table_ptr : table_list) {
        for (int i = 0; i < table_ptr->spec.shard_count; i++) {
            status = check_shard_route(*table_ptr, i);
            if (status.fail()) {
                WARN("22");
            }
        }
    }
}

Status RouteUpdateCheckTask::check_shard_route(const Table& table,
                                               ShardID shard_id) {
    Status status{Status::OK()};
    std::vector<ReplicaPtr> replicas;
    status =
        sdm_store_->list_replicas_by_shard(table.table_id, shard_id, replicas);
    RETURN_IF_INVALID_STATUS(status)

    std::vector<ReplicaPtr> healthy_replicas;
    std::vector<NodePtr> candidate_nodes;
    int have_leader_cnt = 0;

    for (const ReplicaPtr& replica_ptr : replicas) {
        if (!replica_ptr) {
            continue;
        }

        if (replica_ptr->state.status != ReplicaStatus::READY) {
            continue;
        }

        if (replica_ptr->spec.assign_node_id.empty()) {
            continue;
        }

        NodePtr node_ptr;
        status =
            sdm_store_->get_node(replica_ptr->spec.assign_node_id, node_ptr);
        RETURN_IF_INVALID_STATUS(status)

        if (!node_ptr || node_ptr->state.status != NodeStatus::ONLINE) {
            continue;
        }

        healthy_replicas.emplace_back(replica_ptr);
        candidate_nodes.emplace_back(node_ptr);
        if (replica_ptr->state.role == ReplicaRole::LEADER) {
            ++have_leader_cnt;
        }
    }

    if (have_leader_cnt > 1) {
        return Status{StatusCode::ERROR,
                      fmt::format("leader count: {}", have_leader_cnt)};
    }

    if (!healthy_replicas.empty() && have_leader_cnt == 0) {
        NodePtr leader_node;
        status = leader_selector_.select_leader(candidate_nodes, leader_node);
        RETURN_IF_INVALID_STATUS(status)

        for (ReplicaPtr& replica_ptr : healthy_replicas) {
            if (replica_ptr->spec.assign_node_id == leader_node->id) {
                replica_ptr->spec.role = ReplicaRole::LEADER;
            } else {
                replica_ptr->spec.role = ReplicaRole::FOLLOWER;
            }
        }
    }

    ShardRoute route{
        .table_id = table.table_id,
        .shard_id = shard_id,
    };

    for (const ReplicaPtr& replica_ptr : healthy_replicas) {
        route.replicas.push_back(RouteEntry{
            .replica_key = replica_ptr->replica_key,
            .node_id = replica_ptr->spec.assign_node_id,
            .sp = replica_ptr->state.endpoint.ip,
            .port = replica_ptr->state.endpoint.port,
        });
    }

    return sdm_store_->put_shard_route(route);
}

}  // namespace adviskv::sdm
