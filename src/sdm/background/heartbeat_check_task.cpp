
#include "sdm/background/heartbeat_check_task.h"

#include <cassert>
#include <cstdint>

#include "common/define.h"
#include "common/func.h"
#include "common/log.h"
#include "common/status.h"
#include "common/type.h"
#include "sdm/model/store.h"
namespace adviskv::sdm {

constexpr int64_t SUSPECT_TIMEOUT_MS = 10 * 1000;
constexpr int64_t OFFLINE_TIMEOUT_MS = 30 * 1000;

void HeartBeatCheckTask::run() {
    if (sdm_store_ == nullptr) {
        ERROR("[heartbeat] sdm_store is nullptr");
        return;
    }
    std::vector<ResourcePoolPtr> pools;
    Status status = sdm_store_->list_resource_pools(pools);
    if (status.fail()) {
        WARN("[heartbeat] get resource pools eror");
        return;
    }

    if (pools.empty()) {
        WARN("[heartbeat] the pool list is empty");
        return;
    }

    // 枚举资源池里面的每一个node，然后开始检测node的状态
    for (ResourcePoolPtr& one : pools) {
        std::vector<NodePtr> node_list;
        status = sdm_store_->list_nodes_by_resource_pool(one->name, node_list);
        if (status.fail()) {
            WARN("");
            continue;
        }
        for (const NodePtr& node : node_list) {
            // 接下来是检测node的状态
            status = check_and_modify_node(*node);
            if (status.fail()) {
                WARN("...");
            }
        }
    }
}

Status HeartBeatCheckTask::check_and_modify_node(Node& node) {
    int64_t delta_time = get_current_ts_ms() - node.state.last_heartbeat_ts;
    Status status = Status::OK();
    switch (node.state.status) {
        case NodeStatus::ONLINE: {
            if (delta_time < SUSPECT_TIMEOUT_MS) {
                return Status::OK();
            } else if (delta_time < OFFLINE_TIMEOUT_MS) {
                node.state.status = NodeStatus::SUSPECT;
            } else {
                status = mark_node_offline(node);
            }
            break;
        }

        case NodeStatus::SUSPECT: {
            if (delta_time < SUSPECT_TIMEOUT_MS) {
                node.state.status = NodeStatus::ONLINE;
            } else if (delta_time < OFFLINE_TIMEOUT_MS) {
            } else {
                status = mark_node_offline(node);
            }
            break;
        }

        case NodeStatus::OFFLINE: {
            if (delta_time < SUSPECT_TIMEOUT_MS) {
                node.state.status = NodeStatus::ONLINE;
            } else if (delta_time < OFFLINE_TIMEOUT_MS) {
                node.state.status = NodeStatus::SUSPECT;
            }
            break;
        }

        default: {
            return Status{StatusCode::ERROR, "node status is none"};
        }
    }

    return status;
}

Status HeartBeatCheckTask::mark_node_offline(Node& node) {
    Status status = Status::OK();
    node.state.status = NodeStatus::OFFLINE;
    // TODO 应该在这里更新sdm_store那边的node2replicas的缓存
    
    // for (const ReplicaKey& replica_key : node.replicas) {
    //     // 更换逻辑，直接在store里面删除replica，会在CapacityChecker那边补上
    //     status = sdm_store_->del_replica(replica_key);
    //     RETURN_IF_INVALID_STATUS(status)

    //     // ReplicaPtr replica_ptr;
    //     // status = sdm_store_->get_replica(replica_key, replica_ptr);
    //     // RETURN_IF_INVALID_STATUS(status)
    //     // replica_ptr->state.status = ReplicaStatus::LOST;
    //     // replica_ptr->state.assign_node_id = "";
    //     // replica_ptr->state.endpoint = {};
    // }
    // node.replicas.clear();
    return status;
}

}  // namespace adviskv::sdm