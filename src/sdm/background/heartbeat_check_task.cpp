
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

HeartBeatCheckTask::HeartBeatCheckTask(SdmStore* sdm_store)
    : sdm_store_(sdm_store) {}

void HeartBeatCheckTask::run() {
    if (sdm_store_ == nullptr) {
        LOG_ERROR("[heartbeat] sdm_store is nullptr");
        return;
    }
    std::vector<Node> node_list;
    Status status = sdm_store_->list_nodes(node_list);
    if (status.fail()) {
        LOG_WARN("[heartbeat] list nodes failed, msg={}", status.msg());
        return;
    }

    if (node_list.empty()) {
        LOG_DEBUG("[heartbeat] the node list is empty");
        return;
    }

    for (Node& node : node_list) {
        status = check_and_modify_node(node);
        if (status.fail()) {
            LOG_WARN("[heartbeat] check node failed, node_id={}, msg={}",
                     node.id, status.msg());
        }
    }
}

Status HeartBeatCheckTask::check_and_modify_node(Node& node) {
    int64_t delta_time =
        func::get_current_ts_ms() - node.state.last_heartbeat_ts;
    Status status = Status::OK();
    switch (node.spec.status) {
        case NodeStatus::ONLINE: {
            if (delta_time < SUSPECT_TIMEOUT_MS) {
                return Status::OK();
            } else if (delta_time < OFFLINE_TIMEOUT_MS) {
                node.spec.status = NodeStatus::SUSPECT;
            } else {
                status = mark_node_offline(node);
            }
            break;
        }

        case NodeStatus::SUSPECT: {
            if (delta_time < SUSPECT_TIMEOUT_MS) {
                node.spec.status = NodeStatus::ONLINE;
            } else if (delta_time < OFFLINE_TIMEOUT_MS) {
            } else {
                status = mark_node_offline(node);
            }
            break;
        }

        case NodeStatus::OFFLINE: {
            if (delta_time < SUSPECT_TIMEOUT_MS) {
                node.spec.status = NodeStatus::ONLINE;
            } else if (delta_time < OFFLINE_TIMEOUT_MS) {
                node.spec.status = NodeStatus::SUSPECT;
            }
            break;
        }

        default: {
            return Status{StatusCode::ERROR, "node status is none"};
        }
    }
    RETURN_IF_INVALID_STATUS(status)
    return sdm_store_->put_node(node);
}

Status HeartBeatCheckTask::mark_node_offline(Node& node) {
    Status status = Status::OK();
    node.spec.status = NodeStatus::OFFLINE;
    //  应该在这里更新sdm_store那边的node2replicas的缓存

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
    std::vector<Replica> replicas;
    status = sdm_store_->list_replicas_by_node(node.id, replicas);
    RETURN_IF_INVALID_STATUS(status)
    for (Replica& replica : replicas) {
        replica.state.phase = ReplicaPhase::LOST;
        replica.state.update_ts = func::get_current_ts_ms();
        status = sdm_store_->put_replica(replica);
        RETURN_IF_INVALID_STATUS(status)
    }
    return status;
}

}  // namespace adviskv::sdm
