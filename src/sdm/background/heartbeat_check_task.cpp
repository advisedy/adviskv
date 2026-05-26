
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

HeartBeatCheckTask::HeartBeatCheckTask(SdmStore* sdm_store)
    : sdm_store_(sdm_store), start_ts_ms_(func::get_current_ts_ms()) {}

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
    int64_t current_ts = func::get_current_ts_ms();
    int64_t delta_time = current_ts - node.state.last_heartbeat_ts;
    bool in_startup_grace = current_ts - start_ts_ms_ < STARTUP_GRACE_MS;
    Status status = Status::OK();

    if (in_startup_grace) {
        if (delta_time < SUSPECT_TIMEOUT_MS &&
            node.spec.status != NodeStatus::ONLINE) {
            status = mark_node_online(node);
            RETURN_IF_INVALID_STATUS(status)
            return sdm_store_->put_node(node);
        }
        return Status::OK();
    }

    switch (node.spec.status) {
        case NodeStatus::ONLINE: {
            if (delta_time < SUSPECT_TIMEOUT_MS) {
                return Status::OK();
            } else if (delta_time < OFFLINE_TIMEOUT_MS) {
                status = mark_node_suspect(node);
            } else {
                status = mark_node_offline(node);
            }
            break;
        }

        case NodeStatus::SUSPECT: {
            if (delta_time < SUSPECT_TIMEOUT_MS) {
                status = mark_node_online(node);
            } else if (delta_time < OFFLINE_TIMEOUT_MS) {
            } else {
                status = mark_node_offline(node);
            }
            break;
        }

        case NodeStatus::OFFLINE: {
            if (delta_time < SUSPECT_TIMEOUT_MS) {
                status = mark_node_online(node);
            } else if (delta_time < OFFLINE_TIMEOUT_MS) {
                status = mark_node_suspect(node);
            }
            break;
        }

        default: {
            return Status{StatusCode::ERROR, "node status is none"};
        }
    }
    if (status.fail()) {
        LOG_WARN("check_and_modify_node failed, status:{}", status.to_string());
    }
    RETURN_IF_INVALID_STATUS(status)
    return sdm_store_->put_node(node);
}

Status HeartBeatCheckTask::mark_node_offline(Node& node) {
    Status status = Status::OK();
    LOG_INFO("mark node offline: id:{}, ip:{}, port:{}", node.id,
             node.state.endpoint.ip, node.state.endpoint.port);
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

Status HeartBeatCheckTask::mark_node_suspect(Node& node) {
    node.spec.status = NodeStatus::SUSPECT;
    return Status::OK();
}

Status HeartBeatCheckTask::mark_node_online(Node& node) {
    Status status = Status::OK();
    node.spec.status = NodeStatus::ONLINE;
    LOG_INFO("mark node online: id:{}, ip:{}, port:{}", node.id,
             node.state.endpoint.ip, node.state.endpoint.port);
    node.spec.status = NodeStatus::OFFLINE;
    std::vector<Replica> replicas;
    status = sdm_store_->list_replicas_by_node(node.id, replicas);
    RETURN_IF_INVALID_STATUS(status)
    for (Replica& replica : replicas) {
        if (replica.state.desired != ReplicaDesired::PRESENT ||
            replica.state.phase != ReplicaPhase::LOST) {
            continue;
        }
        replica.state.phase = ReplicaPhase::CREATING;
        replica.state.update_ts = func::get_current_ts_ms();
        replica.state.last_error_msg.clear();
        status = sdm_store_->put_replica(replica);
        RETURN_IF_INVALID_STATUS(status)
    }
    return status;
}

}  // namespace adviskv::sdm
