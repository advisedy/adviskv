#include "sdm/service/node_service.h"

#include <algorithm>
#include <unordered_map>
#include <unordered_set>

#include "common/define.h"
#include "common/func.h"
#include "common/log.h"
#include "common/status.h"
#include "sdm/model/sdm_store.h"
#include "sdm/model/sdm_store_txn.h"

namespace adviskv::sdm {

namespace {
constexpr size_t kHeartbeatCheckBatchSize = 16;

// 判断一下是否能够接收，防止旧leader打过来干扰信息
bool should_accept_membership_report(const ReplicaGroup& group, const HeartBeatReplicaInfo& info) {
    if (info.term > group.observed_membership_term) {
        return true;
    }
    if (info.term < group.observed_membership_term) {
        LOG_WARN(
                "[NodeService] skip stale leader membership report, "
                "leader:{}, term:{}, observed_leader:{}, observed_term:{}",
                info.replica_id.to_string(), info.term, group.observed_membership_leader.to_string(),
                group.observed_membership_term);
        return false;
    }
    if (info.replica_id == group.observed_membership_leader) {
        return true;
    }
    LOG_WARN(
            "[NodeService] skip conflicting leader membership report, "
            "leader:{}, term:{}, observed_leader:{}, observed_term:{}",
            info.replica_id.to_string(), info.term, group.observed_membership_leader.to_string(),
            group.observed_membership_term);
    return false;
}

// 把leader心跳里带上来的members进行
Status project_leader_membership(SdmStoreTxn& txn, const HeartBeatReplicaInfo& info) {
    if (info.full_membership.empty()) {
        LOG_WARN(
                "[NodeService] project_leader_membership, leader reports empty raft membership, skip projection, "
                "leader:{}, term:{}",
                info.replica_id.to_string(), info.term);
        return Status::OK();
    }

    ReplicaGroupOr group_or;
    RETURN_IF_INVALID_STATUS(txn.get_replica_group(info.replica_id.get_shard_id(), group_or))
    if (group_or.is_empty()) {
        LOG_WARN(
                "[NodeService] project_leader_membership, leader membership report without replica group, "
                "leader:{}, term:{}",
                info.replica_id.to_string(), info.term);
        return Status::OK();
    }

    ReplicaGroup group = group_or.value();
    if (!should_accept_membership_report(group, info)) {
        return Status::OK();
    }

    std::unordered_map<ReplicaID, RaftMemberType, ReplicaIDHash> types;
    types.reserve(info.full_membership.size());
    for (const RaftMember& member : info.full_membership) {
        const ReplicaID& rid = member.peer.replica_id;
        LOG_DEBUG("[NodeService] project_leader_membership, leader:replica_id:{}, one member:[{}]",
                  info.replica_id.to_string(), member.to_string())
        if (rid.get_shard_id() != group.shard_id) {
            LOG_WARN(
                    "[NodeService] project_leader_membership, leader membership report contains replica from "
                    "another shard, leader:{}, reported:{}",
                    info.replica_id.to_string(), rid.to_string());
            continue;
        }
        if (types.count(rid) > 0) {
            LOG_WARN(
                    "[NodeService] project_leader_membership, leader membership report contains duplicate "
                    "replica, leader:{}, reported:{}",
                    info.replica_id.to_string(), rid.to_string());
            continue;
        }
        types[rid] = member.member_type;
    }

    int64 now = func::get_current_ts_ms();
    for (const ReplicaID& rid : group.desired_members) {
        RaftMemberType next_type = RaftMemberType::NON_MEMBER;
        if (auto it = types.find(rid); it != types.end()) {
            next_type = it->second;
        }
        ReplicaOr replica;
        RETURN_IF_INVALID_STATUS(txn.get_replica(rid, replica))
        if (replica.is_empty()) {
            LOG_WARN("[NodeService] project_leader_membership, desired member replica not found, replica_id:{}",
                     rid.to_string());
            continue;
        }
        if (replica->state.observed_member_type == next_type) {
            continue;
        }
        replica->state.observed_member_type = next_type;
        replica->state.update_ts = now;
        RETURN_IF_INVALID_STATUS(txn.put_replica(replica.value()))
    }

    group.observed_membership_term = info.term;
    group.observed_membership_leader = info.replica_id;
    RETURN_IF_INVALID_STATUS(txn.put_replica_group(group))
    return Status::OK();
}

}  // namespace

NodeService::NodeService(SdmStore* store) : store_(store), start_ts_ms_(func::get_current_ts_ms()) {
}

Status NodeService::register_node(const RegisterNodeParam& param) {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_NULLPTR(store_, "store is nullptr")

    Node node;
    node.id = param.node_id;
    node.meta.dc = param.dc;
    node.meta.resource_pool = param.resource_pool;
    node.state.status = NodeStatus::ONLINE;
    node.state.endpoint = Endpoint{param.ip, param.port};
    node.state.last_heartbeat_ts = param.last_heartbeat_ts;
    return store_->write_with([&](SdmStoreTxn& txn) { return txn.put_node(node); });
}

Status NodeService::heartbeat(const HeartBeatParam& param) {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_NULLPTR(store_, "store is nullptr")

    Status status = store_->write_with([&](SdmStoreTxn& txn) -> Status {
        RETURN_IF_INVALID_STATUS(update_node_heartbeat(txn, param))
        RETURN_IF_INVALID_STATUS(apply_reported_replicas(txn, param))
        RETURN_IF_INVALID_STATUS(mark_no_exist_replicas(txn, param))
        return Status::OK();
    });
    RETURN_IF_INVALID_STATUS(status)

    LOG_DEBUG(
            "[NodeService] get node_id:{} heartbeat, port:{}, "
            "replica_list_size:{}",
            param.node_id, param.port, param.replica_list.size());
    return Status::OK();
}

Status NodeService::update_node_heartbeat(SdmStoreTxn& txn, const HeartBeatParam& param) {
    NodeOr node;
    RETURN_IF_INVALID_STATUS(txn.get_node(param.node_id, node))
    RETURN_IF_INVALID_CONDITION(!node.is_empty(), "node not found")

    node->state.endpoint = Endpoint{param.ip, param.port};
    node->state.last_heartbeat_ts = param.last_heartbeat_ts;
    return txn.put_node(*node);
}

Status NodeService::apply_reported_replicas(SdmStoreTxn& txn, const HeartBeatParam& param) {
    for (const HeartBeatReplicaInfo& info : param.replica_list) {
        ReplicaID key = info.replica_id;
        ReplicaOr replica;
        RETURN_IF_INVALID_STATUS(txn.get_replica(key, replica))

        if (replica.is_empty()) {
            continue;
        }
        if (replica->spec.assign_node_id != param.node_id) {
            LOG_WARN("[NodeService] replica:assign_node_id != node_id");
            continue;
        }

        replica->state.observed_raft_role = info.role;
        replica->state.observed_endpoint = Endpoint{param.ip, param.port};
        replica->state.observed_storage_status = info.storage_status;
        replica->state.observed_no_exist = false;
        replica->state.term = info.term;
        replica->state.update_ts = func::get_current_ts_ms();
        RETURN_IF_INVALID_STATUS(txn.put_replica(*replica))

        if (replica->state.observed_raft_role == ReplicaRole::LEADER) {
            LOG_DEBUG("[NodeService] replica_id:{} is leader", replica->replica_id.to_string());
            RETURN_IF_INVALID_STATUS(project_leader_membership(txn, info))
        }
    }
    return Status::OK();
}

// 收到某个node的heartbeat后，如果这个node被分配的replica处于ABSENT+
// DELETING，但这次heartbeat没上报它，就说明storage已经删掉了，把它标成
// DELETED
Status NodeService::mark_no_exist_replicas(SdmStoreTxn& txn, const HeartBeatParam& param) {
    std::unordered_set<ReplicaID, ReplicaIDHash> reported_replica_ids;
    reported_replica_ids.reserve(param.replica_list.size());
    for (const HeartBeatReplicaInfo& info : param.replica_list) {
        reported_replica_ids.insert(info.replica_id);
    }

    std::vector<Replica> node_replicas;
    RETURN_IF_INVALID_STATUS(txn.list_replicas_by_node(param.node_id, node_replicas))

    for (Replica& replica : node_replicas) {
        if (reported_replica_ids.find(replica.replica_id) != reported_replica_ids.end()) {
            continue;
        }
        if (replica.state.desired != ReplicaDesired::ABSENT || replica.state.phase != ReplicaPhase::DELETING) {
            continue;
        }

        LOG_INFO(
                "[NodeService] mark_no_exist_replicas: node_id:{}, replica no "
                "exist, replica_id:{}",
                param.node_id, replica.replica_id.to_string());

        replica.state.observed_no_exist = true;
        replica.state.update_ts = func::get_current_ts_ms();
        RETURN_IF_INVALID_STATUS(txn.put_replica(replica))
    }

    return Status::OK();
}

Status NodeService::reconcile_all() {
    RETURN_IF_NULLPTR(store_, "store is nullptr")
    std::vector<Node> nodes;
    Status status = store_->read_with([&](const SdmStoreTxn& txn) { return txn.list_nodes(nodes); });
    RETURN_IF_INVALID_STATUS(status)

    std::vector<NodeID> node_ids;
    node_ids.reserve(nodes.size());
    for (const auto& node : nodes) {
        node_ids.push_back(node.id);
    }

    // 这里给node搞批处理，防止持有锁的时间太久了
    for (size_t begin = 0; begin < node_ids.size(); begin += kHeartbeatCheckBatchSize) {
        size_t end = std::min(begin + kHeartbeatCheckBatchSize, node_ids.size());
        status = store_->write_with([&](SdmStoreTxn& txn) -> Status {
            for (size_t index = begin; index < end; ++index) {
                NodeOr current;
                RETURN_IF_INVALID_STATUS(txn.get_node(node_ids[index], current))
                if (current.is_empty()) {
                    continue;
                }
                Node node = *current;
                Status s = check_and_modify_node(txn, node);
                if (s.fail()) {
                    LOG_WARN("[NodeService] check node failed, node_id:{}, msg:{}", node_ids[index], s.msg());
                }
            }
            return Status::OK();
        });
        if (status.fail()) {
            LOG_WARN("[NodeService] node check batch failed, begin:{}, msg:{}", begin, status.msg());
        }
    }
    return Status::OK();
}

Status NodeService::check_and_modify_node(SdmStoreTxn& txn, Node& node) {
    int64_t current_ts_ms = func::get_current_ts_ms();
    int64_t diff = current_ts_ms - node.state.last_heartbeat_ts;
    bool in_startup_grace = (current_ts_ms - start_ts_ms_ <= HEARTBEAT_STARTUP_GRACE_MS);

    if (diff <= HEARTBEAT_SUSPECT_TIMEOUT_MS) {
        if (node.state.status != NodeStatus::ONLINE) {
            return mark_node_online(txn, node);
        }
        return Status::OK();
    }

    if (in_startup_grace) {
        return Status::OK();
    }

    if (diff > HEARTBEAT_OFFLINE_TIMEOUT_MS) {
        if (node.state.status != NodeStatus::OFFLINE) {
            return mark_node_offline(txn, node);
        }
    } else {
        if (node.state.status == NodeStatus::ONLINE) {
            return mark_node_suspect(txn, node);
        }
    }

    return Status::OK();
}

Status NodeService::mark_node_offline(SdmStoreTxn& txn, Node& node) {
    LOG_WARN("[NodeService] node offline, node_id:{}", node.id);
    node.state.status = NodeStatus::OFFLINE;
    return txn.put_node(node);
}

Status NodeService::mark_node_suspect(SdmStoreTxn& txn, Node& node) {
    LOG_WARN("[NodeService] node suspect, node_id:{}", node.id);
    node.state.status = NodeStatus::SUSPECT;
    return txn.put_node(node);
}

Status NodeService::mark_node_online(SdmStoreTxn& txn, Node& node) {
    LOG_INFO("[NodeService] node online, node_id:{}", node.id);
    node.state.status = NodeStatus::ONLINE;
    return txn.put_node(node);
}

Status NodeService::check_and_modify_node_for_test(Node& node) {
    RETURN_IF_NULLPTR(store_, "store is nullptr")
    return store_->write_with([&](SdmStoreTxn& txn) -> Status {
        NodeOr current;
        RETURN_IF_INVALID_STATUS(txn.get_node(node.id, current))
        if (current.is_empty()) {
            return Status::OK();
        }
        node = *current;
        return check_and_modify_node(txn, node);
    });
}

}  // namespace adviskv::sdm