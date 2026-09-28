#include "meta/service/node_service.h"

#include <unordered_map>
#include <unordered_set>

#include "common/define.h"
#include "common/func.h"
#include "common/log.h"
#include "common/status.h"
#include "meta/store/meta_store.h"
#include "meta/store/meta_store_txn.h"

namespace adviskv::meta {

namespace {

constexpr size_t K_HEARTBEAT_CHECK_BATCH_SIZE = 16;

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
Status project_leader_membership(MetaStoreTxn& txn, const HeartBeatReplicaInfo& info) {
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
    if (std::find(group.desired_members.begin(), group.desired_members.end(), info.replica_id) ==
        group.desired_members.end()) {
        LOG_WARN(
                "[NodeService] skip membership report from replica not in desired members, "
                "leader:{}, term:{}",
                info.replica_id.to_string(), info.term);
        return Status::OK();
    }
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
        ReplicaObservedOr observed_or;
        RETURN_IF_INVALID_STATUS(txn.get_replica_observed(rid, observed_or))
        if (observed_or.has_value() && observed_or->member_type == next_type) {
            continue;
        }
        ReplicaObserved observed = observed_or.is_empty() ? ReplicaObserved{} : observed_or.value();
        observed.member_type = next_type;
        observed.update_ts = now;
        RETURN_IF_INVALID_STATUS(txn.put_replica_observed(rid, observed))
    }

    group.observed_membership_term = info.term;
    group.observed_membership_leader = info.replica_id;
    RETURN_IF_INVALID_STATUS(txn.put_replica_group(group))
    return Status::OK();
}

}  // namespace

NodeService::NodeService(MetaStore* store) : store_(store), reconciler_(store) {}

NodeService::NodeService(MetaStore* store, int64_t start_ts_ms)
        : store_(store), reconciler_(store, start_ts_ms) {}

Status NodeService::reconcile_all() { return reconciler_.run(); }

Status NodeService::register_node(const RegisterNodeParam& param) {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_NULLPTR(store_, "store is nullptr")

    return store_->write_with([&](MetaStoreTxn& txn) -> Status {
        NodeOr existing;
        RETURN_IF_INVALID_STATUS(txn.get_node(param.node_id, existing))

        if (existing.is_empty()) {
            Node node;
            node.id = param.node_id;
            node.meta.dc = param.dc;
            node.meta.resource_pool = param.resource_pool;
            node.state.status = NodeStatus::ONLINE;
            node.state.offline_reason = NodeOfflineReason::NONE;
            node.state.endpoint = Endpoint{param.ip, param.port};
            node.state.last_heartbeat_ts = param.last_heartbeat_ts;
            return txn.put_node(node);
        }

        Node node = existing.value();
        bool topology_matches = node.meta.dc == param.dc && node.meta.resource_pool == param.resource_pool;
        node.state.endpoint = Endpoint{param.ip, param.port};
        node.state.last_heartbeat_ts = param.last_heartbeat_ts;
        if (topology_matches) {
            node.state.status = NodeStatus::ONLINE;
            node.state.offline_reason = NodeOfflineReason::NONE;
        } else {
            node.state.status = NodeStatus::OFFLINE;
            node.state.offline_reason = NodeOfflineReason::TOPOLOGY_MISMATCH;
        }
        return txn.put_node(node);
    });
}

Status NodeService::heartbeat(const HeartBeatParam& param) {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_NULLPTR(store_, "store is nullptr")

    Status status = store_->write_with([&](MetaStoreTxn& txn) -> Status {
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

Status NodeService::update_node_heartbeat(MetaStoreTxn& txn, const HeartBeatParam& param) {
    NodeOr node;
    RETURN_IF_INVALID_STATUS(txn.get_node(param.node_id, node))
    RETURN_IF_INVALID_CONDITION(!node.is_empty(), "node not found")

    bool topology_matches = node->meta.dc == param.dc && node->meta.resource_pool == param.resoure_pool_name;
    node->state.endpoint = Endpoint{param.ip, param.port};
    node->state.last_heartbeat_ts = func::get_current_ts_ms();
    if (topology_matches) {
        if (node->state.offline_reason == NodeOfflineReason::TOPOLOGY_MISMATCH) {
            node->state.status = NodeStatus::ONLINE;
        }
        node->state.offline_reason = NodeOfflineReason::NONE;
    } else {
        node->state.status = NodeStatus::OFFLINE;
        node->state.offline_reason = NodeOfflineReason::TOPOLOGY_MISMATCH;
    }
    return txn.put_node(*node);
}

Status NodeService::apply_reported_replicas(MetaStoreTxn& txn, const HeartBeatParam& param) {
    for (const HeartBeatReplicaInfo& info : param.replica_list) {
        ReplicaID key = info.replica_id;
        ReplicaOr replica;
        RETURN_IF_INVALID_STATUS(txn.get_replica(key, replica))

        if (replica.is_empty()) {
            continue;
        }
        if (replica->spec.assign_node_id != param.node_id) {
            LOG_WARN(
                    "[NodeService] heartbeat reported replica on unexpected node, replica_id:{}, "
                    "expected_node_id:{}, reported_node_id:{}",
                    key.to_string(), replica->spec.assign_node_id, param.node_id);
            continue;
        }
        ReplicaObservedOr prev_observed_or;
        RETURN_IF_INVALID_STATUS(txn.get_replica_observed(key, prev_observed_or))
        if (prev_observed_or.has_value() && info.term < prev_observed_or->term) {
            LOG_WARN(
                    "[NodeService] skip stale replica heartbeat, replica_id:{}, reported_term:{}, "
                    "observed_term:{}",
                    key.to_string(), info.term, prev_observed_or->term);
            continue;
        }

        ReplicaObserved observed;
        observed.raft_role = info.role;
        // member_type 由 leader 的 membership 上报更新。
        observed.member_type = prev_observed_or.is_empty() ? RaftMemberType::NON_MEMBER : prev_observed_or->member_type;
        observed.endpoint = Endpoint{param.ip, param.port};
        observed.storage_status = info.storage_status;
        observed.no_exist = false;
        observed.term = info.term;
        observed.update_ts = func::get_current_ts_ms();
        RETURN_IF_INVALID_STATUS(txn.put_replica_observed(key, observed))

        if (observed.raft_role == ReplicaRole::LEADER) {
            LOG_DEBUG("[NodeService] replica_id:{} is leader, term:{}", replica->replica_id.to_string(), info.term);
            RETURN_IF_INVALID_STATUS(project_leader_membership(txn, info))
        }
    }
    return Status::OK();
}

// 收到某个node的heartbeat后，如果这个node被分配的replica处于ABSENT+
// DELETING，但这次heartbeat没上报它，就说明storage已经删掉了，把它标成
// DELETED
Status NodeService::mark_no_exist_replicas(MetaStoreTxn& txn, const HeartBeatParam& param) {
    std::unordered_set<ReplicaID, ReplicaIDHash> reported_replica_ids;
    reported_replica_ids.reserve(param.replica_list.size());
    for (const HeartBeatReplicaInfo& info : param.replica_list) {
        reported_replica_ids.insert(info.replica_id);
    }

    std::vector<Replica> node_replicas;
    RETURN_IF_INVALID_STATUS(txn.list_replicas_by_node(param.node_id, node_replicas))

    for (const Replica& replica : node_replicas) {
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

        ReplicaObservedOr observed_or;
        RETURN_IF_INVALID_STATUS(txn.get_replica_observed(replica.replica_id, observed_or))
        ReplicaObserved observed = observed_or.is_empty() ? ReplicaObserved{} : observed_or.value();
        observed.no_exist = true;
        observed.update_ts = func::get_current_ts_ms();
        RETURN_IF_INVALID_STATUS(txn.put_replica_observed(replica.replica_id, observed))
    }

    return Status::OK();
}

}  // namespace adviskv::meta
