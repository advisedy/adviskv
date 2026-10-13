#include "meta/reconcile/replica_group_reconcile_util.h"

#include <algorithm>

#include "common/define.h"
#include "common/model/model.h"
#include "meta/store/meta_store_txn.h"

namespace adviskv::meta {

namespace {

enum class RemoveVictimClass {
    BAD = 0,
    NON_MEMBER = 1,
    NON_LEADER = 2,
    LEADER = 3,  // 最后才动 leader
};

RemoveVictimClass get_remove_victim_class_from_replica(const ReplicaOr& replica_or,
                                                       const ReplicaObservedOr& observed_or) {
    if (replica_or.is_empty()) {
        return RemoveVictimClass::BAD;
    }
    const Replica& replica = replica_or.value();
    if (replica.state.phase == ReplicaPhase::LOST || replica.state.phase == ReplicaPhase::ERROR) {
        return RemoveVictimClass::BAD;
    }
    // 没有观测记录时按 NON_MEMBER 处理。
    RaftMemberType member_type =
            observed_or.is_empty() ? RaftMemberType::NON_MEMBER : observed_or->member_type;
    ReplicaRole role = observed_or.is_empty() ? ReplicaRole::FOLLOWER : observed_or->raft_role;
    if (member_type == RaftMemberType::NON_MEMBER) {
        return RemoveVictimClass::NON_MEMBER;
    }
    if (role == ReplicaRole::LEADER) {
        return RemoveVictimClass::LEADER;
    }
    return RemoveVictimClass::NON_LEADER;
}

}  // namespace

Status select_remove_member_victims(const MetaStoreTxn& txn, const ReplicaGroup& group, int32_t count,
                                    bool filter_healthy, std::vector<ReplicaID>& victims) {
    victims.clear();
    if (count <= 0 || group.desired_members.empty()) {
        return Status::OK();
    }

    std::vector<std::pair<RemoveVictimClass, ReplicaID>> ranked;
    ranked.reserve(group.desired_members.size());
    for (const ReplicaID& rid : group.desired_members) {
        ReplicaOr replica_or;
        RETURN_IF_INVALID_STATUS(txn.get_replica(rid, replica_or))
        ReplicaObservedOr observed_or;
        RETURN_IF_INVALID_STATUS(txn.get_replica_observed(rid, observed_or))
        RemoveVictimClass cls = get_remove_victim_class_from_replica(replica_or, observed_or);
        if (filter_healthy && cls != RemoveVictimClass::BAD) {
            continue;
        }
        ranked.emplace_back(cls, rid);
    }

    std::sort(
            ranked.begin(), ranked.end(),
            [](const std::pair<RemoveVictimClass, ReplicaID>& lhs, const std::pair<RemoveVictimClass, ReplicaID>& rhs) {
                if (lhs.first != rhs.first) {
                    return lhs.first < rhs.first;
                }
                return lhs.second.replica_seq > rhs.second.replica_seq;
            });

    size_t n = std::min(to<size_t>(count), ranked.size());
    victims.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        victims.push_back(ranked[i].second);
    }
    return Status::OK();
}

Status select_single_remove_victim(const MetaStoreTxn& txn, const ReplicaGroup& group, Optional<ReplicaID>& victim) {
    // 关于这里 filter 的逻辑，只有当我们目前的desired_members 的数量超过了 target 的，才会也把 health
    // 的纳入考虑范围，否则就不会。
    bool include_healthy = to<int32_t>(group.desired_members.size()) > group.target_replica_count;
    std::vector<ReplicaID> victims;
    RETURN_IF_INVALID_STATUS(select_remove_member_victims(txn, group, 1, !include_healthy, victims))
    if (!victims.empty()) {
        victim = victims.front();
    }
    return Status::OK();
}

}  // namespace adviskv::meta
