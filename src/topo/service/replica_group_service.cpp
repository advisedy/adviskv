#include "topo/service/replica_group_service.h"

#include <algorithm>
#include <cstdint>
#include <unordered_set>
#include <vector>

#include "common/define.h"
#include "common/func.h"
#include "common/model/type.h"
#include "common/status.h"
#include "topo/model/model.h"
#include "topo/reconcile/replica_group_membership_reconciler.h"
#include "topo/reconcile/replica_group_plan_reconciler.h"
#include "topo/store/topo_store.h"
#include "topo/store/topo_store_txn.h"

namespace adviskv::topo {

namespace {

enum class RemoveVictimClass {
    BAD = 0,
    NON_MEMBER = 1,
    NON_LEADER = 2,
    LEADER = 3,  // 最后才动 leader
};

RemoveVictimClass get_remove_victim_class_from_replica(const ReplicaOr& replica_or) {
    if (replica_or.is_empty()) {
        return RemoveVictimClass::BAD;
    }
    const Replica& replica = replica_or.value();
    if (replica.state.phase == ReplicaPhase::LOST || replica.state.phase == ReplicaPhase::ERROR) {
        return RemoveVictimClass::BAD;
    }
    if (replica.state.observed_member_type == RaftMemberType::NON_MEMBER) {
        return RemoveVictimClass::NON_MEMBER;
    }
    if (replica.state.observed_raft_role == ReplicaRole::LEADER) {
        return RemoveVictimClass::LEADER;
    }
    return RemoveVictimClass::NON_LEADER;
}

}  // namespace

Status select_remove_member_victims(const TopoStoreTxn& txn, const ReplicaGroup& group, int32_t count,
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
        RemoveVictimClass cls = get_remove_victim_class_from_replica(replica_or);
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

namespace {

ReplicaPhase project_phase_from_observed_facts(const TopoStoreTxn& txn, const Replica& replica) {
    if (replica.state.phase == ReplicaPhase::DELETED) return ReplicaPhase::DELETED;

    if (replica.state.desired == ReplicaDesired::ABSENT) {
        if (replica.state.phase == ReplicaPhase::DELETING && replica.state.observed_no_exist) {
            return ReplicaPhase::DELETED;
        }
        return ReplicaPhase::DELETING;
    }

    if (replica.state.phase == ReplicaPhase::DELETING || replica.state.phase == ReplicaPhase::ERROR) {
        return replica.state.phase;
    }

    NodeOr node;
    if (txn.get_node(replica.spec.assign_node_id, node).fail() || node.is_empty()) {
        return replica.state.phase;
    }
    if (node->state.status == NodeStatus::OFFLINE) {
        return ReplicaPhase::LOST;
    } else if (replica.state.phase == ReplicaPhase::LOST) {  // 说明node是ONLINE的
        return ReplicaPhase::CREATING;
    }
    switch (replica.state.observed_storage_status) {
        case StorageReplicaStatus::INITIALIZING:
        case StorageReplicaStatus::RECOVERING:
            return ReplicaPhase::CREATING;
        case StorageReplicaStatus::READY:
            return ReplicaPhase::READY;
        case StorageReplicaStatus::FAULTED:
        default:
            return ReplicaPhase::ERROR;
    }
}

Status reconcile_replica_phase(TopoStoreTxn& txn, Replica& replica) {
    ReplicaPhase next_phase = project_phase_from_observed_facts(txn, replica);
    if (next_phase == replica.state.phase) {
        return Status::OK();
    }
    replica.state.phase = next_phase;
    if (next_phase == ReplicaPhase::DELETED) {
        replica.state.last_error_msg.clear();
    }
    replica.state.update_ts = func::get_current_ts_ms();
    return txn.put_replica(replica);
}

Status reconcile_all_replica_phases(TopoStoreTxn& txn) {
    std::vector<Replica> replicas;
    RETURN_IF_INVALID_STATUS(txn.list_replicas(replicas))
    for (Replica& replica : replicas) {
        RETURN_IF_INVALID_STATUS(reconcile_replica_phase(txn, replica))
    }
    return Status::OK();
}

std::vector<PeerMember> build_shard_members(const TopoStoreTxn& txn, const ShardID& shard_id, bool voter_only) {
    ReplicaGroupOr group_or;
    if (!txn.get_replica_group(shard_id, group_or).ok() || group_or.is_empty()) {
        return {};
    }

    std::vector<PeerMember> members;
    members.reserve(group_or->desired_members.size());
    for (const ReplicaID& replica_id : group_or->desired_members) {
        ReplicaOr replica;
        if (!txn.get_replica(replica_id, replica).ok() || replica.is_empty()) {
            continue;
        }
        if (voter_only && (replica->state.observed_member_type != RaftMemberType::VOTER)) {
            continue;
        }
        PeerMember member;
        member.node_id = replica->spec.assign_node_id;
        member.replica_id = replica_id;
        member.endpoint = replica->state.observed_endpoint;
        members.push_back(std::move(member));
    }
    return members;
}

Status append_reconfig_command_for_leader(const TopoStoreTxn& txn, const HeartBeatParam& param,
                                          HeartBeatResult& result) {
    std::vector<ReplicaGroup> groups;
    RETURN_IF_INVALID_STATUS(txn.list_replica_groups(groups))

    for (const ReplicaGroup& group : groups) {
        if (group.mode != ReplicaGroupMode::RAFT_RECONFIG || group.target_replica_count <= 0) {
            continue;
        }

        bool node_is_leader = false;
        int32_t healthy_voter_count = 0;
        Optional<ReplicaID> add_target;

        for (const ReplicaID& rid : group.desired_members) {
            ReplicaOr replica_or;
            RETURN_IF_INVALID_STATUS(txn.get_replica(rid, replica_or))
            if (replica_or.is_empty()) continue;

            const Replica& replica = replica_or.value();
            if (replica.spec.assign_node_id == param.node_id &&
                replica.state.observed_raft_role == ReplicaRole::LEADER) {
                node_is_leader = true;
            }
            if (is_healthy(replica) && is_raft_voter(replica.state.observed_member_type)) {
                ++healthy_voter_count;
            }
            if (add_target.is_empty() && replica.state.phase == ReplicaPhase::READY &&
                replica.state.observed_member_type == RaftMemberType::NON_MEMBER) {
                add_target = rid;
            }
        }

        if (!node_is_leader) continue;

        /*
          扩容：
          victim = none
          add_target = new replica

          替换：
          victim = bad old replica
          add_target = new replica

          缩容：
          victim = 尾部最后的那个 replica
          add_target = 尾部最后的那个 replica, because READY+NON_MEMBER
          add_target == victim，所以进行特判一下，如果两个相同的话，就把add_target的取消掉
          由于之前判断 add_target 的逻辑只是通过 READY + NON_MEMBER筛选，所以有可能会和victim撞车
          */

        // 关于这里 filter 的逻辑，只有当我们目前的desired_members 的数量超过了 target 的，才会也把 health
        // 的纳入考虑范围，否则就不会。
        bool include_healthy = static_cast<int32_t>(group.desired_members.size()) > group.target_replica_count;
        std::vector<ReplicaID> victims;
        RETURN_IF_INVALID_STATUS(select_remove_member_victims(txn, group, 1, !include_healthy, victims))
        Optional<ReplicaID> victim;
        if (!victims.empty()) {
            victim = victims.front();
        }
        if (victim.has_value() && add_target.has_value() && add_target.value() == victim.value()) {
            add_target.reset();
        }

        // 在 storage 那边保障幂等性操作。
        if (add_target.has_value()) {
            // 如果add_target也是victim的话，就代表这个里面replica都是好的，打算去掉最后一个，
            // 刚好也是最新添加的一个，但是我们是会限定住的，扩容和缩容只会有一个存在

            ExpectedReplica expect;
            expect.replica_id = add_target.value();
            expect.type = ExpectedReplicaType::ADD_MEMBER;
            expect.initial_members = build_shard_members(txn, group.shard_id, false);
            result.expects.push_back(std::move(expect));
            continue;
        }

        if (healthy_voter_count < group.target_replica_count) {
            continue;
        }

        // 准备开始发送REMOVE_MEMBER
        // 在storgae侧做幂等性操作，如果对于victim正在执行conf change的话，或者已经不在了，都会返回OK
        if (victim.has_value()) {
            ExpectedReplica expect;
            expect.replica_id = victim.value();
            expect.type = ExpectedReplicaType::REMOVE_MEMBER;
            result.expects.push_back(std::move(expect));
        }
    }
    return Status::OK();
}

}  // namespace

ReplicaGroupService::ReplicaGroupService(TopoStore* store, NodeSelector* selector)
        : store_(store), selector_(selector) {}

// 关于这个构建心跳result，分成两个阶段，首先是把期望在但是不在，和期望不在但是在的expected_replicas构成
// 然后再调用append_reconfig_command_for_leader，负责去把ExpectedReplicaType是ADD_MEMBER或者REMOVE_MEMBER的expected_replicas的构建出来
// 注意，这里期望在但是不在的replica，和ADD_MEMBER的replica不会冲突。因为代码判断走的是先是期望在并且也在（READY +
// NON_MEMBER）的才会进行下一步，所以相当于比如在扩容场景下就是分成了两个阶段
Status ReplicaGroupService::build_heartbeat_result(const HeartBeatParam& param, HeartBeatResult& result) const {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_NULLPTR(store_, "store is nullptr")

    result.expects.clear();
    return store_->read_with([&](const TopoStoreTxn& txn) -> Status {
        // 判断这个replica_id是否在shard_id对应的desired_members里面
        auto in_desired_members = [&](const ShardID& shard_id, const ReplicaID& replica_id) -> bool {
            ReplicaGroupOr group_or;
            if (!txn.get_replica_group(shard_id, group_or).ok() || group_or.is_empty()) {
                return false;
            }
            for (const ReplicaID& one : group_or->desired_members) {
                if (one == replica_id) return true;
            }
            return false;
        };

        // node 上报的 replica
        std::unordered_set<ReplicaID, ReplicaIDHash> reported_ids;
        reported_ids.reserve(param.replica_list.size());
        for (const HeartBeatReplicaInfo& info : param.replica_list) {
            reported_ids.insert(info.replica_id);
        }

        std::vector<Replica> replicas;
        RETURN_IF_INVALID_STATUS(txn.list_replicas_by_node(param.node_id, replicas))

        // 期望有却没上报
        for (const Replica& r : replicas) {
            ShardID shard_id{r.replica_id.table_id, r.replica_id.shard_index};
            if (!in_desired_members(shard_id, r.replica_id)) continue;
            if (reported_ids.count(r.replica_id)) continue;

            ExpectedReplica expect;
            expect.replica_id = r.replica_id;
            expect.engine_type = r.spec.engine_type;
            expect.type = ExpectedReplicaType::PRESENT;
            {
                ReplicaGroupOr group_or;
                RETURN_IF_INVALID_STATUS(txn.get_replica_group(shard_id, group_or))
                bool voter_only = group_or.has_value() && group_or->mode == ReplicaGroupMode::RAFT_RECONFIG;
                expect.initial_members = build_shard_members(txn, shard_id, voter_only);
            }
            result.expects.push_back(std::move(expect));
        }

        // 上报了却不该有
        for (const HeartBeatReplicaInfo& info : param.replica_list) {
            ShardID shard_id{info.replica_id.table_id, info.replica_id.shard_index};
            if (in_desired_members(shard_id, info.replica_id)) continue;

            ExpectedReplica expect;
            expect.replica_id = info.replica_id;
            expect.type = ExpectedReplicaType::ABSENT;
            result.expects.push_back(std::move(expect));
        }

        RETURN_IF_INVALID_STATUS(append_reconfig_command_for_leader(txn, param, result))

        return Status::OK();
    });
}

Status ReplicaGroupService::reconcile_all() {
    RETURN_IF_NULLPTR(store_, "store is nullptr")

    ReplicaGroupReconcileContext ctx{store_, selector_};
    ReplicaGroupPlanReconciler plan(ctx);
    ReplicaGroupMembershipReconciler membership(ctx);

    RETURN_IF_INVALID_STATUS(plan.reconcile_all())
    // 在 membership reconcile 之前先把 observed facts 投影成最新 replica phase。
    RETURN_IF_INVALID_STATUS(
            store_->write_with([&](TopoStoreTxn& txn) -> Status { return reconcile_all_replica_phases(txn); }))
    RETURN_IF_INVALID_STATUS(membership.reconcile_all())
    RETURN_IF_INVALID_STATUS(plan.reconcile_all())
    return Status::OK();
}

}  // namespace adviskv::topo