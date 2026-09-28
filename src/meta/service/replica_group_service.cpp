#include "meta/service/replica_group_service.h"

#include <algorithm>
#include <unordered_set>
#include <vector>

#include "common/define.h"
#include "common/log.h"
#include "common/model/type.h"
#include "meta/reconcile/replica_group_reconcile_util.h"
#include "meta/store/meta_store.h"
#include "meta/store/meta_store_txn.h"

namespace adviskv::meta {

namespace {

std::vector<PeerMember> build_shard_members(const MetaStoreTxn& txn, const ShardID& shard_id, bool voter_only) {
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
        ReplicaObservedOr observed_or;
        if (!txn.get_replica_observed(replica_id, observed_or).ok() || observed_or.is_empty()) {
            continue;
        }
        if (voter_only && (observed_or->member_type != RaftMemberType::VOTER)) {
            continue;
        }
        PeerMember member;
        member.node_id = replica->spec.assign_node_id;
        member.replica_id = replica_id;
        member.endpoint = observed_or->endpoint;
        members.push_back(std::move(member));
    }
    return members;
}

Status append_reconfig_command_for_leader(const MetaStoreTxn& txn, const HeartBeatParam& param,
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
            ReplicaObservedOr observed_or;
            RETURN_IF_INVALID_STATUS(txn.get_replica_observed(rid, observed_or))
            const ReplicaObserved observed = observed_or.is_empty() ? ReplicaObserved{} : observed_or.value();

            if (replica.spec.assign_node_id == param.node_id && observed.raft_role == ReplicaRole::LEADER) {
                node_is_leader = true;
            }
            if (is_healthy(replica) && is_raft_voter(observed.member_type)) {
                ++healthy_voter_count;
            }
            if (add_target.is_empty() && replica.state.phase == ReplicaPhase::READY &&
                observed.member_type == RaftMemberType::NON_MEMBER) {
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
          add_target = 尾部最后的那个 replica，因为是 READY+NON_MEMBER
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

ReplicaGroupService::ReplicaGroupService(MetaStore* store, NodeSelector* selector)
        : store_(store), reconciler_(store, selector) {}

Status ReplicaGroupService::reconcile_all() { return reconciler_.run(); }

// 关于这个构建心跳result，分成两个阶段，首先是把期望在但是不在，和期望不在但是在的expected_replicas构成
// 然后再调用append_reconfig_command_for_leader，负责去把ExpectedReplicaType是ADD_MEMBER或者REMOVE_MEMBER的expected_replicas的构建出来
// 注意，这里期望在但是不在的replica，和ADD_MEMBER的replica不会冲突。因为代码判断走的是先是期望在并且也在（READY +
// NON_MEMBER）的才会进行下一步，所以相当于比如在扩容场景下就是分成了两个阶段
Status ReplicaGroupService::build_heartbeat_result(const HeartBeatParam& param, HeartBeatResult& result) const {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_NULLPTR(store_, "store is nullptr")

    result.expects.clear();
    return store_->read_with([&](const MetaStoreTxn& txn) -> Status {
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

        // 空 Meta 收到已有副本时不能下发 ABSENT，否则破坏性升级会误删旧集群数据。
        if (replicas.empty() && !param.replica_list.empty()) {
            std::vector<Table> tables;
            std::vector<ReplicaGroup> groups;
            RETURN_IF_INVALID_STATUS(txn.list_tables(tables))
            RETURN_IF_INVALID_STATUS(txn.list_replica_groups(groups))
            if (tables.empty() && groups.empty()) {
                return Status::CONDITION_NOT_MET(
                        "meta is empty but storage reports replicas; refusing destructive cleanup");
            }
        }

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
            ReplicaGroupOr group_or;
            RETURN_IF_INVALID_STATUS(txn.get_replica_group(shard_id, group_or))
            if (group_or.has_value()) {
                if (std::find(group_or->desired_members.begin(), group_or->desired_members.end(), info.replica_id) !=
                    group_or->desired_members.end()) {
                    continue;
                }
            } else {
                // table先持久化，replica group后续才创建，所以这段时间不能直接删掉storage上报的replica。
                // 如果table已经在删除了，就继续走下面的删除逻辑。
                TableOr table_or;
                RETURN_IF_INVALID_STATUS(txn.get_table(info.replica_id.table_id, table_or))
                if (table_or.has_value() && table_or->state.desired == TableDesired::PRESENT &&
                    table_or->state.phase != TablePhase::DELETED && table_or->state.phase != TablePhase::FAILED) {
                    continue;
                }
            }

            ExpectedReplica expect;
            expect.replica_id = info.replica_id;
            expect.type = ExpectedReplicaType::ABSENT;
            result.expects.push_back(std::move(expect));
        }

        RETURN_IF_INVALID_STATUS(append_reconfig_command_for_leader(txn, param, result))

        return Status::OK();
    });
}

}  // namespace adviskv::meta
