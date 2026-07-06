#include "sdm/reconcile/replica_group_membership_reconciler.h"

#include <algorithm>
#include <optional>
#include <sstream>
#include <unordered_set>

#include "common/define.h"
#include "common/func.h"
#include "common/id_allocator.h"
#include "common/log.h"
#include "common/model/type.h"
#include "common/status.h"
#include "sdm/model/model.h"
#include "sdm/model/param.h"
#include "sdm/selector/node_selector/node_selector.h"
#include "sdm/service/replica_group_service.h"
#include "sdm/store/sdm_store.h"
#include "sdm/store/sdm_store_txn.h"

namespace adviskv::sdm {

namespace {

// 所有的members都是READY + VOTER
// group是空的时候，得返回false啊，否则从0开始扩容的话会出现问题，没有leader啊
bool all_members_ready(const SdmStoreTxn& txn, const ReplicaGroup& group) {
    if (group.desired_members.empty()) return false;
    for (const ReplicaID& rid : group.desired_members) {
        ReplicaOr replica;
        if (txn.get_replica(rid, replica).fail() || replica.is_empty()) {
            return false;
        }
        if (replica->state.phase != ReplicaPhase::READY) {
            return false;
        }
        if (replica->state.observed_member_type != RaftMemberType::VOTER) {
            return false;
        }
    }
    return true;
}

}  // namespace

ReplicaGroupMembershipReconciler::ReplicaGroupMembershipReconciler(ReplicaGroupReconcileContext ctx)
        : ctx_(std::move(ctx)) {}

Status ReplicaGroupMembershipReconciler::reconcile_all() {
    RETURN_IF_NULLPTR(ctx_.store, "store is nullptr")

    std::vector<ReplicaGroup> groups;
    RETURN_IF_INVALID_STATUS(
            ctx_.store->read_with([&](const SdmStoreTxn& txn) { return txn.list_replica_groups(groups); }))

    for (const ReplicaGroup& group : groups) {
        Status status = reconcile_group(group);
        if (status.fail()) {
            LOG_WARN(
                    "[ReplicaGroupMembership] reconcile failed, shard=({},{}), "
                    "msg={}",
                    group.shard_id.table_id, group.shard_id.shard_index, status.msg());
        }
    }
    return Status::OK();
}

Status ReplicaGroupMembershipReconciler::reconcile_group(const ReplicaGroup& group) {
    {
        std::stringstream ids;  // replica_ids
        for (const ReplicaID& rid : group.desired_members) {
            ids << rid.to_string() << ", ";
        }
        LOG_DEBUG(
                "[ReplicaGroupMembershipReconciler] reconcile_group, shard_id:{}, mode:{}, target_replica_count:{}, desired_members:[{}]",
                group.shard_id.to_string(), to<int32>(group.mode), group.target_replica_count, ids.str());
    }
    //
    // 从 ctx_.store->read_with 里面获取到这些数据
    ReplicaGroup current_group;
    bool group_exists = false;
    int32_t healthy_count = 0;
    std::vector<ReplicaID> bad_members;
    Optional<ReplicaID> remove_victim;
    bool remove_victim_is_non_member = false;
    //
    RETURN_IF_INVALID_STATUS(ctx_.store->read_with([&](const SdmStoreTxn& txn) -> Status {
        ReplicaGroupOr current_or;
        RETURN_IF_INVALID_STATUS(txn.get_replica_group(group.shard_id, current_or))
        if (current_or.is_empty()) return Status::OK();

        group_exists = true;
        current_group = current_or.value();
        for (const ReplicaID& rid : current_group.desired_members) {
            ReplicaOr replica_or;
            RETURN_IF_INVALID_STATUS(txn.get_replica(rid, replica_or))
            if (replica_or.is_empty()) {
                LOG_WARN(
                        "[ReplicaGroupMembershipReconciler] replica_or.is_empty(), "
                        "replica_id:{}",
                        rid.to_string());
                continue;
            }
            const Replica& replica = replica_or.value();
            if (is_healthy(replica)) {
                ++healthy_count;
            } else if (replica.state.phase == ReplicaPhase::LOST || replica.state.phase == ReplicaPhase::ERROR) {
                bad_members.push_back(rid);
            }
        }
        RETURN_IF_INVALID_STATUS(select_remove_member_victim(txn, current_group, remove_victim))
        if (remove_victim.has_value()) {
            ReplicaOr victim_or;
            RETURN_IF_INVALID_STATUS(txn.get_replica(remove_victim.value(), victim_or))
            remove_victim_is_non_member =
                    !victim_or.is_empty() && victim_or->state.observed_member_type == RaftMemberType::NON_MEMBER;
        }
        return Status::OK();
    }))
    ///

    if (!group_exists) return Status::OK();

    {
        std::stringstream ids;
        for (const ReplicaID& replica_id : bad_members) {
            ids << replica_id.to_string() << ",";
        }

        LOG_DEBUG(
                "[ReplicaGroupMembershipReconciler] reconcile_group, replica_group:shard_id:{}, healthy_count:{}, bad_members:[{}], remove_victim:{}, remove_victim_is_non_member",
                group.shard_id.to_string(), healthy_count, ids.str(),
                (remove_victim.is_empty() ? "-1" : remove_victim->to_string()), remove_victim_is_non_member);
    }
    int32 target = current_group.target_replica_count;
    int32 member_count = static_cast<int32>(current_group.desired_members.size());

    // 对于 replica_count = 0 的特判
    if (target == 0) {
        if (member_count > 0) {
            return remove_members(current_group, member_count);
        }
        return cleanup_group(current_group);
    }

    // 先把正常，并且不是要删除的replca进行补成员，
    if (healthy_count < target) {
        LOG_DEBUG(
                "[ReplicaGroupMembershipReconciler] reconcile_group, add members, shard_id:{}, healthy_count:{}, target:{}, count_to_add:{}",
                current_group.shard_id.to_string(), healthy_count, target, target - healthy_count);
        return add_members(current_group, target - healthy_count);
    }

    if (current_group.mode == ReplicaGroupMode::RAFT_RECONFIG) {
        // 这种情况下，我们的目的就是想去掉
        // victim，所以如果他是NON_MEMBER的话，就可以直接remove，否则就等待，先暂时返回OK
        if (remove_victim.has_value()) {
            if (remove_victim_is_non_member) {
                LOG_DEBUG(
                        "[ReplicaGroupMembershipReconciler] reconcile_group, remove victim after raft reconfig, shard_id:{}, victim:{}",
                        current_group.shard_id.to_string(), remove_victim->to_string());

                return remove_specific_members(current_group, {remove_victim.value()});
            }
            LOG_DEBUG(
                    "[ReplicaGroupMembershipReconciler] reconcile_group, wait victim become non-member, shard_id:{}, victim:{}",
                    current_group.shard_id.to_string(), remove_victim->to_string());

            return Status::OK();
        }
        return cleanup_group(current_group);
    }

    // current_group.mode == ReplicaGroupMode::BOOTSTRAP
    // 移除坏节点，然后缩容
    if (!bad_members.empty()) {
        LOG_DEBUG(
                "[ReplicaGroupMembershipReconciler] reconcile_group, remove bad members, shard_id:{}, bad_member_count:{}",
                current_group.shard_id.to_string(), bad_members.size());

        return remove_specific_members(current_group, bad_members);
    }
    if (member_count > target) {
        LOG_DEBUG(
                "[ReplicaGroupMembershipReconciler] reconcile_group, shrink members, shard_id:{}, member_count:{}, target:{}, count_to_remove:{}",
                current_group.shard_id.to_string(), member_count, target, member_count - target);

        return remove_members(current_group, member_count - target);
    }
    LOG_DEBUG("[ReplicaGroupMembershipReconciler] reconcile_group, cleanup group, shard_id:{}",
              current_group.shard_id.to_string());

    return cleanup_group(current_group);
}

// 负责ReplicaGroup的Mode修改，replica的物理逻辑删除
Status ReplicaGroupMembershipReconciler::cleanup_group(const ReplicaGroup& group) {
    return ctx_.store->write_with([&](SdmStoreTxn& txn) -> Status {
        ReplicaGroupOr current_or;
        RETURN_IF_INVALID_STATUS(txn.get_replica_group(group.shard_id, current_or))
        if (current_or.is_empty()) return Status::OK();

        std::vector<Replica> shard_replicas;
        RETURN_IF_INVALID_STATUS(txn.list_replicas_by_shard(group.shard_id, shard_replicas));
        for (const Replica& replica : shard_replicas) {
            if (replica.state.phase == ReplicaPhase::DELETED) {
                RETURN_IF_INVALID_STATUS(txn.delete_replica(replica.replica_id))
            }
        }

        ReplicaGroup current = current_or.value();

        if (current.mode == ReplicaGroupMode::RAFT_RECONFIG) {
            if (current.target_replica_count == 0) {
                LOG_DEBUG("[ReplicaGroupMembershipReconciler] cleanup_group, reset mode to bootstrap, shard_id:{}",
                          current.shard_id.to_string());

                current.mode = ReplicaGroupMode::BOOTSTRAP;
                RETURN_IF_INVALID_STATUS(txn.put_replica_group(current))
                return Status::OK();
            }
            return Status::OK();
        }
        // current.mode == ReplicaGroupMode::BOOTSTRAP
        if (static_cast<int32_t>(current.desired_members.size()) == current.target_replica_count &&
            all_members_ready(txn, current)) {
            current.mode = ReplicaGroupMode::RAFT_RECONFIG;
            RETURN_IF_INVALID_STATUS(txn.put_replica_group(current))
        }
        return Status::OK();
    });
}

// TODO111 吧count_to_add改一下名字
Status ReplicaGroupMembershipReconciler::add_members(const ReplicaGroup& group, int32_t count_to_add) {
    RETURN_IF_NULLPTR(ctx_.selector, "selector is nullptr")
    RETURN_IF_INVALID_CONDITION(count_to_add > 0, "count_to_add must be > 0")

    std::string resource_pool;
    EngineType engine_type{EngineType::MAP};
    bool should_add = true;
    std::unordered_set<NodeID> occupied_node_ids;
    {
        TableOr table_or;
        Status status = ctx_.store->read_with([&](const SdmStoreTxn& txn) -> Status {
            RETURN_IF_INVALID_STATUS(txn.get_table(group.shard_id.table_id, table_or))
            if (table_or.is_empty()) {
                should_add = false;
                return Status::OK();
            }

            ReplicaGroupOr current_or;
            RETURN_IF_INVALID_STATUS(txn.get_replica_group(group.shard_id, current_or))
            if (current_or.is_empty() || current_or->target_replica_count <= 0) {
                should_add = false;
                return Status::OK();
            }

            for (const ReplicaID& rid : current_or->desired_members) {
                ReplicaOr replica_or;
                RETURN_IF_INVALID_STATUS(txn.get_replica(rid, replica_or))
                if (!replica_or.is_empty() && !replica_or->spec.assign_node_id.empty()) {
                    occupied_node_ids.insert(replica_or->spec.assign_node_id);
                }
            }
            return Status::OK();
        });
        RETURN_IF_INVALID_STATUS(status)
        if (!should_add) return Status::OK();
        resource_pool = table_or->spec.resource_pool;
        engine_type = table_or->spec.engine_type;
    }

    TablePlacementResult placement;
    PlaceNodesParam param;
    param.resource_pool = resource_pool;
    param.shard_count = 1;
    param.replica_count = count_to_add;
    param.excluded_node_ids = std::move(occupied_node_ids);

    if (Status select_status = ctx_.selector->select_table_nodes(param, placement); select_status.fail()) {
        LOG_WARN("[ReplicaGroupMembershipReconciler] add_members, node selector failed, status:{}",
                 select_status.to_string());
        return select_status;
    }

    const std::vector<Node>& nodes = placement.shards.front().nodes;

    return ctx_.store->write_with([&](SdmStoreTxn& txn) -> Status {
        ReplicaGroupOr current_or;
        RETURN_IF_INVALID_STATUS(txn.get_replica_group(group.shard_id, current_or))
        if (current_or.is_empty()) return Status::OK();

        ReplicaGroup current = current_or.value();
        if (current.target_replica_count <= 0) return Status::OK();

        std::vector<Replica> new_replicas;
        std::vector<ReplicaID> new_rids;
        new_replicas.reserve(count_to_add);
        new_rids.reserve(count_to_add);
        for (int32_t i = 0; i < count_to_add; ++i) {
            ReplicaSeq seq = current.seq_allocator.get_next_id();
            ReplicaID rid{current.shard_id.table_id, current.shard_id.shard_index, seq};
            Replica replica;
            replica.replica_id = rid;
            replica.spec.dc = nodes[i].meta.dc;
            replica.spec.assign_node_id = nodes[i].id;
            replica.spec.engine_type = engine_type;
            replica.state.desired = ReplicaDesired::PRESENT;
            replica.state.phase = ReplicaPhase::CREATING;
            replica.state.observed_raft_role = ReplicaRole::FOLLOWER;
            replica.state.observed_member_type = RaftMemberType::NON_MEMBER;
            replica.state.observed_endpoint = nodes[i].state.endpoint;
            replica.state.update_ts = func::get_current_ts_ms();
            replica.state.term = 0;
            LOG_INFO(
                    "[ReplicaGroupMembershipReconciler] add_members, create replica, shard_id:{}, replica_id:{}, node_id:{}, endpoint:{}",
                    current.shard_id.to_string(), rid.to_string(), nodes[i].id, nodes[i].state.endpoint.to_string());
            new_replicas.emplace_back(std::move(replica));
            new_rids.push_back(rid);
        }

        RETURN_IF_INVALID_STATUS(txn.put_replicas(new_replicas))
        current.desired_members.insert(current.desired_members.end(), new_rids.begin(), new_rids.end());
        RETURN_IF_INVALID_STATUS(txn.put_replica_group(current))
        return Status::OK();
    });
}

Status ReplicaGroupMembershipReconciler::remove_members(const ReplicaGroup& group, int32_t count_to_remove) {
    RETURN_IF_INVALID_CONDITION(count_to_remove > 0, "count_to_remove must be > 0")

    return ctx_.store->write_with([&](SdmStoreTxn& txn) -> Status {
        ReplicaGroupOr current_or;
        RETURN_IF_INVALID_STATUS(txn.get_replica_group(group.shard_id, current_or))
        if (current_or.is_empty()) return Status::OK();

        ReplicaGroup current = current_or.value();
        int32 actual_remove_count = std::min(
                count_to_remove, static_cast<int32_t>(current.desired_members.size()) - current.target_replica_count);
        if (actual_remove_count <= 0) return Status::OK();

        std::vector<ReplicaID> victims;
        victims.reserve(actual_remove_count);
        for (int i = 0; i < actual_remove_count; ++i) {
            // TODO111 这里好像我们得判断一下是不是leader对吗？ 否则会有短暂的窗口有问题？
            victims.push_back(current.desired_members.back());
            current.desired_members.pop_back();
        }
        LOG_DEBUG("[ReplicaGroupMembershipReconciler] remove_members, selected victims, shard_id:{}, victim_count:{}",
                  current.shard_id.to_string(), victims.size());

        for (const ReplicaID& rid : victims) {
            ReplicaOr replica_or;
            RETURN_IF_INVALID_STATUS(txn.get_replica(rid, replica_or))
            if (replica_or.is_empty()) continue;
            Replica replica = replica_or.value();
            bool changed = false;
            if (replica.state.desired != ReplicaDesired::ABSENT) {
                replica.state.desired = ReplicaDesired::ABSENT;
                changed = true;
            }
            if (replica.state.phase != ReplicaPhase::DELETING && replica.state.phase != ReplicaPhase::DELETED) {
                replica.state.phase = ReplicaPhase::DELETING;
                changed = true;
            }
            LOG_DEBUG(
                    "[ReplicaGroupMembershipReconciler] remove_members, mark replica absent, replica_id:{}, changed:{}",
                    rid.to_string(), changed);

            if (changed) {
                replica.state.update_ts = func::get_current_ts_ms();
                RETURN_IF_INVALID_STATUS(txn.put_replica(replica))
            }
        }

        RETURN_IF_INVALID_STATUS(txn.put_replica_group(current))
        return Status::OK();
    });
}

Status ReplicaGroupMembershipReconciler::remove_specific_members(const ReplicaGroup& group,
                                                                 const std::vector<ReplicaID>& victims) {
    if (victims.empty()) return Status::OK();

    return ctx_.store->write_with([&](SdmStoreTxn& txn) -> Status {
        ReplicaGroupOr current_or;
        RETURN_IF_INVALID_STATUS(txn.get_replica_group(group.shard_id, current_or))
        if (current_or.is_empty()) return Status::OK();

        std::unordered_set<ReplicaID, ReplicaIDHash> victim_set;
        victim_set.reserve(victims.size());
        for (const ReplicaID& rid : victims) {
            victim_set.insert(rid);
        }

        ReplicaGroup current = current_or.value();
        std::vector<ReplicaID> kept_members;
        kept_members.reserve(current.desired_members.size());
        bool changed_group = false;
        for (const ReplicaID& rid : current.desired_members) {
            if (victim_set.count(rid)) {
                changed_group = true;
                continue;
            }
            kept_members.push_back(rid);
        }
        if (!changed_group) return Status::OK();
        current.desired_members = std::move(kept_members);
        LOG_DEBUG(
                "[ReplicaGroupMembershipReconciler] remove_specific_members, remove victims from desired members, shard_id:{}, victim_count:{}, kept_member_count:{}",
                current.shard_id.to_string(), victims.size(), current.desired_members.size());

        for (const ReplicaID& rid : victims) {
            ReplicaOr replica_or;
            RETURN_IF_INVALID_STATUS(txn.get_replica(rid, replica_or))
            if (replica_or.is_empty()) continue;
            Replica replica = replica_or.value();
            bool changed_replica = false;
            if (replica.state.desired != ReplicaDesired::ABSENT) {
                replica.state.desired = ReplicaDesired::ABSENT;
                changed_replica = true;
            }
            if (replica.state.phase != ReplicaPhase::DELETING && replica.state.phase != ReplicaPhase::DELETED) {
                replica.state.phase = ReplicaPhase::DELETING;
                changed_replica = true;
            }
            LOG_DEBUG(
                    "[ReplicaGroupMembershipReconciler] remove_specific_members, mark replica absent, replica_id:{}, changed:{}",
                    rid.to_string(), changed_replica);

            if (changed_replica) {
                replica.state.update_ts = func::get_current_ts_ms();
                RETURN_IF_INVALID_STATUS(txn.put_replica(replica))
            }
        }

        RETURN_IF_INVALID_STATUS(txn.put_replica_group(current))
        return Status::OK();
    });
}

}  // namespace adviskv::sdm
