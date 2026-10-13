#include "meta/reconcile/replica_group_plan_reconciler.h"

#include "common/id_allocator.h"
#include "common/log.h"
#include "common/status.h"
#include "meta/store/meta_store.h"
#include "meta/store/meta_store_txn.h"

namespace adviskv::meta {

ReplicaGroupPlanReconciler::ReplicaGroupPlanReconciler(MetaStore* store) : store_(store) {}
// 遍历每一个table，如果table是absent的话，直接replica_count=0，当replica都没了就删了
// 如果是present的话，就遍历table的每一个ReplicaGroup，如果没有group就创建，或者修改target_replica_count为当前table的replica_count
Status ReplicaGroupPlanReconciler::run() {
    if (store_ == nullptr) {
        Status status{StatusCode::INVALID_ARGUMENT, "store is nullptr"};
        LOG_ERROR("[ReplicaGroupPlanReconciler] reconcile skipped, msg={}", status.msg());
        return status;
    }

    std::vector<Table> tables;
    Status status = store_->read_with([&](const MetaStoreTxn& txn) { return txn.list_tables(tables); });
    if (status.fail()) {
        LOG_WARN("[ReplicaGroupPlanReconciler] list tables failed, msg={}", status.msg());
        return status;
    }

    for (const Table& table : tables) {
        if (table.state.phase == TablePhase::DELETED) {
            continue;
        }
        status = reconcile_table(table);
        if (status.fail()) {
            LOG_WARN("[ReplicaGroupPlanReconciler] reconcile failed, table_id={}, msg={}", table.table_id,
                     status.msg());
        }
    }
    return Status::OK();
}

Status ReplicaGroupPlanReconciler::reconcile_table(const Table& table) {
    if (is_absent(table.state.phase)) {
        return reconcile_absent_table(table);
    }
    return reconcile_present_table(table);
}

Status ReplicaGroupPlanReconciler::reconcile_present_table(const Table& table) {
    for (const auto& range : table.spec.shard_ranges) {
        ShardIndex shard_index = range.shard_index;
        Status status = ensure_group_for_shard(table, shard_index);
        if (status.fail()) {
            LOG_WARN(
                    "[ReplicaGroupPlan] ensure group failed, table_id={}, "
                    "shard={}, msg={}",
                    table.table_id, shard_index, status.msg());
        }
    }
    return Status::OK();
}

Status ReplicaGroupPlanReconciler::ensure_group_for_shard(const Table& table, ShardIndex shard_index) {
    ShardID shard_id{table.table_id, shard_index};
    return store_->write_with([&](MetaStoreTxn& txn) -> Status {
        TableOr current;
        RETURN_IF_INVALID_STATUS(txn.get_table(table.table_id, current))
        if (current.is_empty() || is_absent(current->state.phase)) {
            return Status::OK();
        }

        ReplicaGroupOr group_or;
        RETURN_IF_INVALID_STATUS(txn.get_replica_group(shard_id, group_or))

        if (group_or.is_empty()) {
            ReplicaGroup group;
            group.shard_id = shard_id;
            group.mode = ReplicaGroupMode::BOOTSTRAP;
            group.target_replica_count = current->spec.target_replica_count;
            group.seq_allocator = IDAllocator<ReplicaSeq>(0);
            LOG_INFO(
                    "[ReplicaGroupPlanReconciler] ensure_group_for_shard, create replica group, shard_id:{}, target_replica_count:{}",
                    shard_id.to_string(), group.target_replica_count);

            RETURN_IF_INVALID_STATUS(txn.put_replica_group(group))
            return Status::OK();
        }

        if (group_or->target_replica_count != current->spec.target_replica_count) {
            LOG_INFO(
                    "[ReplicaGroupPlanReconciler] ensure_group_for_shard, update target replica count, shard_id:{}, old_target:{}, new_target:{}",
                    shard_id.to_string(), group_or->target_replica_count, current->spec.target_replica_count);

            ReplicaGroup group = group_or.value();
            group.target_replica_count = current->spec.target_replica_count;
            RETURN_IF_INVALID_STATUS(txn.put_replica_group(group))
        }
        return Status::OK();
    });
}

Status ReplicaGroupPlanReconciler::reconcile_absent_table(const Table& table) {
    return store_->write_with([&](MetaStoreTxn& txn) -> Status {
        TableOr current;
        RETURN_IF_INVALID_STATUS(txn.get_table(table.table_id, current))

        if (current.has_value() && current->state.phase != TablePhase::DELETED) {
            // 这个table还在删除过程中，继续处理下面的group
        } else {
            return Status::OK();
        }

        for (const auto& range : current->spec.shard_ranges) {
            ShardIndex shard_index = range.shard_index;
            ShardID shard_id{current->table_id, shard_index};
            ReplicaGroupOr group_or;
            RETURN_IF_INVALID_STATUS(txn.get_replica_group(shard_id, group_or))

            if (group_or.is_empty()) {
                continue;
            }

            if (group_or->target_replica_count != 0) {
                ReplicaGroup group = group_or.value();
                group.target_replica_count = 0;
                RETURN_IF_INVALID_STATUS(txn.put_replica_group(group))
                continue;
            }

            // group_or->target_replica_count == 0
            std::vector<Replica> shard_replicas;
            RETURN_IF_INVALID_STATUS(txn.list_replicas_by_shard(shard_id, shard_replicas))
            if (group_or->desired_members.empty() && shard_replicas.empty()) {
                RETURN_IF_INVALID_STATUS(txn.delete_replica_group(shard_id))
            }
        }
        return Status::OK();
    });
}

}  // namespace adviskv::meta
