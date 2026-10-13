#include "meta/reconcile/table_reconciler.h"

#include <unordered_set>

#include <fmt/format.h>

#include "common/define.h"
#include "common/func.h"
#include "common/log.h"
#include "common/model/model.h"
#include "common/status.h"
#include "meta/model/model.h"
#include "meta/store/meta_store.h"
#include "meta/store/meta_store_txn.h"

namespace adviskv::meta {

TableReconciler::TableReconciler(MetaStore* store) : store_(store) {}

Status TableReconciler::run() {
    if (store_ == nullptr) {
        Status status{StatusCode::INVALID_ARGUMENT, "store is nullptr"};
        LOG_ERROR("[TableReconciler] reconcile skipped, msg={}", status.msg());
        return status;
    }
    std::vector<Table> tables;
    Status status = store_->read_with([&](const MetaStoreTxn& txn) { return txn.list_tables(tables); });
    if (status.fail()) {
        LOG_WARN("[TableReconciler] list tables failed, msg={}", status.msg());
        return status;
    }
    for (Table& table : tables) {
        Status status = reconcile_table(table);
        if (status.fail()) {
            LOG_WARN("[TableReconciler] reconcile table failed, table_id={}, msg={}", table.table_id, status.msg());
        }
    }
    return Status::OK();
}

Status TableReconciler::reconcile_table(Table& table) {
    if (is_absent(table.state.phase) && table.state.phase == TablePhase::DELETED) {
        return Status::OK();
    } else if (!is_absent(table.state.phase) && table.state.phase == TablePhase::READY) {
        return Status::OK();
    }
    LOG_DEBUG(
            "[TableReconciler] reconcile_table: name:{}, id:{}, "
            "table_phase:{}",
            table.spec.name, table.table_id, (int32)table.state.phase);

    if (is_absent(table.state.phase)) {
        Status status = finalize_absent_table(table);
        if (status.ok() and table.state.phase == TablePhase::DELETED) {
            LOG_INFO(
                    "[TableReconciler] table delete finish, phase is deleted. "
                    "table_id:{}, "
                    "name:{}",
                    table.table_id, table.spec.name);
        }
        return status;
    }
    switch (table.state.phase) {
        case TablePhase::CREATING: {
            Status status = finalize_creating_table(table);
            if (status.ok() and table.state.phase == TablePhase::READY) {
                LOG_INFO(
                        "[TableReconciler] table create finish, phase is ready. "
                        "table_id:{}, name:{}",
                        table.table_id, table.spec.name);
            }
            return status;
        }
        case TablePhase::RESIZING: {
            Status status = finalize_resizing_table(table);
            if (status.ok() and table.state.phase == TablePhase::READY) {
                LOG_INFO(
                        "[TableReconciler] table resize finish, phase is ready. "
                        "table_id:{}, name:{}",
                        table.table_id, table.spec.name);
            }
            return status;
        }
        default:
            LOG_WARN(
                    "[TableReconciler] unexpected PRESENT table phase, "
                    "table_id={}, phase={}",
                    table.table_id, to<int32>(table.state.phase));
            return Status::OK();
    }
}

Status TableReconciler::finalize_creating_table(Table& table) {
    return finalize_table_until_ready(table, TablePhase::CREATING);
}

Status TableReconciler::finalize_resizing_table(Table& table) {
    return finalize_table_until_ready(table, TablePhase::RESIZING);
}

Status TableReconciler::finalize_table_until_ready(Table& table, TablePhase waiting_phase) {
    return store_->write_with([&](MetaStoreTxn& txn) -> Status {
        TableOr current;
        RETURN_IF_INVALID_STATUS(txn.get_table(table.table_id, current));
        if (current.is_empty()) {
            return Status::OK();
        }

        if (current->state.phase == TablePhase::READY || is_absent(current->state.phase)) {
            table = *current;
            return Status::OK();
        }
        if (current->state.phase != waiting_phase) {
            table = *current;
            return Status::OK();
        }

        bool all_shards_ready_flag{false};
        RETURN_IF_INVALID_STATUS(ensure_all_shards_ok(txn, current.value(), all_shards_ready_flag))

        current->state.phase = all_shards_ready_flag ? TablePhase::READY : waiting_phase;
        if (all_shards_ready_flag) {
            current->state.last_error_msg.clear();
        }
        current->state.update_ts = func::get_current_ts_ms();
        table = current.value();
        return txn.put_table(*current);
    });
}

Status TableReconciler::ensure_all_shards_ok(MetaStoreTxn& txn, Table& table, bool& all_shards_ok) {
    all_shards_ok = true;
    RETURN_IF_INVALID_STATUS(check_shard_ranges_is_valid(table.shard_count(), table.spec.shard_ranges))
    for (const auto& range : table.spec.shard_ranges) {
        ShardIndex shard_index = range.shard_index;
        ShardID shard_id{table.table_id, shard_index};

        std::vector<Replica> replicas;
        RETURN_IF_INVALID_STATUS(txn.list_replicas_by_shard(shard_id, replicas))
        if (to<int32>(replicas.size()) != table.spec.target_replica_count) {
            LOG_WARN(
                    "[TableReconciler] shard not ready: replica count mismatch, "
                    "table_id={}, shard_index={}, replicas.size()={}, "
                    "table.spec.target_replica_count={}",
                    table.table_id, shard_index, replicas.size(), table.spec.target_replica_count);
            all_shards_ok = false;
            return Status::OK();
        }

        std::unordered_set<ReplicaID, ReplicaIDHash> ready_replica_ids;
        for (const Replica& replica : replicas) {
            ReplicaObservedOr observed_or;
            RETURN_IF_INVALID_STATUS(txn.get_replica_observed(replica.replica_id, observed_or))
            ReplicaObserved observed = observed_or.is_empty() ? ReplicaObserved{} : observed_or.value();
            if (replica.state.phase != ReplicaPhase::READY || !is_raft_voter(observed.member_type)) {
                LOG_WARN(
                        "[TableReconciler] shard not ready: replica not ready, "
                        "table_id={}, shard_index={}, replica_id={}, "
                        "phase={}, storage_status={}, role={}, member_type={}, "
                        "term={}, endpoint={}:{}",
                        table.table_id, shard_index, replica.replica_id.to_string(),
                        static_cast<int32>(replica.state.phase),
                        static_cast<int32>(observed.storage_status), static_cast<int32>(observed.raft_role),
                        static_cast<int32>(observed.member_type), observed.term, observed.endpoint.ip,
                        observed.endpoint.port);
                all_shards_ok = false;
                return Status::OK();
            }
            ready_replica_ids.insert(replica.replica_id);
        }

        // 这里要去确保 replica_group 也创建完成了
        {
            ReplicaGroupOr group;
            RETURN_IF_INVALID_STATUS(txn.get_replica_group(shard_id, group))
            if (group.is_empty() || group->target_replica_count != table.spec.target_replica_count ||
                to<int32>(group->desired_members.size()) != table.spec.target_replica_count) {
                if (group.is_empty()) {
                    LOG_WARN(
                            "[TableReconciler] shard not ready: replica group "
                            "mismatch, "
                            "table_id={}, shard_index={}, group_empty=true, "
                            "expected_replica_count={}",
                            table.table_id, shard_index, table.spec.target_replica_count);
                } else {
                    LOG_WARN(
                            "[TableReconciler] shard not ready: replica group "
                            "mismatch, "
                            "table_id={}, shard_index={}, group_empty=false, "
                            "mode={}, "
                            "target_replica_count={}, desired_members={}, "
                            "expected_replica_count={}",
                            table.table_id, shard_index, to<int32>(group->mode), group->target_replica_count,
                            to<int32>(group->desired_members.size()), table.spec.target_replica_count);
                }

                all_shards_ok = false;
                return Status::OK();
            }
            for (const ReplicaID& replica_id : group->desired_members) {
                if (ready_replica_ids.find(replica_id) == ready_replica_ids.end()) {
                    LOG_WARN(
                            "[TableReconciler] shard not ready: desired member is "
                            "not ready, table_id={}, shard_index={}, "
                            "replica_id={}",
                            table.table_id, shard_index, replica_id.to_string());
                    all_shards_ok = false;
                    return Status::OK();
                }
            }
        }

        if (table.spec.target_replica_count == 0) {
            continue;
        }

        // 确保这个 shard 的 route 没有问题
        ShardRouteOr route;
        RETURN_IF_INVALID_STATUS(txn.get_shard_route(shard_id, route))
        if (route.is_empty() || !route->is_writable()) {
            LOG_WARN(
                    "[TableReconciler] shard not ready: route not writable, "
                    "table_id={}, shard_index={}, route_empty={}, entries={}",
                    table.table_id, shard_index, route.is_empty(),
                    route.is_empty() ? to<int>(-1) : to<int>(route->replicas.size()));
            all_shards_ok = false;
            return Status::OK();
        }
    }
    return Status::OK();
}

Status TableReconciler::finalize_absent_table(Table& table) {
    return store_->write_with([&](MetaStoreTxn& txn) -> Status {
        TableOr current;
        RETURN_IF_INVALID_STATUS(txn.get_table(table.table_id, current))
        if (current.is_empty()) {
            return Status::OK();
        }
        if (!is_absent(current->state.phase) || current->state.phase == TablePhase::DELETED) {
            table = *current;
            return Status::OK();
        }

        bool all_shards_deleted{false};
        RETURN_IF_INVALID_STATUS(ensure_all_shards_deleted(txn, current.value(), all_shards_deleted))

        if (!all_shards_deleted) {
            current->state.phase = TablePhase::DELETING;
            current->state.update_ts = func::get_current_ts_ms();
            table = *current;
            return txn.put_table(*current);
        }

        current->state.phase = TablePhase::DELETED;
        current->state.last_error_msg.clear();
        current->state.update_ts = func::get_current_ts_ms();
        table = *current;
        return txn.put_table(*current);
    });
}

Status TableReconciler::ensure_all_shards_deleted(MetaStoreTxn& txn, Table& table, bool& all_shards_deleted) {
    all_shards_deleted = true;
    for (const auto& range : table.spec.shard_ranges) {
        ShardIndex shard_index = range.shard_index;
        ShardID shard_id{table.table_id, shard_index};

        std::vector<Replica> replicas;
        RETURN_IF_INVALID_STATUS(txn.list_replicas_by_shard(shard_id, replicas))
        if (!replicas.empty()) {
            all_shards_deleted = false;
            return Status::OK();
        }

        ReplicaGroupOr group_or;
        RETURN_IF_INVALID_STATUS(txn.get_replica_group(shard_id, group_or))
        if (!group_or.is_empty()) {
            all_shards_deleted = false;
            return Status::OK();
        }

        // 这里就不check是否route存在了，毕竟这个
        // 只是一个缓存，不在的话就是SDK可能访问到
        // 残留的route打过去，但是会返回replica已经不在的error，
        // 这个个人觉得是可以理解的
    }
    return Status::OK();
}

}  // namespace adviskv::meta
