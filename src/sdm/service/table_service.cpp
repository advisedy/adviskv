#include "sdm/service/table_service.h"

#include <algorithm>
#include <unordered_set>

#include <fmt/format.h>

#include "common/define.h"
#include "common/func.h"
#include "common/log.h"
#include "common/status.h"
#include "common/type.h"
#include "sdm/model/sdm_store.h"
#include "sdm/model/sdm_store_txn.h"
#include "sdm/model/store.h"
#include "sdm/model/table_status.h"

namespace adviskv::sdm {

namespace {
bool has_exactly_one_writable_leader(const ShardRoute& route) {
    int leader_count = std::count_if(route.replicas.begin(), route.replicas.end(), [](const RouteEntry& entry) {
        return entry.role == ReplicaRole::LEADER && !entry.ip.empty() && entry.port > 0;
    });
    return leader_count == 1;
}
}  // namespace

TableService::TableService(SdmStore* store) : store_(store) {
}

// 语义，执行创建 table 操作，如果 table 已经存在了，就会返回ALREADY_EXISTS
Status TableService::place_table(const PlaceTableParam& param) {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_NULLPTR(store_, "store is nullptr")

    bool put_table_flag{false};
    Status status = store_->write_with([&](SdmStoreTxn& txn) -> Status {
        TableOr existing;
        RETURN_IF_INVALID_STATUS(txn.get_table(param.table_id, existing))
        // 判断一下是否幂等的行为
        if (existing.has_value()) {
            if (existing->spec.operation_id != param.operation_id) {
                return Status::ALREADY_EXIST(fmt::format("table_id {} already exists with operation_id {}",
                                                         param.table_id, existing->spec.operation_id));
            }
            return Status::OK();
        }

        Table table;
        table.table_id = param.table_id;
        table.spec.table_name = param.table_name;
        table.spec.db_id = param.db_id;
        table.spec.db_name = param.db_name;
        table.spec.shard_count = param.shard_count;
        table.spec.replica_count = param.replica_count;
        table.spec.resource_pool = param.resource_pool;
        table.spec.operation_id = param.operation_id;
        table.state.desired = TableDesired::PRESENT;
        table.state.phase = TablePhase::CREATING;
        table.state.update_ts = func::get_current_ts_ms();

        testhook::crash_point("sdm.place_table.before_put_table");
        RETURN_IF_INVALID_STATUS(txn.put_table(table))
        put_table_flag = true;
        return Status::OK();
    });
    if (status.ok() && put_table_flag) {
        testhook::crash_point("sdm.place_table.after_put_table");
    }
    return status;
}

// 语义，确保 table 不存在
Status TableService::drop_table(const DropTableParam& param) {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_NULLPTR(store_, "store is nullptr")

    Status status = store_->write_with([&](SdmStoreTxn& txn) -> Status {
        TableOr existing;
        RETURN_IF_INVALID_STATUS(txn.get_table(param.table_id, existing))
        // 在删除 table 的语义上，我们定义是确保最终 table 不存在，因为创建
        // table
        // 的时候，我们还可以使用 operation_id 去判断， 但是删除的 table
        // 我们不一定能够追溯到他的 operation_id，所以就改一下语义。
        if (existing.is_empty()) {
            return Status::OK();
        }

        if (existing->state.phase == TablePhase::DELETED && existing->state.desired == TableDesired::ABSENT) {
            return Status::OK(fmt::format("the table has been droped, table_id:{}", existing->table_id));
        }

        if (existing->state.desired == TableDesired::ABSENT) {
            return Status::OK(
                    fmt::format("the table's desired is absent, so it is dropping. table_id:{}", existing->table_id));
        }

        if (existing->state.phase != TablePhase::READY) {
            return Status::ALREADY_EXIST(fmt::format("table_id {} is not READY for drop", param.table_id));
        }

        existing->state.desired = TableDesired::ABSENT;
        existing->state.phase = TablePhase::DELETING;
        existing->spec.operation_id = param.operation_id;
        existing->state.last_error_msg.clear();
        existing->state.update_ts = func::get_current_ts_ms();
        return txn.put_table(*existing);
    });

    // TODO111
    if (status.ok()) {
        LOG_INFO("[TableService] the table ok");
    } else {
        LOG_WARN("[TableService] the table failed");
    }

    return status;
}

Status TableService::alter_table_replica_count(const AlterReplicaCountParam& param) {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_NULLPTR(store_, "store is nullptr")

    return store_->write_with([&](SdmStoreTxn& txn) -> Status {
        TableOr existing;
        RETURN_IF_INVALID_STATUS(txn.get_table(param.table_id, existing))
        if (existing.is_empty()) {
            return Status::TABLE_NOT_FOUND(fmt::format("table_id {} not found", param.table_id));
        }
        if (existing->state.desired != TableDesired::PRESENT || existing->state.phase == TablePhase::DELETING ||
            existing->state.phase == TablePhase::DELETED) {
            return Status::INVALID_ARGUMENT(
                    fmt::format("table_id {} is not present for alter replica_count", param.table_id));
        }
        if (existing->state.phase == TablePhase::FAILED) {
            return Status::INVALID_ARGUMENT(
                    fmt::format("table_id {} is FAILED for alter replica_count", param.table_id));
        }
        if (existing->spec.operation_id == param.operation_id) {
            return Status::OK();
        }
        if (existing->state.phase != TablePhase::READY) {
            return Status::ALREADY_EXIST(
                    fmt::format("table_id {} is not READY for alter replica_count", param.table_id));
        }
        if (existing->spec.replica_count == param.replica_count) {
            return Status::OK();
        }

        existing->spec.replica_count = param.replica_count;
        existing->spec.operation_id = param.operation_id;
        existing->state.phase = TablePhase::RESIZING;
        existing->state.last_error_msg.clear();
        existing->state.update_ts = func::get_current_ts_ms();
        return txn.put_table(*existing);
    });
}

// 语义： 这个接口是给 meta 侧在创建 table 的时候可以及时抓取查看 table
// 的信息而写的接口，并不是专门给用户看的
Status TableService::get_table_status(const GetTableStatusParam& param, Table* out_table) const {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_NULLPTR(store_, "store is nullptr")

    TableOr existing;
    RETURN_IF_INVALID_STATUS(
            store_->read_with([&](const SdmStoreTxn& txn) { return txn.get_table(param.table_id, existing); }))
    if (existing.is_empty()) {
        return Status::TABLE_NOT_FOUND(fmt::format("table_id {} not found", param.table_id));
    }
    if (!param.operation_id.empty() && existing->spec.operation_id != param.operation_id) {
        return Status::INVALID_ARGUMENT(fmt::format("operation_id mismatch for table_id {}", param.table_id));
    }
    if (out_table != nullptr) {
        *out_table = *existing;
    }
    return Status::OK();
}

Status TableService::reconcile_all() {
    RETURN_IF_NULLPTR(store_, "store is nullptr")
    std::vector<Table> tables;
    RETURN_IF_INVALID_STATUS(store_->read_with([&](const SdmStoreTxn& txn) { return txn.list_tables(tables); }))
    for (Table& table : tables) {
        Status status = reconcile_table(table);
        if (status.fail()) {
            LOG_WARN("[TableService] reconcile table failed, table_id={}, msg={}", table.table_id, status.msg());
        }
    }
    return Status::OK();
}

Status TableService::reconcile_table(Table& table) {
    if (table.state.phase == TablePhase::FAILED) {
        return Status::OK();
    } else if (table.state.desired == TableDesired::PRESENT && table.state.phase == TablePhase::READY) {
        return Status::OK();
    } else if (table.state.desired == TableDesired::ABSENT && table.state.phase == TablePhase::DELETED) {
        return Status::OK();
    }
    LOG_DEBUG(
            "[TableService] reconcile_table: name:{}, id:{}, table_desired:{}, "
            "table_phase:{}",
            table.spec.table_name, table.table_id, (int32)table.state.desired, (int32)table.state.phase);

    if (table.state.desired == TableDesired::ABSENT) {
        Status status = finalize_absent_table(table);
        if (status.ok() and table.state.phase == TablePhase::DELETED) {
            LOG_INFO(
                    "[TableService] table delete finish, phase is deleted. "
                    "table_id:{}, "
                    "name:{}",
                    table.table_id, table.spec.table_name);
        }
        return status;
    }
    // else if (table.state.desired == TableDesired::PRESENT)
    switch (table.state.phase) {
        case TablePhase::CREATING: {
            Status status = finalize_creating_table(table);
            if (status.ok() and table.state.phase == TablePhase::READY) {
                LOG_INFO(
                        "[TableService] table create finish, phase is ready. "
                        "table_id:{}, name:{}",
                        table.table_id, table.spec.table_name);
            }
            return status;
        }
        case TablePhase::RESIZING: {
            Status status = finalize_resizing_table(table);
            if (status.ok() and table.state.phase == TablePhase::READY) {
                LOG_INFO(
                        "[TableService] table resize finish, phase is ready. "
                        "table_id:{}, name:{}",
                        table.table_id, table.spec.table_name);
            }
            return status;
        }
        default:
            LOG_WARN(
                    "[TableService] unexpected PRESENT table phase, "
                    "table_id={}, phase={}",
                    table.table_id, to<int32>(table.state.phase));
            return Status::OK();
    }
    return Status::OK();
}

Status TableService::finalize_creating_table(Table& table) {
    return finalize_table_until_ready(table, TablePhase::CREATING);
}

Status TableService::finalize_resizing_table(Table& table) {
    return finalize_table_until_ready(table, TablePhase::RESIZING);
}

Status TableService::finalize_table_until_ready(Table& table, TablePhase waiting_phase) {
    return store_->write_with([&](SdmStoreTxn& txn) -> Status {
        TableOr current;
        RETURN_IF_INVALID_STATUS(txn.get_table(table.table_id, current));
        if (current.is_empty()) {  // TODO
            return Status::OK();
        }

        // TODO111 这个desired真有的存在的必要吗？
        if (current->state.desired != TableDesired::PRESENT || current->state.phase == TablePhase::FAILED ||
            current->state.phase == TablePhase::READY) {
            table = *current;
            return Status::OK();
        }
        if (current->state.phase != waiting_phase) {
            table = *current;
            return Status::OK();
        }

        bool all_shards_ready_flag{false};
        RETURN_IF_INVALID_STATUS(ensure_all_shards_ok(txn, current.value(), all_shards_ready_flag))

        if (current->state.phase == TablePhase::FAILED) {
            current->state.update_ts = func::get_current_ts_ms();
            table = current.value();
            return txn.put_table(current.value());
        }

        current->state.phase = all_shards_ready_flag ? TablePhase::READY : waiting_phase;
        if (all_shards_ready_flag) {
            current->state.last_error_msg.clear();
        }
        current->state.update_ts = func::get_current_ts_ms();
        table = current.value();
        return txn.put_table(*current);
    });
}

Status TableService::ensure_all_shards_ok(SdmStoreTxn& txn, Table& table, bool& all_shards_ok) {
    all_shards_ok = true;
    for (ShardIndex shard_index = 0; shard_index < table.spec.shard_count; ++shard_index) {
        ShardID shard_id{table.table_id, shard_index};

        std::vector<Replica> replicas;
        RETURN_IF_INVALID_STATUS(txn.list_replicas_by_shard(shard_id, replicas))
        if (to<int32>(replicas.size()) != table.spec.replica_count) {
            LOG_WARN(
                    "[TableService] shard not ready: replica count mismatch, "
                    "table_id={}, shard_index={}, replicas.size()={}, "
                    "table.spec.replica_count={}",
                    table.table_id, shard_index, replicas.size(), table.spec.replica_count);
            all_shards_ok = false;
            return Status::OK();
        }

        std::unordered_set<ReplicaID, ReplicaIDHash> ready_replica_ids;
        for (const Replica& replica : replicas) {
            if (replica.state.phase == ReplicaPhase::ERROR) {
                table.state.phase = TablePhase::FAILED;
                table.state.last_error_msg = fmt::format(
                        "replica is in ERROR phase, "
                        "replica.state.last_error_msg:{}",
                        replica.state.last_error_msg);
                all_shards_ok = false;
                return Status::OK();
            }
            if (replica.state.desired != ReplicaDesired::PRESENT || replica.state.phase != ReplicaPhase::READY ||
                !is_raft_voter(replica.state.observed_member_type)) {
                // 必须 desired=PRESENT、phase=READY，并且已经是 Raft voter。
                LOG_WARN(
                        "[TableService] shard not ready: replica not ready, "
                        "table_id={}, shard_index={}, replica_id={}, desired={}, "
                        "phase={}, storage_status={}, role={}, member_type={}, "
                        "term={}, endpoint={}:{}",
                        table.table_id, shard_index, replica.replica_id.to_string(),
                        static_cast<int32>(replica.state.desired), static_cast<int32>(replica.state.phase),
                        static_cast<int32>(replica.state.observed_storage_status),
                        static_cast<int32>(replica.state.observed_raft_role),
                        static_cast<int32>(replica.state.observed_member_type), replica.state.term,
                        replica.state.observed_endpoint.ip, replica.state.observed_endpoint.port);
                all_shards_ok = false;
                return Status::OK();
            }
            ready_replica_ids.insert(replica.replica_id);
        }

        // 这里要去确保 replica_group 也创建完成了
        {
            ReplicaGroupOr group;
            RETURN_IF_INVALID_STATUS(txn.get_replica_group(shard_id, group))
            bool group_mode_ok = !group.is_empty() && (group->mode == ReplicaGroupMode::BOOTSTRAP ||
                                                       group->mode == ReplicaGroupMode::RAFT_RECONFIG);
            if (!group_mode_ok || group->target_replica_count != table.spec.replica_count ||
                to<int32>(group->desired_members.size()) != table.spec.replica_count) {
                if (group.is_empty()) {
                    LOG_WARN(
                            "[TableService] shard not ready: replica group "
                            "mismatch, "
                            "table_id={}, shard_index={}, group_empty=true, "
                            "expected_replica_count={}",
                            table.table_id, shard_index, table.spec.replica_count);
                } else {
                    LOG_WARN(
                            "[TableService] shard not ready: replica group "
                            "mismatch, "
                            "table_id={}, shard_index={}, group_empty=false, "
                            "mode={}, "
                            "target_replica_count={}, desired_members={}, "
                            "expected_replica_count={}",
                            table.table_id, shard_index, to<int32>(group->mode), group->target_replica_count,
                            to<int32>(group->desired_members.size()), table.spec.replica_count);
                }

                all_shards_ok = false;
                return Status::OK();
            }
            for (const ReplicaID& replica_id : group->desired_members) {
                if (ready_replica_ids.find(replica_id) == ready_replica_ids.end()) {
                    LOG_WARN(
                            "[TableService] shard not ready: desired member is "
                            "not ready, table_id={}, shard_index={}, "
                            "replica_id={}",
                            table.table_id, shard_index, replica_id.to_string());
                    all_shards_ok = false;
                    return Status::OK();
                }
            }
        }

        // 确保这个 shard 的 route 没有问题
        ShardRouteOr route;
        RETURN_IF_INVALID_STATUS(txn.get_shard_route(shard_id, route))
        if (route.is_empty() || route->replicas.empty() || !has_exactly_one_writable_leader(*route)) {
            LOG_WARN(
                    "[TableService] shard not ready: route not writable, "
                    "table_id={}, shard_index={}, route_empty={}, entries={}",
                    table.table_id, shard_index, route.is_empty(), route.is_empty() ? -1 : route->replicas.size());
            all_shards_ok = false;
            return Status::OK();
        }
    }
    return Status::OK();
}

Status TableService::finalize_absent_table(Table& table) {
    return store_->write_with([&](SdmStoreTxn& txn) -> Status {
        TableOr current;
        RETURN_IF_INVALID_STATUS(txn.get_table(table.table_id, current))
        if (current.is_empty()) {
            return Status::OK();
        }
        if (current->state.desired != TableDesired::ABSENT || current->state.phase == TablePhase::DELETED ||
            current->state.phase == TablePhase::FAILED) {
            table = *current;
            return Status::OK();
        }

        bool all_shards_deleted{false};
        RETURN_IF_INVALID_STATUS(ensure_all_shards_deleted(txn, current.value(), all_shards_deleted))

        if (current->state.phase == TablePhase::FAILED) {
            current->state.update_ts = func::get_current_ts_ms();
            table = current.value();
            return txn.put_table(current.value());
        }

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

Status TableService::ensure_all_shards_deleted(SdmStoreTxn& txn, Table& table, bool& all_shards_deleted) {
    all_shards_deleted = true;
    for (ShardIndex shard_index = 0; shard_index < table.spec.shard_count; ++shard_index) {
        ShardID shard_id{table.table_id, shard_index};

        std::vector<Replica> replicas;
        RETURN_IF_INVALID_STATUS(txn.list_replicas_by_shard(shard_id, replicas))
        for (const Replica& replica : replicas) {
            if (replica.state.phase == ReplicaPhase::ERROR) {
                table.state.phase = TablePhase::FAILED;
                table.state.last_error_msg = fmt::format("replica delete failed, replica.state.last_error_msg:{}",
                                                         replica.state.last_error_msg);
                all_shards_deleted = false;
                return Status::OK();
            }
        }
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

        ShardRouteOr route;
        RETURN_IF_INVALID_STATUS(txn.get_shard_route(shard_id, route))
        if (!route.is_empty()) {
            all_shards_deleted = false;
            return Status::OK();
        }
    }
    return Status::OK();
}

}  // namespace adviskv::sdm