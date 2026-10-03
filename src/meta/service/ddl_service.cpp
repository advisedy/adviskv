#include "meta/service/ddl_service.h"

#include <fmt/format.h>

#include "common/crash_injection.h"
#include "common/func.h"
#include "common/shard_range.h"
#include "meta/store/meta_store.h"
#include "meta/store/meta_store_txn.h"

namespace adviskv::meta {

DdlService::DdlService(MetaStore* store) : store_(store), reconciler_(store) {}

Status DdlService::reconcile_all() { return reconciler_.run(); }

Status DdlService::create_db(const CreateDBParam& param, Database* db) {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_NULLPTR(store_, "store is nullptr")
    Database created;
    RETURN_IF_INVALID_STATUS(store_->write_with([&](MetaStoreTxn& txn) -> Status {
        DatabaseOr existing;
        RETURN_IF_INVALID_STATUS(txn.get_database_by_name(param.db_name, existing))
        if (existing.has_value()) {
            return Status::ALREADY_EXIST(fmt::format("db_name:{} already exists", param.db_name));
        }
        created.db_id = txn.allocate_db_id();
        created.name = param.db_name;
        created.zone = param.zone;
        return txn.put_database(created);
    }))
    if (db) *db = created;
    return Status::OK();
}

Status DdlService::drop_db(const DropDBParam& param, Database* db) {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_NULLPTR(store_, "store is nullptr")
    Database dropped;
    RETURN_IF_INVALID_STATUS(store_->write_with([&](MetaStoreTxn& txn) -> Status {
        DatabaseOr existing;
        RETURN_IF_INVALID_STATUS(txn.get_database_by_name(param.db_name, existing))
        if (existing.is_empty()) {
            return Status::DB_NOT_FOUND(fmt::format("db_name:{} does not exist", param.db_name));
        }
        std::vector<Table> tables;
        RETURN_IF_INVALID_STATUS(txn.list_tables(tables))
        for (const Table& table : tables) {
            if (table.spec.db_id == existing->db_id && table.state.phase != TablePhase::DELETED) {
                return Status::INVALID_ARGUMENT(
                        fmt::format("db_id:{} still has tables, cannot delete", existing->db_id));
            }
        }
        dropped = *existing;
        return txn.delete_database(existing->db_id);
    }))
    if (db) *db = dropped;
    return Status::OK();
}

Status DdlService::create_table(const CreateTableParam& param, Table* table) {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_NULLPTR(store_, "store is nullptr")
    Table created;
    bool committed = false;
    Status status = store_->write_with([&](MetaStoreTxn& txn) -> Status {
        DatabaseOr db;
        RETURN_IF_INVALID_STATUS(txn.get_database_by_name(param.db_name, db))
        if (db.is_empty()) {
            return Status::DB_NOT_FOUND(fmt::format("db_name:{} does not exist", param.db_name));
        }
        TableOr existing;
        Status found = txn.get_table_by_name(param.db_name, param.table_name, existing);
        RETURN_IF_INVALID_STATUS(found)
        if (existing.has_value() && existing->state.phase != TablePhase::DELETED) {
            if (existing->state.phase == TablePhase::DELETING) {
                return Status::ALREADY_EXIST(
                        fmt::format("table_name:{} is deleting in db:{}", param.table_name, param.db_name));
            }
            bool same_spec = existing->spec.db_id == db->db_id && existing->shard_count() == param.shard_count &&
                                   existing->spec.target_replica_count == param.replica_count &&
                                   existing->spec.resource_pool == param.resource_pool &&
                                   existing->spec.engine_type == param.engine_type;
            if (same_spec) {
                created = *existing;
                return Status::OK();
            }
            return Status::ALREADY_EXIST(
                    fmt::format("table_name:{} already exists in db:{}", param.table_name, param.db_name));
        }

        created.table_id = txn.allocate_table_id();
        created.spec.db_id = db->db_id;
        created.spec.db_name = db->name;
        created.spec.name = param.table_name;
        created.spec.engine_type = param.engine_type;
        created.spec.shard_ranges = make_shard_ranges(param.shard_count);
        created.spec.target_replica_count = param.replica_count;
        created.spec.resource_pool = param.resource_pool;
        created.state.phase = TablePhase::CREATING;
        created.state.create_ts = func::get_current_ts_ms();
        created.state.update_ts = created.state.create_ts;
        created.state.operation_id = fmt::format("create-table-{}-{}", created.table_id, created.state.create_ts);
        testhook::crash_point("meta.create_table.before_commit");
        RETURN_IF_INVALID_STATUS(txn.put_table(created))
        committed = true;
        return Status::OK();
    });
    RETURN_IF_INVALID_STATUS(status)
    if (committed) testhook::crash_point("meta.create_table.after_commit");
    if (table) *table = created;
    return Status::OK();
}

Status DdlService::drop_table(const DropTableByNameParam& param, Table* table) {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_NULLPTR(store_, "store is nullptr")
    Table dropped;
    RETURN_IF_INVALID_STATUS(store_->write_with([&](MetaStoreTxn& txn) -> Status {
        DatabaseOr db;
        RETURN_IF_INVALID_STATUS(txn.get_database_by_name(param.db_name, db))
        if (db.is_empty()) {
            return Status::DB_NOT_FOUND(fmt::format("db_name:{} does not exist", param.db_name));
        }
        TableOr existing;
        Status found = txn.get_table_by_name(param.db_name, param.table_name, existing);
        if (found.fail() || existing.is_empty() || existing->state.phase == TablePhase::DELETED) {
            return Status::TABLE_NOT_FOUND(
                    fmt::format("table_name:{} does not exist in db:{}", param.table_name, param.db_name));
        }
        if (existing->state.phase == TablePhase::DELETING) {
            dropped = *existing;
            return Status::OK();
        }
        existing->state.phase = TablePhase::DELETING;
        existing->state.operation_id = fmt::format("drop-table-{}-{}", existing->table_id, func::get_current_ts_ms());
        existing->state.last_error_msg.clear();
        existing->state.update_ts = func::get_current_ts_ms();
        dropped = *existing;
        return txn.put_table(*existing);
    }))
    if (table) *table = dropped;
    return Status::OK();
}

Status DdlService::alter_table_replica_count(const AlterTableReplicaCountParam& param, Table* table) {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_NULLPTR(store_, "store is nullptr")
    Table updated;
    RETURN_IF_INVALID_STATUS(store_->write_with([&](MetaStoreTxn& txn) -> Status {
        DatabaseOr db;
        RETURN_IF_INVALID_STATUS(txn.get_database_by_name(param.db_name, db))
        if (db.is_empty()) {
            return Status::DB_NOT_FOUND(fmt::format("db_name:{} does not exist", param.db_name));
        }
        TableOr existing;
        Status found = txn.get_table_by_name(param.db_name, param.table_name, existing);
        if (found.fail() || existing.is_empty() || existing->state.phase == TablePhase::DELETED) {
            return Status::TABLE_NOT_FOUND(
                    fmt::format("table_name:{}.{} does not exist", param.db_name, param.table_name));
        }
        if (existing->state.phase == TablePhase::RESIZING) {
            if (existing->spec.target_replica_count != param.replica_count) {
                return Status::INVALID_ARGUMENT(fmt::format("table_id:{} is already altering replica_count to {}",
                                                            existing->table_id, existing->spec.target_replica_count));
            }
            updated = *existing;
            return Status::OK();
        }
        if (existing->state.phase != TablePhase::READY) {
            return Status::INVALID_ARGUMENT(
                    fmt::format("table_id:{} state is not READY for alter replica_count", existing->table_id));
        }
        if (existing->spec.target_replica_count == param.replica_count) {
            updated = *existing;
            return Status::OK();
        }
        existing->spec.target_replica_count = param.replica_count;
        existing->state.phase = TablePhase::RESIZING;
        existing->state.operation_id =
                fmt::format("alter-table-replica-count-{}-{}", existing->table_id, func::get_current_ts_ms());
        existing->state.last_error_msg.clear();
        existing->state.update_ts = func::get_current_ts_ms();
        updated = *existing;
        return txn.put_table(*existing);
    }))
    if (table) *table = updated;
    return Status::OK();
}

Status DdlService::get_table(const GetTableParam& param, Table* table) {
    RETURN_IF_INVALID_PARAM(param)
    RETURN_IF_NULLPTR(store_, "store is nullptr")
    RETURN_IF_NULLPTR(table, "table is nullptr")
    TableOr found;
    if (param.use_table_id) {
        RETURN_IF_INVALID_STATUS(
                store_->read_with([&](const MetaStoreTxn& txn) { return txn.get_table(param.table_id, found); }))
    } else {
        RETURN_IF_INVALID_STATUS(store_->read_with([&](const MetaStoreTxn& txn) {
            DatabaseOr db;
            RETURN_IF_INVALID_STATUS(txn.get_database_by_name(param.db_name, db))
            if (db.is_empty()) {
                return Status::DB_NOT_FOUND(fmt::format("db_name:{} does not exist", param.db_name));
            }
            return txn.get_table_by_name(param.db_name, param.table_name, found);
        }))
    }
    if (found.is_empty() || (!param.use_table_id && found->state.phase == TablePhase::DELETED)) {
        return Status::TABLE_NOT_FOUND("table not found");
    }
    *table = *found;
    return Status::OK();
}

}  // namespace adviskv::meta
