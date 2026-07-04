#include "meta/catalog/catalog_manager.h"

#include <fmt/format.h>

#include <cassert>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>

#include "common/define.h"
#include "common/func.h"
#include "common/log.h"
#include "common/status.h"
#include "common/type.h"
#include "meta/catalog/meta_types.h"
#include "meta/persist/i_meta_persist_engine.h"

namespace adviskv::meta {

CatalogManager::CatalogManager(IMetaPersistEngine* persist_engine)
    : persist_engine_(persist_engine) {}

Status CatalogManager::init() {
    if (!persist_engine_) {
        LOG_WARN("persist_engine is nullptr");
        return Status::OK();
    }

    std::unique_lock lock(mutex_);

    PersistedMetaRecord record;
    Status status = persist_engine_->load_meta(record);
    RETURN_IF_INVALID_STATUS(status)

    db_id_allocator_ = IDAllocator<DatabaseID>(record.next_db_id);
    table_id_allocator_ = IDAllocator<TableID>(record.next_table_id);

    for (const auto& [db_id, db_meta] : record.db_meta_map) {
        db_meta_map_[db_id] = db_meta;
        db_name2db_id_[db_meta.db_name] = db_id;
    }

    for (const auto& [table_id, table_meta] : record.table_id2table_meta) {
        table_id2table_meta_[table_id] = table_meta;
        db_id2table_ids_[table_meta.db_id].insert(table_id);

        if (table_meta.state == TableState::DELETED) continue;

        auto& table_name_index = db_table_name2table_id_[table_meta.db_name];
        auto [it, inserted] =
            table_name_index.emplace(table_meta.table_name, table_id);
        if (!inserted) {  // 按道理讲不应该insert不进去
            return Status{
                StatusCode::ERROR,
                fmt::format("catalog has duplicate current table name: db={}, "
                            "table={}, table_id={}, duplicate_table_id={}",
                            table_meta.db_name, table_meta.table_name,
                            it->second, table_id)};
        }
    }

    LOG_INFO(
        "catalog manager init from disk, db_count={}, table_count={}, "
        "next_db_id={}, next_table_id={}",
        record.db_meta_map.size(), record.table_id2table_meta.size(),
        record.next_db_id, record.next_table_id);
    return Status::OK();
}

Status CatalogManager::persist_meta() {
    if (!persist_engine_) {
        return Status::ERROR("persist engine is nullptr");
    }

    PersistedMetaRecord record;
    record.db_meta_map = db_meta_map_;
    record.table_id2table_meta = table_id2table_meta_;
    record.next_db_id = db_id_allocator_.current_id();
    record.next_table_id = table_id_allocator_.current_id();
    return persist_engine_->save_meta(record);
}

void CatalogManager::remove_table_name_index(const TableMeta& table_meta) {
    auto db_table_it = db_table_name2table_id_.find(table_meta.db_name);
    if (db_table_it == db_table_name2table_id_.end()) {
        return;
    }
    auto table_it = db_table_it->second.find(table_meta.table_name);
    if (table_it != db_table_it->second.end() &&
        table_it->second == table_meta.table_id) {
        db_table_it->second.erase(table_it);
    }
    if (db_table_it->second.empty()) {
        db_table_name2table_id_.erase(db_table_it);
    }
}

Status CatalogManager::add_table_name_index(const TableMeta& table_meta) {
    if ((table_meta.state == TableState::DELETED)) {
        return Status::OK();
    }
    auto& name_index = db_table_name2table_id_[table_meta.db_name];
    auto [it, inserted] =
        name_index.emplace(table_meta.table_name, table_meta.table_id);
    if (!inserted && it->second != table_meta.table_id) {
        return Status{StatusCode::ALREADY_EXIST,
                      fmt::format("table_name:{} already exists in db:{}",
                                  table_meta.table_name, table_meta.db_name)};
    }
    return Status::OK();
}

Status CatalogManager::create_db(const CreateDBParam& param,
                                 DBMeta* db_meta) {
    const std::string& db_name = param.db_name;

    RETURN_IF_INVALID_PARAM(param)

    std::unique_lock lock(mutex_);

    Status status = lookup_db_by_name(db_name, nullptr);
    if (status.ok()) {
        return Status{StatusCode::ALREADY_EXIST,
                      fmt::format("db_name:{} already exists", db_name)};
    }

    if (status.code() != StatusCode::DB_NOT_FOUND) {
        return status;
    }

    DBMeta new_db_meta;
    new_db_meta.db_id = db_id_allocator_.get_next_id();
    new_db_meta.db_name = db_name;
    new_db_meta.zone = param.zone;
    RETURN_IF_INVALID_STATUS(put_db_meta(new_db_meta))

    if (db_meta != nullptr) {
        *db_meta = new_db_meta;
    }

    return Status::OK();
}

Status CatalogManager::create_table(const CreateTableParam& param,
                                    TableMeta* table_meta) {
    const std::string& db_name = param.db_name;
    const std::string& table_name = param.table_name;

    RETURN_IF_INVALID_PARAM(param)

    std::unique_lock lock(mutex_);

    DBMeta db_meta;
    Status status = lookup_db_by_name(db_name, &db_meta);

    RETURN_IF_INVALID_STATUS(status)

    status = lookup_table_by_name(db_name, table_name, nullptr);

    if (status.ok()) {
        return Status{StatusCode::ALREADY_EXIST,
                      fmt::format("table_name:{} already exists in db:{}",
                                  table_name, db_name)};
    }

    if (status.code() != StatusCode::TABLE_NOT_FOUND) {
        return status;
    }

    TableMeta new_table_meta;

    new_table_meta.table_id = table_id_allocator_.get_next_id();
    new_table_meta.db_name = db_name;
    new_table_meta.table_name = table_name;
    new_table_meta.shard_count = param.shard_count;
    new_table_meta.replica_count = param.replica_count;
    new_table_meta.db_id = db_meta.db_id;
    new_table_meta.resource_pool = param.resource_pool;
    new_table_meta.state = TableState::ADDING;
    new_table_meta.create_ts = func::get_current_ts_ms();
    new_table_meta.update_ts = new_table_meta.create_ts;
    new_table_meta.operation_id =
        fmt::format("create-table-{}-{}", new_table_meta.table_id,
                    new_table_meta.create_ts);

    RETURN_IF_INVALID_STATUS(put_table_meta(new_table_meta))

    if (table_meta != nullptr) {
        *table_meta = new_table_meta;
    }

    return Status::OK();
}

Status CatalogManager::alter_table_replica_count(
    const AlterTableReplicaCountParam& param, TableMeta* table_meta) {
    RETURN_IF_INVALID_PARAM(param)

    std::unique_lock lock(mutex_);

    TableMeta old_table_meta;
    RETURN_IF_INVALID_STATUS(
        lookup_table_by_name(param.db_name, param.table_name, &old_table_meta));

    if (old_table_meta.state == TableState::DELETED) {
        return Status{StatusCode::TABLE_NOT_FOUND,
                      fmt::format("table_name:{}.{} does not exist",
                                  param.db_name, param.table_name)};
    }

    if (old_table_meta.state == TableState::ALTERING) {
        if (old_table_meta.replica_count != param.replica_count) {
            LOG_WARN(
                "[CatalogManager] alter_table_replica_count: table_id:{} is "
                "already altering replica_count to {}",
                old_table_meta.table_id, old_table_meta.replica_count);

            return Status::INVALID_ARGUMENT(fmt::format(
                "table_id:{} is already altering replica_count to {}",
                old_table_meta.table_id, old_table_meta.replica_count));
        }
        if (table_meta != nullptr) {
            *table_meta = old_table_meta;
        }
        return Status::OK();
    }

    if (old_table_meta.state != TableState::NORMAL) {
        LOG_WARN(
            "[CatalogManager] alter_table_replica_count: table_id:{} state is "
            "not NORMAL for alter replica_count",
            old_table_meta.table_id);
        return Status::INVALID_ARGUMENT(fmt::format(
            "table_id:{} state is not NORMAL for alter replica_count",
            old_table_meta.table_id));
    }

    if (old_table_meta.replica_count == param.replica_count) {
        LOG_INFO(
            "[CatalogManager] alter_table_replica_count: table_id:{} old "
            "replica count = new replica count");
        if (table_meta != nullptr) {
            *table_meta = old_table_meta;
        }
        return Status::OK();
    }

    TableMeta new_table_meta = old_table_meta;
    new_table_meta.replica_count = param.replica_count;
    new_table_meta.state = TableState::ALTERING;
    new_table_meta.operation_id =
        fmt::format("alter-table-replica-count-{}-{}", new_table_meta.table_id,
                    func::get_current_ts_ms());
    new_table_meta.last_error_msg.clear();
    new_table_meta.update_ts = func::get_current_ts_ms();

    RETURN_IF_INVALID_STATUS(put_table_meta(new_table_meta))

    if (table_meta != nullptr) {
        *table_meta = new_table_meta;
    }
    return Status::OK();
}

Status CatalogManager::delete_db(DatabaseID db_id) {
    std::unique_lock lock(mutex_);

    auto db_it = db_meta_map_.find(db_id);
    if (db_it == db_meta_map_.end()) {
        return Status{StatusCode::DB_NOT_FOUND,
                      fmt::format("db_id:{} does not exist", db_id)};
    }

    auto table_set_it = db_id2table_ids_.find(db_id);
    std::unordered_set<TableID> old_table_ids;
    if (table_set_it != db_id2table_ids_.end()) {
        old_table_ids = table_set_it->second;
        for (const auto table_id : table_set_it->second) {
            auto table_it = table_id2table_meta_.find(table_id);
            if (table_it == table_id2table_meta_.end()) {
                return Status{
                    StatusCode::ERROR,
                    fmt::format("internal error: table_id:{} should exist",
                                table_id)};
            }
            if (table_it->second.state != TableState::DELETED) {
                return Status{StatusCode::INVALID_ARGUMENT,
                              fmt::format("db_id:{} still has tables, cannot "
                                          "delete",
                                          db_id)};
            }
        }
    }

    DBMeta old_db_meta = db_it->second;
    db_meta_map_.erase(db_it);
    db_name2db_id_.erase(old_db_meta.db_name);
    db_table_name2table_id_.erase(old_db_meta.db_name);
    db_id2table_ids_.erase(db_id);

    Status status = persist_meta();
    if (status.fail()) {
        db_meta_map_[old_db_meta.db_id] = old_db_meta;
        db_name2db_id_[old_db_meta.db_name] = old_db_meta.db_id;
        if (!old_table_ids.empty()) {
            db_id2table_ids_[old_db_meta.db_id] = std::move(old_table_ids);
        }
        return status;
    }
    return Status::OK();
}

Status CatalogManager::delete_table(TableID table_id, TableMeta* table_meta) {
    std::unique_lock lock(mutex_);

    auto table_it = table_id2table_meta_.find(table_id);
    if (table_it == table_id2table_meta_.end()) {
        return Status{StatusCode::TABLE_NOT_FOUND,
                      fmt::format("table_id:{} does not exist", table_id)};
    }

    TableMeta old_table_meta = table_it->second;
    if (old_table_meta.state == TableState::DROPPING) {
        if (table_meta != nullptr) {
            *table_meta = old_table_meta;
        }
        return Status::OK();
    }

    if (old_table_meta.state == TableState::DELETED) {
        return Status{StatusCode::TABLE_NOT_FOUND,
                      fmt::format("table_id:{} does not exist", table_id)};
    }

    if (old_table_meta.state != TableState::NORMAL) {
        LOG_WARN(
            "[CatalogManager] delete_table: table_id:{} state is not NORMAL "
            "for drop",
            old_table_meta.table_id);
        return Status::INVALID_ARGUMENT(fmt::format(
            "table_id:{} state is not NORMAL for drop", table_id));
    }

    TableMeta new_table_meta = old_table_meta;
    new_table_meta.state = TableState::DROPPING;
    new_table_meta.operation_id =
        fmt::format("drop-table-{}-{}", table_id, func::get_current_ts_ms());
    new_table_meta.last_error_msg.clear();
    new_table_meta.update_ts = func::get_current_ts_ms();
    table_it->second = new_table_meta;

    Status status = persist_meta();
    if (status.fail()) {
        table_it->second = old_table_meta;
        return status;
    }
    if (table_meta != nullptr) {
        *table_meta = new_table_meta;
    }
    return Status::OK();
}

Status CatalogManager::update_table_state(TableID table_id, TableState state,
                                          const std::string& last_error_msg) {
    std::unique_lock lock(mutex_);

    auto table_it = table_id2table_meta_.find(table_id);
    if (table_it == table_id2table_meta_.end()) {
        return Status{StatusCode::TABLE_NOT_FOUND,
                      fmt::format("table_id:{} does not exist", table_id)};
    }

    TableMeta old_table_meta = table_it->second;
    table_it->second.state = state;
    table_it->second.last_error_msg = last_error_msg;
    table_it->second.update_ts = func::get_current_ts_ms();
    TableMeta new_table_meta = table_it->second;
    remove_table_name_index(old_table_meta);
    Status status = add_table_name_index(new_table_meta);
    if (status.fail()) {
        table_it->second = old_table_meta;
        RETURN_IF_INVALID_STATUS(add_table_name_index(old_table_meta))
        return status;
    }

    status = persist_meta();
    if (status.fail()) {
        table_it->second = old_table_meta;
        remove_table_name_index(new_table_meta);
        RETURN_IF_INVALID_STATUS(add_table_name_index(old_table_meta))
        return status;
    }
    return Status::OK();
}

Status CatalogManager::get_db(const std::string& db_name, DBMeta* db_meta) {
    std::shared_lock lock(mutex_);
    return lookup_db_by_name(db_name, db_meta);
}

Status CatalogManager::get_table_by_id(TableID table_id,
                                       TableMeta* table_meta) {
    std::shared_lock lock(mutex_);
    return lookup_table_by_id(table_id, table_meta);
}

Status CatalogManager::get_table_by_name(const std::string& db_name,
                                         const std::string& table_name,
                                         TableMeta* table_meta) {
    std::shared_lock lock(mutex_);
    if (Status status = lookup_db_by_name(db_name, nullptr); status.fail()) {
        return status;
    }
    return lookup_table_by_name(db_name, table_name, table_meta);
}

Status CatalogManager::list_tables(const std::string& db_name,
                                   std::vector<TableMeta>* table_meta_list) {
    if (!table_meta_list) {
        return Status{StatusCode::INVALID_ARGUMENT,
                      "table_meta_list is nullptr"};
    }
    table_meta_list->clear();
    std::shared_lock lock(mutex_);
    DBMeta db_meta;
    RETURN_IF_INVALID_STATUS(lookup_db_by_name(db_name, &db_meta))
    auto set_it = db_id2table_ids_.find(db_meta.db_id);
    if (set_it == db_id2table_ids_.end()) {
        return Status::OK();
    }
    for (const auto& table_id : set_it->second) {
        auto it = table_id2table_meta_.find(table_id);
        if (it == table_id2table_meta_.end()) {
            return Status{
                StatusCode::ERROR,
                fmt::format("internal error: table_id:{} should exist",
                            table_id)};
        }
        if (it->second.state == TableState::DELETED) {
            continue;
        }
        table_meta_list->push_back(it->second);
    }
    return Status::OK();
}

Status CatalogManager::list_tables_by_state(
    TableState state, std::vector<TableMeta>* table_meta_list) {
    if (!table_meta_list) {
        return Status{StatusCode::INVALID_ARGUMENT,
                      "table_meta_list is nullptr"};
    }
    table_meta_list->clear();
    std::shared_lock lock(mutex_);
    for (const auto& [_, table_meta] : table_id2table_meta_) {
        UNUSED(_);
        if (table_meta.state == state) {
            table_meta_list->push_back(table_meta);
        }
    }
    return Status::OK();
}

Status CatalogManager::lookup_db_by_name(const std::string& db_name,
                                         DBMeta* db_meta) {
    auto it = db_name2db_id_.find(db_name);
    if (it == db_name2db_id_.end()) {
        return Status{StatusCode::DB_NOT_FOUND,
                      fmt::format("db_name:{} does not exist", db_name)};
    }
    if (db_meta == nullptr) {
        return Status::OK();
    }
    DatabaseID db_id = it->second;
    auto db_meta_it = db_meta_map_.find(db_id);
    if (db_meta_it == db_meta_map_.end()) {
        return Status{StatusCode::ERROR,
                      fmt::format("internal error: {} should exist", db_name)};
    }
    *db_meta = db_meta_it->second;
    return Status::OK();
}

Status CatalogManager::lookup_table_by_id(TableID table_id,
                                          TableMeta* table_meta) {
    auto table_meta_it = table_id2table_meta_.find(table_id);
    if (table_meta_it == table_id2table_meta_.end()) {
        return Status{StatusCode::TABLE_NOT_FOUND,
                      fmt::format("table_id:{} does not exist", table_id)};
    }
    if (table_meta != nullptr) {
        *table_meta = table_meta_it->second;
    }
    return Status::OK();
}

Status CatalogManager::lookup_table_by_name(const std::string& db_name,
                                            const std::string& table_name,
                                            TableMeta* table_meta) {
    auto it = db_table_name2table_id_.find(db_name);
    if (it == db_table_name2table_id_.end()) {
        return Status{StatusCode::TABLE_NOT_FOUND,
                      fmt::format("table_name:{} does not exist in db:{}",
                                  table_name, db_name)};
    }
    auto it2 = it->second.find(table_name);
    if (it2 == it->second.end()) {
        return Status{StatusCode::TABLE_NOT_FOUND,
                      fmt::format("table_name:{} does not exist in db:{}",
                                  table_name, db_name)};
    }
    if (table_meta == nullptr) {
        return Status::OK();
    }
    TableID table_id = it2->second;
    auto table_meta_it = table_id2table_meta_.find(table_id);
    if (table_meta_it == table_id2table_meta_.end()) {
        return Status{
            StatusCode::ERROR,
            fmt::format("internal error: table_id:{} should exist", table_id)};
    }
    *table_meta = table_meta_it->second;
    return Status::OK();
}

Status CatalogManager::put_table_meta(TableMeta new_table_meta) {
    DBMeta db_meta;
    if (auto it = db_meta_map_.find(new_table_meta.db_id);
        it != db_meta_map_.end()) {
        db_meta = it->second;
    } else {
        return Status::ERROR("");
    }

    table_id2table_meta_[new_table_meta.table_id] = new_table_meta;

    RETURN_IF_INVALID_STATUS(add_table_name_index(new_table_meta))

    db_id2table_ids_[db_meta.db_id].insert(new_table_meta.table_id);

    Status persist_status = persist_meta();
    if (persist_status.fail()) {
        table_id2table_meta_.erase(new_table_meta.table_id);
        remove_table_name_index(new_table_meta);
        db_id2table_ids_[db_meta.db_id].erase(new_table_meta.table_id);
        return persist_status;
    }
    return Status::OK();
}

Status CatalogManager::put_db_meta(DBMeta new_db_meta) {
    auto old_it = db_meta_map_.find(new_db_meta.db_id);
    bool had_old = old_it != db_meta_map_.end();
    DBMeta old_db_meta;
    if (had_old) {
        old_db_meta = old_it->second;
    }

    auto name_it = db_name2db_id_.find(new_db_meta.db_name);
    if (name_it != db_name2db_id_.end() &&
        name_it->second != new_db_meta.db_id) {
        return Status{StatusCode::ALREADY_EXIST,
                      fmt::format("db_name:{} already exists",
                                  new_db_meta.db_name)};
    }

    if (had_old && old_db_meta.db_name != new_db_meta.db_name) {
        db_name2db_id_.erase(old_db_meta.db_name);
    }
    db_meta_map_[new_db_meta.db_id] = new_db_meta;
    db_name2db_id_[new_db_meta.db_name] = new_db_meta.db_id;

    Status persist_status = persist_meta();
    if (persist_status.fail()) {
        db_meta_map_.erase(new_db_meta.db_id);
        db_name2db_id_.erase(new_db_meta.db_name);
        if (had_old) {
            db_meta_map_[old_db_meta.db_id] = old_db_meta;
            db_name2db_id_[old_db_meta.db_name] = old_db_meta.db_id;
        }
        return persist_status;
    }
    return Status::OK();
}

}  // namespace adviskv::meta