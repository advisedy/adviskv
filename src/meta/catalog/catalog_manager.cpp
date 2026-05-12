#include "meta/catalog/catalog_manager.h"

#include <fmt/format.h>

#include <cassert>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>

#include "common/define.h"
#include "common/log.h"
#include "common/status.h"
#include "common/type.h"
#include "meta/persist/meta_persist_engine.h"

namespace adviskv::meta {

CatalogManager::CatalogManager(MetaPersistEngine *persist_engine)
    : persist_engine_(persist_engine) {}

Status CatalogManager::init() {
    if (!persist_engine_) {
        LOG_WARN("persist_engine is nullptr");
        return Status::OK();
    }

    PersistedMetaRecord record;
    Status status = persist_engine_->load_meta(record);
    RETURN_IF_INVALID_STATUS(status)

    std::unique_lock lock(mutex_);

    db_id_allocator_ = IDAllocator<DatabaseID>(record.next_db_id);
    table_id_allocator_ = IDAllocator<TableID>(record.next_table_id);

    for (const auto &[db_id, db_meta] : record.db_meta_map) {
        db_meta_map_[db_id] = db_meta;
        db_name2db_id_[db_meta.db_name] = db_id;
    }

    for (const auto &[table_id, table_meta] : record.table_id2table_meta) {
        table_id2table_meta_[table_id] = table_meta;
        db_table_name2table_id_[table_meta.db_name][table_meta.table_name] =
            table_id;
        db_id2table_ids_[table_meta.db_id].insert(table_id);
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
        return Status::OK();
    }

    PersistedMetaRecord record{
        .db_meta_map = db_meta_map_,
        .table_id2table_meta = table_id2table_meta_,
        .next_db_id = db_id_allocator_.current_id(),
        .next_table_id = table_id_allocator_.current_id()};
    return persist_engine_->save_meta(record);
}

Status CatalogManager::create_db(const CreateDBMetaParam &param,
                                 DBMeta *db_meta) {
    const std::string &db_name = param.db_name;

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
    db_meta_map_[new_db_meta.db_id] = new_db_meta;
    db_name2db_id_[db_name] = new_db_meta.db_id;

    status = persist_meta();
    if (status.fail()) {
        db_meta_map_.erase(new_db_meta.db_id);
        db_name2db_id_.erase(db_name);
        return status;
    }

    if (db_meta != nullptr) {
        *db_meta = new_db_meta;
    }

    return Status::OK();
}

Status CatalogManager::create_table(const CreateTableMetaParam &param,
                                    TableMeta *table_meta) {
    const std::string &db_name = param.db_name;
    const std::string &table_name = param.table_name;

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

    table_id2table_meta_[new_table_meta.table_id] = new_table_meta;

    db_table_name2table_id_[db_name][table_name] = new_table_meta.table_id;

    db_id2table_ids_[db_meta.db_id].insert(new_table_meta.table_id);

    Status persist_status = persist_meta();
    if (persist_status.fail()) {
        table_id2table_meta_.erase(new_table_meta.table_id);
        db_table_name2table_id_[db_name].erase(table_name);
        db_id2table_ids_[db_meta.db_id].erase(new_table_meta.table_id);
        return persist_status;
    }

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
    if (table_set_it != db_id2table_ids_.end() &&
        !table_set_it->second.empty()) {
        return Status{
            StatusCode::INVALID_ARGUMENT,
            fmt::format("db_id:{} still has tables, cannot delete", db_id)};
    }

    DBMeta old_db_meta = db_it->second;
    db_meta_map_.erase(db_it);
    db_name2db_id_.erase(old_db_meta.db_name);
    db_id2table_ids_.erase(db_id);

    Status status = persist_meta();
    if (status.fail()) {
        db_meta_map_[old_db_meta.db_id] = old_db_meta;
        db_name2db_id_[old_db_meta.db_name] = old_db_meta.db_id;
        return status;
    }
    return Status::OK();
}

Status CatalogManager::delete_table(TableID table_id) {
    std::unique_lock lock(mutex_);

    auto table_it = table_id2table_meta_.find(table_id);
    if (table_it == table_id2table_meta_.end()) {
        return Status{StatusCode::TABLE_NOT_FOUND,
                      fmt::format("table_id:{} does not exist", table_id)};
    }

    TableMeta old_table_meta = table_it->second;
    table_id2table_meta_.erase(table_it);

    auto db_table_it = db_table_name2table_id_.find(old_table_meta.db_name);
    if (db_table_it != db_table_name2table_id_.end()) {
        db_table_it->second.erase(old_table_meta.table_name);
        if (db_table_it->second.empty()) {
            db_table_name2table_id_.erase(db_table_it);
        }
    }

    auto db_table_ids_it = db_id2table_ids_.find(old_table_meta.db_id);
    if (db_table_ids_it != db_id2table_ids_.end()) {
        db_table_ids_it->second.erase(old_table_meta.table_id);
    }

    Status status = persist_meta();
    if (status.fail()) {
        table_id2table_meta_[old_table_meta.table_id] = old_table_meta;
        db_table_name2table_id_[old_table_meta.db_name]
                               [old_table_meta.table_name] =
                                   old_table_meta.table_id;
        db_id2table_ids_[old_table_meta.db_id].insert(old_table_meta.table_id);
        return status;
    }
    return Status::OK();
}

Status CatalogManager::get_db(const std::string &db_name, DBMeta *db_meta) {
    std::shared_lock lock(mutex_);
    return lookup_db_by_name(db_name, db_meta);
}

Status CatalogManager::get_table_by_id(TableID table_id,
                                       TableMeta *table_meta) {
    // TODO: 目前get_table_by_id这个接口好像没有什么用，后续如果需要的话再完善
    // std::shared_lock lock(mutex_);
    // return lookup_table_by_id(table_id, &table_meta);
    return Status{StatusCode::NOT_SUPPORTED,
                  "get_table_by_id is not implemented yet"};
}

Status CatalogManager::get_table_by_name(const std::string &db_name,
                                         const std::string &table_name,
                                         TableMeta *table_meta) {
    std::shared_lock lock(mutex_);
    if (Status status = lookup_db_by_name(db_name, nullptr); status.fail()) {
        return status;
    }
    return lookup_table_by_name(db_name, table_name, table_meta);
}

Status CatalogManager::list_tables(const std::string &db_name,
                                   std::vector<TableMeta> *table_meta_list) {
    if (!table_meta_list) {
        return Status{StatusCode::INVALID_ARGUMENT,
                      "table_meta_list is nullptr"};
    }
    table_meta_list->clear();
    std::shared_lock lock(mutex_);
    DBMeta db_meta;
    if (Status status = lookup_db_by_name(db_name, &db_meta); status.fail()) {
        return status;
    }
    auto set_it = db_id2table_ids_.find(db_meta.db_id);
    if (set_it == db_id2table_ids_.end()) {
        return Status::OK();
    }
    for (const auto &table_id : set_it->second) {
        auto it = table_id2table_meta_.find(table_id);
        if (it == table_id2table_meta_.end()) {
            return Status{
                StatusCode::ERROR,
                fmt::format("internal error: table_id:{} should exist",
                            table_id)};
        }
        table_meta_list->push_back(it->second);
    }
    return Status::OK();
}

Status CatalogManager::lookup_db_by_name(const std::string &db_name,
                                         DBMeta *db_meta) {
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

Status CatalogManager::lookup_table_by_name(const std::string &db_name,
                                            const std::string &table_name,
                                            TableMeta *table_meta) {
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

}  // namespace adviskv::meta
