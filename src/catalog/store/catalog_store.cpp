#include "catalog/store/catalog_store.h"

#include <cassert>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>

#include <fmt/format.h>

#include "common/define.h"
#include "common/func.h"
#include "common/log.h"
#include "common/model/type.h"
#include "common/status.h"
#include "catalog/model/model.h"
#include "catalog/persist/i_catalog_persist_engine.h"

namespace adviskv::catalog {

CatalogStore::CatalogStore(ICatalogPersistEngine* persist_engine) : persist_engine_(persist_engine) {}

Status CatalogStore::init() {
    if (!persist_engine_) {
        LOG_WARN("persist_engine is nullptr");
        return Status::OK();
    }

    std::unique_lock lock(mutex_);

    CatalogPersistedRecord record;
    Status status = persist_engine_->load_catalog(record);
    RETURN_IF_INVALID_STATUS(status)

    db_id_allocator_ = IDAllocator<DatabaseID>(record.next_db_id);
    table_id_allocator_ = IDAllocator<TableID>(record.next_table_id);

    for (const auto& [db_id, db_desc] : record.db_desc_map) {
        db_desc_map_[db_id] = db_desc;
        db_name2db_id_[db_desc.db_name] = db_id;
    }

    for (const auto& [table_id, table_desc] : record.table_id2table_desc) {
        table_id2table_desc_[table_id] = table_desc;
        db_id2table_ids_[table_desc.db_id].insert(table_id);

        if (table_desc.state == TableState::DELETED) continue;

        auto& table_name_index = db_table_name2table_id_[table_desc.db_name];
        auto [it, inserted] = table_name_index.emplace(table_desc.table_name, table_id);
        if (!inserted) {  // 按道理讲不应该insert不进去
            return Status{StatusCode::ERROR,
                          fmt::format("catalog has duplicate current table name: db={}, "
                                      "table={}, table_id={}, duplicate_table_id={}",
                                      table_desc.db_name, table_desc.table_name, it->second, table_id)};
        }
    }

    LOG_INFO(
            "catalog store init from disk, db_count={}, table_count={}, "
            "next_db_id={}, next_table_id={}",
            record.db_desc_map.size(), record.table_id2table_desc.size(), record.next_db_id, record.next_table_id);
    return Status::OK();
}

Status CatalogStore::persist_catalog() {
    if (!persist_engine_) {
        return Status::ERROR("persist engine is nullptr");
    }

    CatalogPersistedRecord record;
    record.db_desc_map = db_desc_map_;
    record.table_id2table_desc = table_id2table_desc_;
    record.next_db_id = db_id_allocator_.current_id();
    record.next_table_id = table_id_allocator_.current_id();
    return persist_engine_->save_catalog(record);
}

void CatalogStore::remove_table_name_index(const TableDesc& table_desc) {
    auto db_table_it = db_table_name2table_id_.find(table_desc.db_name);
    if (db_table_it == db_table_name2table_id_.end()) {
        return;
    }
    auto table_it = db_table_it->second.find(table_desc.table_name);
    if (table_it != db_table_it->second.end() && table_it->second == table_desc.table_id) {
        db_table_it->second.erase(table_it);
    }
    if (db_table_it->second.empty()) {
        db_table_name2table_id_.erase(db_table_it);
    }
}

Status CatalogStore::add_table_name_index(const TableDesc& table_desc) {
    if ((table_desc.state == TableState::DELETED)) {
        return Status::OK();
    }
    auto& name_index = db_table_name2table_id_[table_desc.db_name];
    auto [it, inserted] = name_index.emplace(table_desc.table_name, table_desc.table_id);
    if (!inserted && it->second != table_desc.table_id) {
        return Status{StatusCode::ALREADY_EXIST,
                      fmt::format("table_name:{} already exists in db:{}", table_desc.table_name, table_desc.db_name)};
    }
    return Status::OK();
}

Status CatalogStore::create_db(const CreateDBParam& param, DatabaseDesc* db_desc) {
    const std::string& db_name = param.db_name;

    RETURN_IF_INVALID_PARAM(param)

    std::unique_lock lock(mutex_);

    Status status = lookup_db_by_name(db_name, nullptr);
    if (status.ok()) {
        return Status{StatusCode::ALREADY_EXIST, fmt::format("db_name:{} already exists", db_name)};
    }

    if (status.code() != StatusCode::DB_NOT_FOUND) {
        return status;
    }

    DatabaseDesc new_db_desc;
    new_db_desc.db_id = db_id_allocator_.get_next_id();
    new_db_desc.db_name = db_name;
    new_db_desc.zone = param.zone;
    RETURN_IF_INVALID_STATUS(put_db_desc(new_db_desc))

    if (db_desc != nullptr) {
        *db_desc = new_db_desc;
    }

    return Status::OK();
}

Status CatalogStore::create_table(const CreateTableParam& param, TableDesc* table_desc) {
    const std::string& db_name = param.db_name;
    const std::string& table_name = param.table_name;

    RETURN_IF_INVALID_PARAM(param)

    std::unique_lock lock(mutex_);

    DatabaseDesc db_desc;
    Status status = lookup_db_by_name(db_name, &db_desc);

    RETURN_IF_INVALID_STATUS(status)

    status = lookup_table_by_name(db_name, table_name, nullptr);

    if (status.ok()) {
        return Status{StatusCode::ALREADY_EXIST,
                      fmt::format("table_name:{} already exists in db:{}", table_name, db_name)};
    }

    if (status.code() != StatusCode::TABLE_NOT_FOUND) {
        return status;
    }

    TableDesc new_table_desc;

    new_table_desc.table_id = table_id_allocator_.get_next_id();
    new_table_desc.db_name = db_name;
    new_table_desc.table_name = table_name;
    new_table_desc.shard_count = param.shard_count;
    new_table_desc.replica_count = param.replica_count;
    new_table_desc.db_id = db_desc.db_id;
    new_table_desc.resource_pool = param.resource_pool;
    new_table_desc.engine_type = param.engine_type;
    new_table_desc.state = TableState::ADDING;
    new_table_desc.create_ts = func::get_current_ts_ms();
    new_table_desc.update_ts = new_table_desc.create_ts;
    new_table_desc.operation_id = fmt::format("create-table-{}-{}", new_table_desc.table_id, new_table_desc.create_ts);

    RETURN_IF_INVALID_STATUS(put_table_desc(new_table_desc))

    if (table_desc != nullptr) {
        *table_desc = new_table_desc;
    }

    return Status::OK();
}

Status CatalogStore::alter_table_replica_count(const AlterTableReplicaCountParam& param, TableDesc* table_desc) {
    RETURN_IF_INVALID_PARAM(param)

    std::unique_lock lock(mutex_);

    TableDesc old_table_desc;
    RETURN_IF_INVALID_STATUS(lookup_table_by_name(param.db_name, param.table_name, &old_table_desc));

    if (old_table_desc.state == TableState::DELETED) {
        return Status{StatusCode::TABLE_NOT_FOUND,
                      fmt::format("table_name:{}.{} does not exist", param.db_name, param.table_name)};
    }

    if (old_table_desc.state == TableState::ALTERING) {
        if (old_table_desc.replica_count != param.replica_count) {
            LOG_WARN(
                    "[CatalogStore] alter_table_replica_count: table_id:{} is "
                    "already altering replica_count to {}",
                    old_table_desc.table_id, old_table_desc.replica_count);

            return Status::INVALID_ARGUMENT(fmt::format("table_id:{} is already altering replica_count to {}",
                                                        old_table_desc.table_id, old_table_desc.replica_count));
        }
        if (table_desc != nullptr) {
            *table_desc = old_table_desc;
        }
        return Status::OK();
    }

    if (old_table_desc.state != TableState::NORMAL) {
        LOG_WARN(
                "[CatalogStore] alter_table_replica_count: table_id:{} state is "
                "not NORMAL for alter replica_count",
                old_table_desc.table_id);
        return Status::INVALID_ARGUMENT(
                fmt::format("table_id:{} state is not NORMAL for alter replica_count", old_table_desc.table_id));
    }

    if (old_table_desc.replica_count == param.replica_count) {
        LOG_INFO(
                "[CatalogStore] alter_table_replica_count: table_id:{} old "
                "replica count = new replica count");
        if (table_desc != nullptr) {
            *table_desc = old_table_desc;
        }
        return Status::OK();
    }

    TableDesc new_table_desc = old_table_desc;
    new_table_desc.replica_count = param.replica_count;
    new_table_desc.state = TableState::ALTERING;
    new_table_desc.operation_id =
            fmt::format("alter-table-replica-count-{}-{}", new_table_desc.table_id, func::get_current_ts_ms());
    new_table_desc.last_error_msg.clear();
    new_table_desc.update_ts = func::get_current_ts_ms();

    RETURN_IF_INVALID_STATUS(put_table_desc(new_table_desc))

    if (table_desc != nullptr) {
        *table_desc = new_table_desc;
    }
    return Status::OK();
}

Status CatalogStore::delete_db(DatabaseID db_id) {
    std::unique_lock lock(mutex_);

    auto db_it = db_desc_map_.find(db_id);
    if (db_it == db_desc_map_.end()) {
        return Status{StatusCode::DB_NOT_FOUND, fmt::format("db_id:{} does not exist", db_id)};
    }

    auto table_set_it = db_id2table_ids_.find(db_id);
    std::unordered_set<TableID> old_table_ids;
    if (table_set_it != db_id2table_ids_.end()) {
        old_table_ids = table_set_it->second;
        for (const auto table_id : table_set_it->second) {
            auto table_it = table_id2table_desc_.find(table_id);
            if (table_it == table_id2table_desc_.end()) {
                return Status{StatusCode::ERROR, fmt::format("internal error: table_id:{} should exist", table_id)};
            }
            if (table_it->second.state != TableState::DELETED) {
                return Status{StatusCode::INVALID_ARGUMENT, fmt::format("db_id:{} still has tables, cannot "
                                                                        "delete",
                                                                        db_id)};
            }
        }
    }

    DatabaseDesc old_db_desc = db_it->second;
    db_desc_map_.erase(db_it);
    db_name2db_id_.erase(old_db_desc.db_name);
    db_table_name2table_id_.erase(old_db_desc.db_name);
    db_id2table_ids_.erase(db_id);

    Status status = persist_catalog();
    if (status.fail()) {
        db_desc_map_[old_db_desc.db_id] = old_db_desc;
        db_name2db_id_[old_db_desc.db_name] = old_db_desc.db_id;
        if (!old_table_ids.empty()) {
            db_id2table_ids_[old_db_desc.db_id] = std::move(old_table_ids);
        }
        return status;
    }
    return Status::OK();
}

Status CatalogStore::delete_table(TableID table_id, TableDesc* table_desc) {
    std::unique_lock lock(mutex_);

    auto table_it = table_id2table_desc_.find(table_id);
    if (table_it == table_id2table_desc_.end()) {
        return Status{StatusCode::TABLE_NOT_FOUND, fmt::format("table_id:{} does not exist", table_id)};
    }

    TableDesc old_table_desc = table_it->second;
    if (old_table_desc.state == TableState::DROPPING) {
        if (table_desc != nullptr) {
            *table_desc = old_table_desc;
        }
        return Status::OK();
    }

    if (old_table_desc.state == TableState::DELETED) {
        return Status{StatusCode::TABLE_NOT_FOUND, fmt::format("table_id:{} does not exist", table_id)};
    }

    if (old_table_desc.state != TableState::NORMAL) {
        LOG_WARN(
                "[CatalogStore] delete_table: table_id:{} state is not NORMAL "
                "for drop",
                old_table_desc.table_id);
        return Status::INVALID_ARGUMENT(fmt::format("table_id:{} state is not NORMAL for drop", table_id));
    }

    TableDesc new_table_desc = old_table_desc;
    new_table_desc.state = TableState::DROPPING;
    new_table_desc.operation_id = fmt::format("drop-table-{}-{}", table_id, func::get_current_ts_ms());
    new_table_desc.last_error_msg.clear();
    new_table_desc.update_ts = func::get_current_ts_ms();
    table_it->second = new_table_desc;

    Status status = persist_catalog();
    if (status.fail()) {
        table_it->second = old_table_desc;
        return status;
    }
    if (table_desc != nullptr) {
        *table_desc = new_table_desc;
    }
    return Status::OK();
}

Status CatalogStore::update_table_state(TableID table_id, TableState state, const std::string& last_error_msg) {
    std::unique_lock lock(mutex_);

    auto table_it = table_id2table_desc_.find(table_id);
    if (table_it == table_id2table_desc_.end()) {
        return Status{StatusCode::TABLE_NOT_FOUND, fmt::format("table_id:{} does not exist", table_id)};
    }

    TableDesc old_table_desc = table_it->second;
    table_it->second.state = state;
    table_it->second.last_error_msg = last_error_msg;
    table_it->second.update_ts = func::get_current_ts_ms();
    TableDesc new_table_desc = table_it->second;
    remove_table_name_index(old_table_desc);
    Status status = add_table_name_index(new_table_desc);
    if (status.fail()) {
        table_it->second = old_table_desc;
        RETURN_IF_INVALID_STATUS(add_table_name_index(old_table_desc))
        return status;
    }

    status = persist_catalog();
    if (status.fail()) {
        table_it->second = old_table_desc;
        remove_table_name_index(new_table_desc);
        RETURN_IF_INVALID_STATUS(add_table_name_index(old_table_desc))
        return status;
    }
    return Status::OK();
}

Status CatalogStore::get_db(const std::string& db_name, DatabaseDesc* db_desc) {
    std::shared_lock lock(mutex_);
    return lookup_db_by_name(db_name, db_desc);
}

Status CatalogStore::get_table_by_id(TableID table_id, TableDesc* table_desc) {
    std::shared_lock lock(mutex_);
    return lookup_table_by_id(table_id, table_desc);
}

Status CatalogStore::get_table_by_name(const std::string& db_name, const std::string& table_name,
                                         TableDesc* table_desc) {
    std::shared_lock lock(mutex_);
    if (Status status = lookup_db_by_name(db_name, nullptr); status.fail()) {
        return status;
    }
    return lookup_table_by_name(db_name, table_name, table_desc);
}

Status CatalogStore::list_tables(const std::string& db_name, std::vector<TableDesc>* table_desc_list) {
    if (!table_desc_list) {
        return Status{StatusCode::INVALID_ARGUMENT, "table_desc_list is nullptr"};
    }
    table_desc_list->clear();
    std::shared_lock lock(mutex_);
    DatabaseDesc db_desc;
    RETURN_IF_INVALID_STATUS(lookup_db_by_name(db_name, &db_desc))
    auto set_it = db_id2table_ids_.find(db_desc.db_id);
    if (set_it == db_id2table_ids_.end()) {
        return Status::OK();
    }
    for (const auto& table_id : set_it->second) {
        auto it = table_id2table_desc_.find(table_id);
        if (it == table_id2table_desc_.end()) {
            return Status{StatusCode::ERROR, fmt::format("internal error: table_id:{} should exist", table_id)};
        }
        if (it->second.state == TableState::DELETED) {
            continue;
        }
        table_desc_list->push_back(it->second);
    }
    return Status::OK();
}

Status CatalogStore::list_tables_by_state(TableState state, std::vector<TableDesc>* table_desc_list) {
    if (!table_desc_list) {
        return Status{StatusCode::INVALID_ARGUMENT, "table_desc_list is nullptr"};
    }
    table_desc_list->clear();
    std::shared_lock lock(mutex_);
    for (const auto& [_, table_desc] : table_id2table_desc_) {
        UNUSED(_);
        if (table_desc.state == state) {
            table_desc_list->push_back(table_desc);
        }
    }
    return Status::OK();
}

Status CatalogStore::lookup_db_by_name(const std::string& db_name, DatabaseDesc* db_desc) {
    auto it = db_name2db_id_.find(db_name);
    if (it == db_name2db_id_.end()) {
        return Status{StatusCode::DB_NOT_FOUND, fmt::format("db_name:{} does not exist", db_name)};
    }
    if (db_desc == nullptr) {
        return Status::OK();
    }
    DatabaseID db_id = it->second;
    auto db_desc_it = db_desc_map_.find(db_id);
    if (db_desc_it == db_desc_map_.end()) {
        return Status{StatusCode::ERROR, fmt::format("internal error: {} should exist", db_name)};
    }
    *db_desc = db_desc_it->second;
    return Status::OK();
}

Status CatalogStore::lookup_table_by_id(TableID table_id, TableDesc* table_desc) {
    auto table_desc_it = table_id2table_desc_.find(table_id);
    if (table_desc_it == table_id2table_desc_.end()) {
        return Status{StatusCode::TABLE_NOT_FOUND, fmt::format("table_id:{} does not exist", table_id)};
    }
    if (table_desc != nullptr) {
        *table_desc = table_desc_it->second;
    }
    return Status::OK();
}

Status CatalogStore::lookup_table_by_name(const std::string& db_name, const std::string& table_name,
                                            TableDesc* table_desc) {
    auto it = db_table_name2table_id_.find(db_name);
    if (it == db_table_name2table_id_.end()) {
        return Status{StatusCode::TABLE_NOT_FOUND,
                      fmt::format("table_name:{} does not exist in db:{}", table_name, db_name)};
    }
    auto it2 = it->second.find(table_name);
    if (it2 == it->second.end()) {
        return Status{StatusCode::TABLE_NOT_FOUND,
                      fmt::format("table_name:{} does not exist in db:{}", table_name, db_name)};
    }
    if (table_desc == nullptr) {
        return Status::OK();
    }
    TableID table_id = it2->second;
    auto table_desc_it = table_id2table_desc_.find(table_id);
    if (table_desc_it == table_id2table_desc_.end()) {
        return Status{StatusCode::ERROR, fmt::format("internal error: table_id:{} should exist", table_id)};
    }
    *table_desc = table_desc_it->second;
    return Status::OK();
}

Status CatalogStore::put_table_desc(TableDesc new_table_desc) {
    DatabaseDesc db_desc;
    if (auto it = db_desc_map_.find(new_table_desc.db_id); it != db_desc_map_.end()) {
        db_desc = it->second;
    } else {
        return Status::ERROR("");
    }

    table_id2table_desc_[new_table_desc.table_id] = new_table_desc;

    RETURN_IF_INVALID_STATUS(add_table_name_index(new_table_desc))

    db_id2table_ids_[db_desc.db_id].insert(new_table_desc.table_id);

    Status persist_status = persist_catalog();
    if (persist_status.fail()) {
        table_id2table_desc_.erase(new_table_desc.table_id);
        remove_table_name_index(new_table_desc);
        db_id2table_ids_[db_desc.db_id].erase(new_table_desc.table_id);
        return persist_status;
    }
    return Status::OK();
}

Status CatalogStore::put_db_desc(DatabaseDesc new_db_desc) {
    auto old_it = db_desc_map_.find(new_db_desc.db_id);
    bool had_old = old_it != db_desc_map_.end();
    DatabaseDesc old_db_desc;
    if (had_old) {
        old_db_desc = old_it->second;
    }

    auto name_it = db_name2db_id_.find(new_db_desc.db_name);
    if (name_it != db_name2db_id_.end() && name_it->second != new_db_desc.db_id) {
        return Status{StatusCode::ALREADY_EXIST, fmt::format("db_name:{} already exists", new_db_desc.db_name)};
    }

    if (had_old && old_db_desc.db_name != new_db_desc.db_name) {
        db_name2db_id_.erase(old_db_desc.db_name);
    }
    db_desc_map_[new_db_desc.db_id] = new_db_desc;
    db_name2db_id_[new_db_desc.db_name] = new_db_desc.db_id;

    Status persist_status = persist_catalog();
    if (persist_status.fail()) {
        db_desc_map_.erase(new_db_desc.db_id);
        db_name2db_id_.erase(new_db_desc.db_name);
        if (had_old) {
            db_desc_map_[old_db_desc.db_id] = old_db_desc;
            db_name2db_id_[old_db_desc.db_name] = old_db_desc.db_id;
        }
        return persist_status;
    }
    return Status::OK();
}

}  // namespace adviskv::catalog
