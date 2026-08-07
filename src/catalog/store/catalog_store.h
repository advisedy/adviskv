#pragma once
#include <atomic>
#include <cstdint>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common.pb.h"
#include "common/define.h"
#include "common/id_allocator.h"
#include "common/model/type.h"
#include "common/status.h"
#include "catalog/model/model.h"
#include "catalog/model/param.h"
// #include "catalog/persist/i_catalog_persist_engine.h"

namespace adviskv::catalog {

class ICatalogPersistEngine;

class CatalogStore {
public:
    explicit CatalogStore(ICatalogPersistEngine* persist_engine);
    Status init();

    Status create_db(const CreateDBParam& param, DatabaseDesc* db_desc);
    Status create_table(const CreateTableParam& param, TableDesc* table_desc);
    Status alter_table_replica_count(const AlterTableReplicaCountParam& param, TableDesc* table_desc);
    Status delete_db(DatabaseID db_id);
    Status delete_table(TableID table_id, TableDesc* table_desc = nullptr);
    Status update_table_state(TableID table_id, TableState state, const std::string& last_error_msg = "");
    Status get_db(const std::string& db_name, DatabaseDesc* db_desc);
    Status get_table_by_id(TableID table_id, TableDesc* table_desc);
    Status get_table_by_name(const std::string& db_name, const std::string& table_name, TableDesc* table_desc);

    // 注意，这个函数不会把删除的table列出来了
    Status list_tables(const std::string& db_name, std::vector<TableDesc>* table_desc_list);
    Status list_tables_by_state(TableState state, std::vector<TableDesc>* table_desc_list);

private:
    Status persist_catalog();
    void remove_table_name_index(const TableDesc& table_desc);
    Status add_table_name_index(const TableDesc& table_desc);

    Status lookup_db_by_name(const std::string& db_name, DatabaseDesc* db_desc);
    Status lookup_table_by_name(const std::string& db_name, const std::string& table_name, TableDesc* table_desc);
    Status lookup_table_by_id(TableID table_id, TableDesc* table_desc);

    Status put_table_desc(TableDesc table_desc);
    Status put_db_desc(DatabaseDesc db_desc);

    ICatalogPersistEngine* persist_engine_;

    IDAllocator<DatabaseID> db_id_allocator_;
    IDAllocator<TableID> table_id_allocator_;

    mutable std::shared_mutex mutex_;
    std::unordered_map<TableID, TableDesc> table_id2table_desc_;
    std::unordered_map<std::string, std::unordered_map<std::string, TableID>> db_table_name2table_id_;

    std::unordered_map<DatabaseID, DatabaseDesc> db_desc_map_;
    std::unordered_map<std::string, DatabaseID> db_name2db_id_;

    std::unordered_map<DatabaseID, std::unordered_set<TableID>> db_id2table_ids_;
};

}  // namespace adviskv::catalog