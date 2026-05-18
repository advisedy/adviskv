#pragma once
#include <atomic>
#include <cstdint>
#include <vector>
#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <unordered_set>
#include "common.pb.h"
#include "common/define.h"
#include "common/type.h"
#include "common/status.h"
#include "meta/catalog/meta_types.h"
// #include "meta/persist/i_meta_persist_engine.h"

namespace adviskv::meta {

class IMetaPersistEngine;

template<typename T>
class IDAllocator{
    static_assert(std::is_integral_v<T>, "IDAllocator<T> requires integral T");
public:

    T get_next_id(){
        return cur_id_++;
    }

    IDAllocator() = default;
    explicit IDAllocator(T start_id):cur_id_(start_id){}

    T current_id() const { return cur_id_; }

private:
    T cur_id_{0};
};

class CatalogManager{

public:
    explicit CatalogManager(IMetaPersistEngine* persist_engine);
    Status init();

    Status create_db(const CreateDBMetaParam& param, DBMeta* db_meta);
    Status create_table(const CreateTableMetaParam& param, TableMeta* table_meta);
    Status delete_db(DatabaseID db_id);
    Status delete_table(TableID table_id, TableMeta* table_meta = nullptr);
    Status update_table_state(TableID table_id, TableState state,
                              const std::string& last_error_msg = "");
    Status get_db(const std::string& db_name, DBMeta* db_meta);
    Status get_table_by_id(TableID table_id, TableMeta* table_meta);
    Status get_table_by_name(const std::string& db_name, const std::string& table_name, TableMeta* table_meta);

    // 注意，这个函数不会把删除的table列出来了
    Status list_tables(const std::string& db_name, std::vector<TableMeta>* table_meta_list);
    Status list_tables_by_state(TableState state,
                                std::vector<TableMeta>* table_meta_list);

private:

    Status persist_meta();
    void remove_table_name_index(const TableMeta& table_meta);
    Status add_table_name_index(const TableMeta& table_meta);

    Status lookup_db_by_name(const std::string& db_name, DBMeta* db_meta);
    Status lookup_table_by_name(const std::string& db_name, const std::string& table_name, TableMeta* table_meta);
    Status lookup_table_by_id(TableID table_id, TableMeta* table_meta);

    IMetaPersistEngine* persist_engine_;

    IDAllocator<DatabaseID> db_id_allocator_;
    IDAllocator<TableID> table_id_allocator_;

    mutable std::shared_mutex mutex_;
    std::unordered_map<TableID, TableMeta> table_id2table_meta_;
    std::unordered_map<std::string, std::unordered_map<std::string, TableID>> db_table_name2table_id_;

    std::unordered_map<DatabaseID, DBMeta> db_meta_map_;
    std::unordered_map<std::string, DatabaseID> db_name2db_id_;

    std::unordered_map<DatabaseID, std::unordered_set<TableID>> db_id2table_ids_;
};

}
