#pragma once
#include <atomic>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <unordered_set>
#include "common.pb.h"
#include "common/type.h"
#include "common/status.h"

namespace adviskv{

struct TableMeta{
    TableID table_id;
    int32_t shard_count;
    int32_t replica_count;
    DatabaseID db_id;
    std::string db_name;
    std::string table_name;
};

struct DBMeta{
    DatabaseID db_id;
    std::string db_name;
};

struct CreateDBMetaOption {
    std::string db_name;
    static Status validate(const CreateDBMetaOption& option){
        if(option.db_name.empty()){
            return Status{StatusCode::INVALID_ARGUMENT, "db_name is empty"};
        }
        return Status::OK();
    }
};

struct CreateTableMetaOption {
    std::string db_name;
    std::string table_name;
    int32_t shard_count;
    int32_t replica_count;

    static Status validate(const CreateTableMetaOption& option){
        if(option.db_name.empty()){
            return Status{StatusCode::INVALID_ARGUMENT, "db_name is empty"};
        }
        if(option.table_name.empty()){
            return Status{StatusCode::INVALID_ARGUMENT, "table_name is empty"};
        }
        if(option.shard_count <= 0){
            return Status{StatusCode::INVALID_ARGUMENT, "shard_count should be greater than 0"};
        }
        if(option.replica_count < 0){
            return Status{StatusCode::INVALID_ARGUMENT, "replica_count should be greater than or equal to 0"};
        }
        return Status::OK();
    }
};


template<typename T>
class IDAllocator{
    static_assert(std::is_integral_v<T>, "IDAllocator<T> requires integral T");
public:

    T get_next_id(){
        return cur_id_++;
    }

    IDAllocator() = default;
    explicit IDAllocator(T start_id):cur_id_(start_id){}

private:
    T cur_id_{0};
};

class CatalogManager{

public:
    CatalogManager() = default;
    Status create_db(const CreateDBMetaOption& option, DBMeta* db_meta);
    Status create_table(const CreateTableMetaOption& option, TableMeta* table_meta);
    Status get_db(const std::string& db_name, DBMeta* db_meta);
    Status get_table_by_id(TableID table_id, TableMeta* table_meta);
    Status get_table_by_name(const std::string& db_name, const std::string& table_name, TableMeta* table_meta);
    Status list_tables(const std::string& db_name, std::vector<TableMeta>* table_meta_list);

private:

    Status lookup_db_by_name(const std::string& db_name, DBMeta* db_meta);
    //这个函数只check table相关的内容，不会先check一下db
    Status lookup_table_by_name(const std::string& db_name, const std::string& table_name, TableMeta* table_meta);
    Status lookup_table_by_id(TableID table_id, TableMeta* table_meta);

    IDAllocator<DatabaseID> db_id_allocator_;
    IDAllocator<TableID> table_id_allocator_;

    mutable std::shared_mutex mutex_;
    std::unordered_map<TableID, TableMeta> table_id2table_meta_;
    std::unordered_map<std::string, std::unordered_map<std::string, TableID>> db_table_name2table_id_; //key是db_name + table_name
    // std::unordered_map<std::string, TableID> db_table_name2table_id_; //db_table_name的格式: db_name.table_name

    std::unordered_map<DatabaseID, DBMeta> db_meta_map_;
    std::unordered_map<std::string, DatabaseID> db_name2db_id_;

    std::unordered_map<DatabaseID, std::unordered_set<TableID>> db_id2table_ids_;
};

}