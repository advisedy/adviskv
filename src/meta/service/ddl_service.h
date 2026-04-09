#pragma once


#include "common/status.h"
#include "meta/catalog/catalog_manager.h"
#include <cstdint>
#include <memory>
#include <string>
#include "common/define.h"

namespace adviskv{

struct SdmClient;



struct CreateDBOption {
    const std::string& db_name;

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!db_name.empty(), "db_name should not empty")
        return Status::OK();
    }
};

struct CreateTableOption {
    const std::string& db_name;
    const std::string& table_name;
    int32_t shard_count;
    int32_t replica_count;

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!db_name.empty(), "db_name should not empty")
        RETURN_IF_INVALID_CONDITION(!table_name.empty(), "table_name should not empty")
        RETURN_IF_INVALID_CONDITION(shard_count > 0, "shard count should > 0")
        RETURN_IF_INVALID_CONDITION(replica_count >= 0, "replica count should >= 0")
        return Status::OK();
    }

};

struct GetTableOption{
    //可选
   const std::string& db_name;
   const std::string& table_name;
   //可选
   int32_t table_id{-1};
   Status validate() const {
    RETURN_IF_INVALID_CONDITION(table_id != -1 or (!db_name.empty() and !table_name.empty()), "please fill table_id or (db_name, table_name)")
    return Status::OK();
}

//    static Status validate(const GetTableOption& option){
//     if(option.table_id!=-1){
//         return Status::OK();
//     }
//     if(option.db_name.empty() or option.table_name.empty()){
//         return Status{StatusCode::INVALID_ARGUMENT, "please fill table_id or (db_name, table_name)"};
//     }
//     return Status::OK();
//    }
};

class DdlSerivce{

public:

    explicit DdlSerivce(CatalogManager* catalog_manager);

    //负责一些会涉及到catalog和sdm的操作
    Status create_table(const CreateTableOption& option, TableMeta* table_meta);
    Status create_db(const CreateDBOption& option, DBMeta* db_meta);
    Status get_table(const GetTableOption& option, TableMeta* table_meta);
private:

    // SdmClient* sdm_client_;
    CatalogManager* catalog_manager_;
};

}