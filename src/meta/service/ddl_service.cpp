#include "meta/service/ddl_service.h"
#include "common/status.h"
#include "meta/catalog/catalog_manager.h"
#include <memory>


namespace adviskv{


DdlSerivce::DdlSerivce(CatalogManager* catalog_manager){
    catalog_manager_ = catalog_manager;
    //TODO sdm_client
}


    /*
    //负责一些会涉及到catalog和sdm的操作
    Status create_table(const CreateTableOption& option);
    Status create_db(const CreateDBOption& option);
    Status get_table(const GetTableOption& option, TableMeta& table_meta);
*/

Status DdlSerivce::create_table(const CreateTableOption& option, TableMeta* table_meta){

    // 这种句式应该也可以搞一个宏定义
    // if(Status status = CreateTableOption::validate(option); status.fail()){
    //     return status;
    // }
    RETURN_IF_INVALID_OPTION(option)

    CreateTableMetaOption meta_option{
        .table_name = option.table_name,
        .db_name = option.db_name,
        .replica_count = option.replica_count,
        .shard_count = option.shard_count
    };

    Status status = catalog_manager_->create_table(meta_option, table_meta);

    RETURN_IF_INVALID_STATUS(status)

    //TODO
    // 调用sdm的client。应该会有一个函数去分配table。
    // 这个分配table的函数在sdm里去细讲， 记得得去更新sdm里的TableMetaCache

    return status;

}

Status DdlSerivce::create_db(const CreateDBOption& option, DBMeta* db_meta){
    // if(Status status = CreateDBOption::validate(option); status.fail()){
    //     return status;
    // }

    // if(Status status = option.validate(); status.fail()){
    //     return status;
    // }

    RETURN_IF_INVALID_OPTION(option)

    CreateDBMetaOption meta_option{
        .db_name = option.db_name
    };

    return catalog_manager_->create_db(meta_option, db_meta);
}

Status DdlSerivce::get_table(const GetTableOption& option, TableMeta* table_meta){
    // if(Status status = GetTableOption::validate(option); status.fail()){
    //     return status;
    // }
    RETURN_IF_INVALID_OPTION(option)

    if(option.table_id != -1){
        return catalog_manager_->get_table_by_id(option.table_id, table_meta);
    } else{
        return catalog_manager_->get_table_by_name(option.db_name, option.table_name, table_meta);
    }

}

}