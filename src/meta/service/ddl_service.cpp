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
    Status create_table(const CreateTableParam& param);
    Status create_db(const CreateDBParam& param);
    Status get_table(const GetTableParam& param, TableMeta& table_meta);
*/

Status DdlSerivce::create_table(const CreateTableParam& param, TableMeta* table_meta){

    // 这种句式应该也可以搞一个宏定义
    // if(Status status = CreateTableParam::validate(param); status.fail()){
    //     return status;
    // }
    RETURN_IF_INVALID_PARAM(param)

    CreateTableMetaParam meta_param{
        .table_name = param.table_name,
        .db_name = param.db_name,
        .replica_count = param.replica_count,
        .shard_count = param.shard_count
    };

    Status status = catalog_manager_->create_table(meta_param, table_meta);

    RETURN_IF_INVALID_STATUS(status)

    //TODO
    // 调用sdm的client。应该会有一个函数去分配table。
    // 这个分配table的函数在sdm里去细讲， 记得得去更新sdm里的TableMetaCache

    return status;

}

Status DdlSerivce::create_db(const CreateDBParam& param, DBMeta* db_meta){
    // if(Status status = CreateDBParam::validate(param); status.fail()){
    //     return status;
    // }

    // if(Status status = param.validate(); status.fail()){
    //     return status;
    // }

    RETURN_IF_INVALID_PARAM(param)

    CreateDBMetaParam meta_param{
        .db_name = param.db_name
    };

    return catalog_manager_->create_db(meta_param, db_meta);
}

Status DdlSerivce::get_table(const GetTableParam& param, TableMeta* table_meta){
    // if(Status status = GetTableParam::validate(param); status.fail()){
    //     return status;
    // }
    RETURN_IF_INVALID_PARAM(param)

    if(param.table_id != -1){
        return catalog_manager_->get_table_by_id(param.table_id, table_meta);
    } else{
        return catalog_manager_->get_table_by_name(param.db_name, param.table_name, table_meta);
    }

}

}