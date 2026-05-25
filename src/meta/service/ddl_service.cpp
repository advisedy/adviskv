#include "meta/service/ddl_service.h"

#include "common/define.h"
#include "common/status.h"
#include "meta/catalog/catalog_manager.h"

namespace adviskv::meta {

DdlService::DdlService(CatalogManager* catalog_manager,
                       ISdmClient* sdm_client) {
    catalog_manager_ = catalog_manager;
    sdm_client_ = sdm_client;
}

/*
//负责一些会涉及到catalog和sdm的操作
Status create_table(const CreateTableParam& param);
Status create_db(const CreateDBParam& param);
Status get_table(const GetTableParam& param, TableMeta& table_meta);
*/

Status DdlService::create_table(const CreateTableParam& param,
                                TableMeta* table_meta) {
    // 这种句式应该也可以搞一个宏定义
    // if(Status status = CreateTableParam::validate(param); status.fail()){
    //     return status;
    // }
    RETURN_IF_INVALID_PARAM(param)

    CreateTableMetaParam meta_param;
    meta_param.db_name = param.db_name;
    meta_param.table_name = param.table_name;
    meta_param.shard_count = param.shard_count;
    meta_param.replica_count = param.replica_count;
    meta_param.resource_pool = param.resource_pool;

    TableMeta created_table_meta;
    Status status =
        catalog_manager_->create_table(meta_param, &created_table_meta);

    RETURN_IF_INVALID_STATUS(status)

    if (!sdm_client_) {
        Status state_status = catalog_manager_->update_table_state(
            created_table_meta.table_id, TableState::ADDING,
            "sdm_client is nullptr");
        RETURN_IF_INVALID_STATUS(state_status)
        created_table_meta.last_error_msg = "sdm_client is nullptr";
        if (table_meta != nullptr) {
            *table_meta = created_table_meta;
        }
        return Status::OK();
    }

    status = sdm_client_->call_place_table(created_table_meta);
    if (status.fail()) {
        Status state_status = catalog_manager_->update_table_state(
            created_table_meta.table_id, TableState::ADDING,
            status.to_string());
        RETURN_IF_INVALID_STATUS(state_status)
        created_table_meta.last_error_msg = status.to_string();
    }

    if (table_meta != nullptr) {
        *table_meta = created_table_meta;
    }
    return Status::OK();
}

Status DdlService::drop_table(const DropTableParam& param,
                              TableMeta* table_meta) {
    RETURN_IF_INVALID_PARAM(param)

    TableMeta table;
    Status status = catalog_manager_->get_table_by_name(
        param.db_name, param.table_name, &table);
    RETURN_IF_INVALID_STATUS(status)

    if (table.state == TableState::DELETED) {
        return Status{StatusCode::TABLE_NOT_FOUND,
                      fmt::format("table_name:{} does not exist in db:{}",
                                  param.table_name, param.db_name)};
    }

    if (table.state == TableState::DROPPING) {
        if (table_meta != nullptr) {
            *table_meta = table;
        }
        return Status::OK();
    }

    RETURN_IF_INVALID_STATUS(catalog_manager_->delete_table(table.table_id,
                                                            &table))

    if (!sdm_client_) {
        // 这里后续的内容交给reconclier去做，先返回OK。
        const std::string err_msg = "sdm_client is nullptr";

        //更新一遍持久化那边的last_error_msg
        RETURN_IF_INVALID_STATUS(catalog_manager_->update_table_state(
            table.table_id, TableState::DROPPING, err_msg))
        table.last_error_msg = err_msg;
        if (table_meta != nullptr) {
            *table_meta = table;
        }
        return Status::OK();
    }

    status = sdm_client_->call_drop_table(table);
    if (status.fail()) {
        RETURN_IF_INVALID_STATUS(catalog_manager_->update_table_state(
            table.table_id, TableState::DROPPING, status.to_string()))
        table.last_error_msg = status.to_string();
    }

    if (table_meta != nullptr) {
        *table_meta = table;
    }
    return Status::OK();
}

Status DdlService::create_db(const CreateDBParam& param, DBMeta* db_meta) {
    // if(Status status = CreateDBParam::validate(param); status.fail()){
    //     return status;
    // }

    // if(Status status = param.validate(); status.fail()){
    //     return status;
    // }

    RETURN_IF_INVALID_PARAM(param)

    CreateDBMetaParam meta_param;
    meta_param.db_name = param.db_name;
    meta_param.zone = param.zone;

    DBMeta created_db_meta;
    Status status = catalog_manager_->create_db(meta_param, &created_db_meta);

    RETURN_IF_INVALID_STATUS(status)

    if (!sdm_client_) {
        Status rollback_status =
            catalog_manager_->delete_db(created_db_meta.db_id);
        RETURN_IF_INVALID_STATUS(rollback_status)
        return Status{StatusCode::ERROR, "sdm_client is nullptr"};
    }

    status = sdm_client_->call_place_db(created_db_meta);
    if (status.fail()) {
        Status rollback_status =
            catalog_manager_->delete_db(created_db_meta.db_id);
        if (rollback_status.fail()) {
            return Status{
                StatusCode::ERROR,
                fmt::format("call sdm place_db failed: {}; rollback db "
                            "failed: {}",
                            status.to_string(), rollback_status.to_string())};
        }
        return status;
    }

    if (db_meta != nullptr) {
        *db_meta = created_db_meta;
    }
    return Status::OK();
}

Status DdlService::get_table(const GetTableParam& param,
                             TableMeta* table_meta) {
    RETURN_IF_INVALID_PARAM(param)

    if (param.use_table_id) {
        return catalog_manager_->get_table_by_id(param.table_id, table_meta);
    } else {
        return catalog_manager_->get_table_by_name(
            param.db_name, param.table_name, table_meta);
    }
}

}  // namespace adviskv::meta