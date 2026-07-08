#include "meta/service/ddl_service.h"

#include "common/define.h"
#include "common/status.h"
#include "meta/catalog/catalog_manager.h"

namespace adviskv::meta {

DdlService::DdlService(CatalogManager* catalog_manager, ISdmClient* sdm_client) {
    catalog_manager_ = catalog_manager;
    sdm_client_ = sdm_client;
}

Status DdlService::create_table(const CreateTableParam& param, TableMeta* table_meta) {
    RETURN_IF_INVALID_PARAM(param)

    TableMeta created_table_meta;
    Status status = catalog_manager_->create_table(param, &created_table_meta);

    RETURN_IF_INVALID_STATUS(status)

    if (!sdm_client_) {
        Status state_status = catalog_manager_->update_table_state(created_table_meta.table_id, TableState::ADDING,
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
        Status state_status = catalog_manager_->update_table_state(created_table_meta.table_id, TableState::ADDING,
                                                                   status.to_string());
        RETURN_IF_INVALID_STATUS(state_status)
        created_table_meta.last_error_msg = status.to_string();
    }

    if (table_meta != nullptr) {
        *table_meta = created_table_meta;
    }
    return Status::OK();
}

Status DdlService::drop_table(const DropTableParam& param, TableMeta* table_meta) {
    RETURN_IF_INVALID_PARAM(param)

    TableMeta table;
    Status status = catalog_manager_->get_table_by_name(param.db_name, param.table_name, &table);
    RETURN_IF_INVALID_STATUS(status)

    if (table.state == TableState::DELETED) {
        return Status{StatusCode::TABLE_NOT_FOUND,
                      fmt::format("table_name:{} does not exist in db:{}", param.table_name, param.db_name)};
    }

    if (table.state == TableState::DROPPING) {
        if (table_meta != nullptr) {
            *table_meta = table;
        }
        return Status::OK();
    }

    RETURN_IF_INVALID_STATUS(catalog_manager_->delete_table(table.table_id, &table))

    if (!sdm_client_) {
        // 这里后续的内容交给reconclier去做，先返回OK。
        const std::string err_msg = "sdm_client is nullptr";

        // 更新一遍持久化那边的last_error_msg
        RETURN_IF_INVALID_STATUS(catalog_manager_->update_table_state(table.table_id, TableState::DROPPING, err_msg))
        table.last_error_msg = err_msg;
        if (table_meta != nullptr) {
            *table_meta = table;
        }
        return Status::OK();
    }

    status = sdm_client_->call_drop_table(table);
    if (status.fail()) {
        RETURN_IF_INVALID_STATUS(
                catalog_manager_->update_table_state(table.table_id, TableState::DROPPING, status.to_string()))
        table.last_error_msg = status.to_string();
    }

    if (table_meta != nullptr) {
        *table_meta = table;
    }
    return Status::OK();
}

Status DdlService::alter_table_replica_count(const AlterTableReplicaCountParam& param, TableMeta* table_meta) {
    RETURN_IF_INVALID_PARAM(param)

    TableMeta table;

    RETURN_IF_INVALID_STATUS(catalog_manager_->alter_table_replica_count(param, &table))

    if (table.state == TableState::NORMAL) {
        if (table_meta != nullptr) {
            *table_meta = table;
        }
        return Status::OK();
    }

    if (!sdm_client_) {
        const std::string err_msg = "sdm_client is nullptr";
        RETURN_IF_INVALID_STATUS(catalog_manager_->update_table_state(table.table_id, TableState::ALTERING, err_msg))
        table.last_error_msg = err_msg;
        if (table_meta != nullptr) {
            *table_meta = table;
        }
        return Status::OK();
    }

    Status status = sdm_client_->call_alter_table_replica_count(table);
    if (status.fail()) {
        RETURN_IF_INVALID_STATUS(
                catalog_manager_->update_table_state(table.table_id, TableState::ALTERING, status.to_string()))
        table.last_error_msg = status.to_string();
    }

    if (table_meta != nullptr) {
        *table_meta = table;
    }
    return Status::OK();
}

Status DdlService::create_db(const CreateDBParam& param, DBMeta* db_meta) {
    RETURN_IF_INVALID_PARAM(param)
    return catalog_manager_->create_db(param, db_meta);
}

Status DdlService::drop_db(const DropDBParam& param, DBMeta* db_meta) {
    RETURN_IF_INVALID_PARAM(param)

    DBMeta db;
    RETURN_IF_INVALID_STATUS(catalog_manager_->get_db(param.db_name, &db))
    RETURN_IF_INVALID_STATUS(catalog_manager_->delete_db(db.db_id))

    if (db_meta != nullptr) {
        *db_meta = db;
    }
    return Status::OK();
}

Status DdlService::get_table(const GetTableParam& param, TableMeta* table_meta) {
    RETURN_IF_INVALID_PARAM(param)

    if (param.use_table_id) {
        return catalog_manager_->get_table_by_id(param.table_id, table_meta);
    } else {
        return catalog_manager_->get_table_by_name(param.db_name, param.table_name, table_meta);
    }
}

}  // namespace adviskv::meta