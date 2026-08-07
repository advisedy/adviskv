#include "catalog/service/ddl_service.h"

#include "common/define.h"
#include "common/status.h"
#include "catalog/store/catalog_store.h"

namespace adviskv::catalog {

DdlService::DdlService(CatalogStore* catalog_store, ITopoClient* topo_client) {
    catalog_store_ = catalog_store;
    topo_client_ = topo_client;
}

Status DdlService::create_table(const CreateTableParam& param, TableDesc* table_desc) {
    RETURN_IF_INVALID_PARAM(param)

    TableDesc created_table_desc;
    Status status = catalog_store_->create_table(param, &created_table_desc);

    RETURN_IF_INVALID_STATUS(status)

    if (!topo_client_) {
        Status state_status = catalog_store_->update_table_state(created_table_desc.table_id, TableState::ADDING,
                                                                   "topo_client is nullptr");
        RETURN_IF_INVALID_STATUS(state_status)
        created_table_desc.last_error_msg = "topo_client is nullptr";
        if (table_desc != nullptr) {
            *table_desc = created_table_desc;
        }
        return Status::OK();
    }

    status = topo_client_->call_place_table(created_table_desc);
    if (status.fail()) {
        Status state_status = catalog_store_->update_table_state(created_table_desc.table_id, TableState::ADDING,
                                                                   status.to_string());
        RETURN_IF_INVALID_STATUS(state_status)
        created_table_desc.last_error_msg = status.to_string();
    }

    if (table_desc != nullptr) {
        *table_desc = created_table_desc;
    }
    return Status::OK();
}

Status DdlService::drop_table(const DropTableParam& param, TableDesc* table_desc) {
    RETURN_IF_INVALID_PARAM(param)

    TableDesc table;
    Status status = catalog_store_->get_table_by_name(param.db_name, param.table_name, &table);
    RETURN_IF_INVALID_STATUS(status)

    if (table.state == TableState::DELETED) {
        return Status{StatusCode::TABLE_NOT_FOUND,
                      fmt::format("table_name:{} does not exist in db:{}", param.table_name, param.db_name)};
    }

    if (table.state == TableState::DROPPING) {
        if (table_desc != nullptr) {
            *table_desc = table;
        }
        return Status::OK();
    }

    RETURN_IF_INVALID_STATUS(catalog_store_->delete_table(table.table_id, &table))

    if (!topo_client_) {
        // 这里后续的内容交给reconclier去做，先返回OK。
        const std::string err_msg = "topo_client is nullptr";

        // 更新一遍持久化那边的last_error_msg
        RETURN_IF_INVALID_STATUS(catalog_store_->update_table_state(table.table_id, TableState::DROPPING, err_msg))
        table.last_error_msg = err_msg;
        if (table_desc != nullptr) {
            *table_desc = table;
        }
        return Status::OK();
    }

    status = topo_client_->call_drop_table(table);
    if (status.fail()) {
        RETURN_IF_INVALID_STATUS(
                catalog_store_->update_table_state(table.table_id, TableState::DROPPING, status.to_string()))
        table.last_error_msg = status.to_string();
    }

    if (table_desc != nullptr) {
        *table_desc = table;
    }
    return Status::OK();
}

Status DdlService::alter_table_replica_count(const AlterTableReplicaCountParam& param, TableDesc* table_desc) {
    RETURN_IF_INVALID_PARAM(param)

    TableDesc table;

    RETURN_IF_INVALID_STATUS(catalog_store_->alter_table_replica_count(param, &table))

    if (table.state == TableState::NORMAL) {
        if (table_desc != nullptr) {
            *table_desc = table;
        }
        return Status::OK();
    }

    if (!topo_client_) {
        const std::string err_msg = "topo_client is nullptr";
        RETURN_IF_INVALID_STATUS(catalog_store_->update_table_state(table.table_id, TableState::ALTERING, err_msg))
        table.last_error_msg = err_msg;
        if (table_desc != nullptr) {
            *table_desc = table;
        }
        return Status::OK();
    }

    Status status = topo_client_->call_alter_table_replica_count(table);
    if (status.fail()) {
        RETURN_IF_INVALID_STATUS(
                catalog_store_->update_table_state(table.table_id, TableState::ALTERING, status.to_string()))
        table.last_error_msg = status.to_string();
    }

    if (table_desc != nullptr) {
        *table_desc = table;
    }
    return Status::OK();
}

Status DdlService::create_db(const CreateDBParam& param, DatabaseDesc* db_desc) {
    RETURN_IF_INVALID_PARAM(param)
    return catalog_store_->create_db(param, db_desc);
}

Status DdlService::drop_db(const DropDBParam& param, DatabaseDesc* db_desc) {
    RETURN_IF_INVALID_PARAM(param)

    DatabaseDesc db;
    RETURN_IF_INVALID_STATUS(catalog_store_->get_db(param.db_name, &db))
    RETURN_IF_INVALID_STATUS(catalog_store_->delete_db(db.db_id))

    if (db_desc != nullptr) {
        *db_desc = db;
    }
    return Status::OK();
}

Status DdlService::get_table(const GetTableParam& param, TableDesc* table_desc) {
    RETURN_IF_INVALID_PARAM(param)

    if (param.use_table_id) {
        return catalog_store_->get_table_by_id(param.table_id, table_desc);
    } else {
        return catalog_store_->get_table_by_name(param.db_name, param.table_name, table_desc);
    }
}

}  // namespace adviskv::catalog
