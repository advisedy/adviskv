#include "meta/service/ddl_service.h"

#include <grpcpp/support/status.h>

#include <memory>

#include "common/define.h"
#include "common/status.h"
#include "meta/catalog/catalog_manager.h"
#include "sdm.grpc.pb.h"
#include "sdm.pb.h"

namespace adviskv::meta {

DdlService::DdlService(CatalogManager* catalog_manager, SdmClient* sdm_client) {
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

    CreateTableMetaParam meta_param{
        .table_name = param.table_name,
        .db_name = param.db_name,
        .replica_count = param.replica_count,
        .shard_count = param.shard_count,
        .resource_pool = param.resource_pool,
    };

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

Status DdlService::create_db(const CreateDBParam& param, DBMeta* db_meta) {
    // if(Status status = CreateDBParam::validate(param); status.fail()){
    //     return status;
    // }

    // if(Status status = param.validate(); status.fail()){
    //     return status;
    // }

    RETURN_IF_INVALID_PARAM(param)

    CreateDBMetaParam meta_param{.db_name = param.db_name, .zone = param.zone};

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
    // if(Status status = GetTableParam::validate(param); status.fail()){
    //     return status;
    // }
    RETURN_IF_INVALID_PARAM(param)

    if (param.table_id != -1) {
        return catalog_manager_->get_table_by_id(param.table_id, table_meta);
    } else {
        return catalog_manager_->get_table_by_name(
            param.db_name, param.table_name, table_meta);
    }
}

Status SdmClient::call_place_table(const TableMeta& table_meta) {
    SdmClientStub& sdm_client = this->client();

    rpc::PlaceTableRequest request;
    request.set_db_id(table_meta.db_id);
    request.set_table_id(table_meta.table_id);
    request.set_db_name(table_meta.db_name);
    request.set_table_name(table_meta.table_name);
    request.set_shard_count(table_meta.shard_count);
    request.set_replica_count(table_meta.replica_count);
    request.set_resource_pool(table_meta.resource_pool);
    request.set_operation_id(table_meta.operation_id);
    rpc::PlaceTableResponse response;
    grpc::ClientContext context;
    grpc::Status status = sdm_client->PlaceTable(&context, request, &response);

    if (!status.ok()) {
        return Status{
            StatusCode::ERROR,
            fmt::format("call sdm place_table failed, grpc code = {}, msg = {}",
                        (int)status.error_code(), status.error_message())};
    }

    if (response.mutable_base_rsp()->code() != to_rpc_code(StatusCode::OK)) {
        return Status{
            static_cast<StatusCode>(response.mutable_base_rsp()->code()),
            response.mutable_base_rsp()->msg()};
    }
    return Status::OK();
}

Status SdmClient::get_table_status(const TableMeta& table_meta,
                                   SdmTableStatus* table_status) {
    SdmClientStub& sdm_client = this->client();

    rpc::GetTableStatusRequest request;
    request.set_table_id(table_meta.table_id);
    request.set_operation_id(table_meta.operation_id);
    rpc::GetTableStatusResponse response;
    grpc::ClientContext context;
    grpc::Status status =
        sdm_client->GetTableStatus(&context, request, &response);

    if (!status.ok()) {
        return Status{
            StatusCode::ERROR,
            fmt::format(
                "call sdm get_table_status failed, grpc code = {}, msg = {}",
                static_cast<int>(status.error_code()), status.error_message())};
    }

    if (response.mutable_base_rsp()->code() != to_rpc_code(StatusCode::OK)) {
        return Status{
            static_cast<StatusCode>(response.mutable_base_rsp()->code()),
            response.mutable_base_rsp()->msg()};
    }

    if (!table_status) return Status::OK();

    table_status->table_id = response.table_id();
    table_status->desired = response.desired();
    table_status->phase = response.phase();
    table_status->last_error_msg = response.last_error_msg();
    table_status->operation_id = response.operation_id();

    return Status::OK();
}

Status SdmClient::call_drop_table(const TableMeta& table_meta) {
    SdmClientStub& sdm_client = this->client();

    rpc::DropTableRequest request;
    request.set_table_id(table_meta.table_id);
    request.set_operation_id(table_meta.operation_id);
    rpc::DropTableResponse response;
    grpc::ClientContext context;
    grpc::Status status = sdm_client->DropTable(&context, request, &response);

    if (!status.ok()) {
        return Status{
            StatusCode::ERROR,
            fmt::format("call sdm drop_table failed, grpc code = {}, msg = {}",
                        static_cast<int>(status.error_code()),
                        status.error_message())};
    }
    if (response.mutable_base_rsp()->code() != to_rpc_code(StatusCode::OK)) {
        return Status{
            static_cast<StatusCode>(response.mutable_base_rsp()->code()),
            response.mutable_base_rsp()->msg()};
    }
    return Status::OK();
}

Status SdmClient::call_place_db(const DBMeta& db_meta) {
    SdmClientStub& sdm_client = this->client();

    rpc::PlaceDBRequest request;
    request.set_db_id(db_meta.db_id);
    request.set_db_name(db_meta.db_name);
    request.set_zone(db_meta.zone);
    rpc::PlaceDBResponse response;
    grpc::ClientContext context;
    grpc::Status status = sdm_client->PlaceDB(&context, request, &response);

    if (!status.ok()) {
        return Status{
            StatusCode::ERROR,
            fmt::format("call sdm place_db failed, grpc code = {}, msg = {}",
                        (int)status.error_code(), status.error_message())};
    }

    if (response.mutable_base_rsp()->code() != to_rpc_code(StatusCode::OK)) {
        return Status{
            static_cast<StatusCode>(response.mutable_base_rsp()->code()),
            response.mutable_base_rsp()->msg()};
    }
    return Status::OK();
}

}  // namespace adviskv::meta