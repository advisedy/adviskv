#include "meta/service/sdm_client.h"

#include <chrono>

#include <grpcpp/support/status.h>

#include "common/define.h"
#include "common/model/type.h"
#include "common/proto/proto.h"
#include "common/status.h"
#include "sdm.pb.h"

namespace adviskv::meta {

Status SdmClient::call_place_table(const TableMeta& table_meta) {
    sdm_rpc::PlaceTableRequest request;
    request.set_db_id(table_meta.db_id);
    request.set_table_id(table_meta.table_id);
    request.set_db_name(table_meta.db_name);
    request.set_table_name(table_meta.table_name);
    request.set_shard_count(table_meta.shard_count);
    request.set_replica_count(table_meta.replica_count);
    request.set_resource_pool(table_meta.resource_pool);
    request.set_operation_id(table_meta.operation_id);
    pb::EngineType engine_type_pb;
    RETURN_IF_INVALID_CONDITION(encode_pb_engine_type(table_meta.engine_type, engine_type_pb),
                                "table engine_type is not valid")
    request.set_engine_type(engine_type_pb);
    sdm_rpc::PlaceTableResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + Milliseconds(timeout_ms_));
    grpc::Status status = stub_->PlaceTable(&context, request, &response);

    if (!status.ok()) {
        return Status{StatusCode::ERROR, fmt::format("call sdm place_table failed, grpc code = {}, msg = {}",
                                                     static_cast<int>(status.error_code()), status.error_message())};
    }

    RETURN_IF_INVALID_STATUS(decode_base_rsp_status(response.base_rsp()))

    return Status::OK();
}

Status SdmClient::get_table_status(const TableMeta& table_meta, SdmTableStatus* table_status) {
    sdm_rpc::GetTableStatusRequest request;
    request.set_table_id(table_meta.table_id);
    request.set_operation_id(table_meta.operation_id);
    sdm_rpc::GetTableStatusResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + Milliseconds(timeout_ms_));
    grpc::Status status = stub_->GetTableStatus(&context, request, &response);

    if (!status.ok()) {
        return Status{StatusCode::ERROR, fmt::format("call sdm get_table_status failed, grpc code = {}, msg = {}",
                                                     static_cast<int>(status.error_code()), status.error_message())};
    }

    RETURN_IF_INVALID_STATUS(decode_base_rsp_status(response.base_rsp()))

    if (!table_status) return Status::OK();

    table_status->table_id = response.table_id();
    RETURN_IF_INVALID_CONDITION(decode_pb_sdm_table_desired(response.desired(), table_status->desired),
                                "sdm table desired is not valid")
    RETURN_IF_INVALID_CONDITION(decode_pb_sdm_table_phase(response.phase(), table_status->phase),
                                "sdm table phase is not valid")
    table_status->last_error_msg = response.last_error_msg();
    table_status->operation_id = response.operation_id();

    return Status::OK();
}

Status SdmClient::call_drop_table(const TableMeta& table_meta) {
    sdm_rpc::DropTableRequest request;
    request.set_table_id(table_meta.table_id);
    request.set_operation_id(table_meta.operation_id);
    sdm_rpc::DropTableResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + Milliseconds(timeout_ms_));
    grpc::Status status = stub_->DropTable(&context, request, &response);

    if (!status.ok()) {
        return Status{StatusCode::ERROR, fmt::format("call sdm drop_table failed, grpc code = {}, msg = {}",
                                                     static_cast<int>(status.error_code()), status.error_message())};
    }
    RETURN_IF_INVALID_STATUS(decode_base_rsp_status(response.base_rsp()))
    return Status::OK();
}

Status SdmClient::call_alter_table_replica_count(const TableMeta& table_meta) {
    sdm_rpc::AlterTableReplicaCountRequest request;
    request.set_table_id(table_meta.table_id);
    request.set_replica_count(table_meta.replica_count);
    request.set_operation_id(table_meta.operation_id);
    sdm_rpc::AlterTableReplicaCountResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + Milliseconds(timeout_ms_));
    grpc::Status status = stub_->AlterTableReplicaCount(&context, request, &response);

    if (!status.ok()) {
        return Status{StatusCode::ERROR, fmt::format("call sdm alter_table_replica_count failed, grpc "
                                                     "code = {}, msg = {}",
                                                     static_cast<int>(status.error_code()), status.error_message())};
    }
    RETURN_IF_INVALID_STATUS(decode_base_rsp_status(response.base_rsp()))
    return Status::OK();
}

}  // namespace adviskv::meta
