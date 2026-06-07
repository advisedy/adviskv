#include "meta/service/sdm_client.h"

#include <grpcpp/support/status.h>

#include "common/define.h"
#include "common/proto/sdm_table_status_proto.h"
#include "common/status.h"
#include "sdm.pb.h"

namespace adviskv::meta {

Status SdmClient::call_place_table(const TableMeta& table_meta) {
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
    grpc::Status status = stub_->PlaceTable(&context, request, &response);

    if (!status.ok()) {
        return Status{
            StatusCode::ERROR,
            fmt::format("call sdm place_table failed, grpc code = {}, msg = {}",
                        static_cast<int>(status.error_code()),
                        status.error_message())};
    }

    RETURN_IF_INVALID_STATUS(decode_base_rsp_status(response.base_rsp()))

    return Status::OK();
}

Status SdmClient::get_table_status(const TableMeta& table_meta,
                                   SdmTableStatus* table_status) {
    rpc::GetTableStatusRequest request;
    request.set_table_id(table_meta.table_id);
    request.set_operation_id(table_meta.operation_id);
    rpc::GetTableStatusResponse response;
    grpc::ClientContext context;
    grpc::Status status = stub_->GetTableStatus(&context, request, &response);

    if (!status.ok()) {
        return Status{
            StatusCode::ERROR,
            fmt::format(
                "call sdm get_table_status failed, grpc code = {}, msg = {}",
                static_cast<int>(status.error_code()), status.error_message())};
    }

    RETURN_IF_INVALID_STATUS(decode_base_rsp_status(response.base_rsp()))

    if (!table_status) return Status::OK();

    table_status->table_id = response.table_id();
    RETURN_IF_INVALID_CONDITION(
        decode_pb_sdm_table_desired(response.desired(), table_status->desired),
        "sdm table desired is not valid")
    RETURN_IF_INVALID_CONDITION(
        decode_pb_sdm_table_phase(response.phase(), table_status->phase),
        "sdm table phase is not valid")
    table_status->last_error_msg = response.last_error_msg();
    table_status->operation_id = response.operation_id();

    return Status::OK();
}

Status SdmClient::call_drop_table(const TableMeta& table_meta) {
    rpc::DropTableRequest request;
    request.set_table_id(table_meta.table_id);
    request.set_operation_id(table_meta.operation_id);
    rpc::DropTableResponse response;
    grpc::ClientContext context;
    grpc::Status status = stub_->DropTable(&context, request, &response);

    if (!status.ok()) {
        return Status{
            StatusCode::ERROR,
            fmt::format("call sdm drop_table failed, grpc code = {}, msg = {}",
                        static_cast<int>(status.error_code()),
                        status.error_message())};
    }
    RETURN_IF_INVALID_STATUS(decode_base_rsp_status(response.base_rsp()))
    return Status::OK();
}

Status SdmClient::call_place_db(const DBMeta& db_meta) {
    rpc::PlaceDBRequest request;
    request.set_db_id(db_meta.db_id);
    request.set_db_name(db_meta.db_name);
    request.set_zone(db_meta.zone);
    rpc::PlaceDBResponse response;
    grpc::ClientContext context;
    grpc::Status status = stub_->PlaceDB(&context, request, &response);

    if (!status.ok()) {
        return Status{
            StatusCode::ERROR,
            fmt::format("call sdm place_db failed, grpc code = {}, msg = {}",
                        static_cast<int>(status.error_code()),
                        status.error_message())};
    }

    RETURN_IF_INVALID_STATUS(decode_base_rsp_status(response.base_rsp()))
    return Status::OK();
}

}  // namespace adviskv::meta