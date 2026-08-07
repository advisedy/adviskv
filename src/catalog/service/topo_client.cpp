#include "catalog/service/topo_client.h"

#include <chrono>

#include <grpcpp/support/status.h>

#include "common/define.h"
#include "common/model/type.h"
#include "common/proto/proto.h"
#include "common/status.h"
#include "topo.pb.h"

namespace adviskv::catalog {

Status TopoClient::call_place_table(const TableDesc& table_desc) {
    topo_rpc::PlaceTableRequest request;
    request.set_db_id(table_desc.db_id);
    request.set_table_id(table_desc.table_id);
    request.set_db_name(table_desc.db_name);
    request.set_table_name(table_desc.table_name);
    request.set_shard_count(table_desc.shard_count);
    request.set_replica_count(table_desc.replica_count);
    request.set_resource_pool(table_desc.resource_pool);
    request.set_operation_id(table_desc.operation_id);
    pb::EngineType engine_type_pb;
    RETURN_IF_INVALID_CONDITION(encode_pb_engine_type(table_desc.engine_type, engine_type_pb),
                                "table engine_type is not valid")
    request.set_engine_type(engine_type_pb);
    topo_rpc::PlaceTableResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + Milliseconds(timeout_ms_));
    grpc::Status status = stub_->PlaceTable(&context, request, &response);

    if (!status.ok()) {
        return Status{StatusCode::ERROR, fmt::format("call topo place_table failed, grpc code = {}, msg = {}",
                                                     static_cast<int>(status.error_code()), status.error_message())};
    }

    RETURN_IF_INVALID_STATUS(decode_base_rsp_status(response.base_rsp()))

    return Status::OK();
}

Status TopoClient::get_table_status(const TableDesc& table_desc, TopoTableStatus* table_status) {
    topo_rpc::GetTableStatusRequest request;
    request.set_table_id(table_desc.table_id);
    request.set_operation_id(table_desc.operation_id);
    topo_rpc::GetTableStatusResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + Milliseconds(timeout_ms_));
    grpc::Status status = stub_->GetTableStatus(&context, request, &response);

    if (!status.ok()) {
        return Status{StatusCode::ERROR, fmt::format("call topo get_table_status failed, grpc code = {}, msg = {}",
                                                     static_cast<int>(status.error_code()), status.error_message())};
    }

    RETURN_IF_INVALID_STATUS(decode_base_rsp_status(response.base_rsp()))

    if (!table_status) return Status::OK();

    table_status->table_id = response.table_id();
    RETURN_IF_INVALID_CONDITION(decode_pb_topo_table_desired(response.desired(), table_status->desired),
                                "topo table desired is not valid")
    RETURN_IF_INVALID_CONDITION(decode_pb_topo_table_phase(response.phase(), table_status->phase),
                                "topo table phase is not valid")
    table_status->last_error_msg = response.last_error_msg();
    table_status->operation_id = response.operation_id();

    return Status::OK();
}

Status TopoClient::call_drop_table(const TableDesc& table_desc) {
    topo_rpc::DropTableRequest request;
    request.set_table_id(table_desc.table_id);
    request.set_operation_id(table_desc.operation_id);
    topo_rpc::DropTableResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + Milliseconds(timeout_ms_));
    grpc::Status status = stub_->DropTable(&context, request, &response);

    if (!status.ok()) {
        return Status{StatusCode::ERROR, fmt::format("call topo drop_table failed, grpc code = {}, msg = {}",
                                                     static_cast<int>(status.error_code()), status.error_message())};
    }
    RETURN_IF_INVALID_STATUS(decode_base_rsp_status(response.base_rsp()))
    return Status::OK();
}

Status TopoClient::call_alter_table_replica_count(const TableDesc& table_desc) {
    topo_rpc::AlterTableReplicaCountRequest request;
    request.set_table_id(table_desc.table_id);
    request.set_replica_count(table_desc.replica_count);
    request.set_operation_id(table_desc.operation_id);
    topo_rpc::AlterTableReplicaCountResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + Milliseconds(timeout_ms_));
    grpc::Status status = stub_->AlterTableReplicaCount(&context, request, &response);

    if (!status.ok()) {
        return Status{StatusCode::ERROR, fmt::format("call topo alter_table_replica_count failed, grpc "
                                                     "code = {}, msg = {}",
                                                     static_cast<int>(status.error_code()), status.error_message())};
    }
    RETURN_IF_INVALID_STATUS(decode_base_rsp_status(response.base_rsp()))
    return Status::OK();
}

}  // namespace adviskv::catalog
