#include "direct_meta_client.h"

#include <chrono>

#include <fmt/format.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include "common/define.h"
#include "common/proto/proto.h"
#include "meta/proto/table_state_proto.h"

namespace adviskv::cli {

DirectMetaClient::DirectMetaClient(const MetaCliTarget& target) : target_(target) {
    const std::string endpoint = fmt::format("{}:{}", target_.endpoint.ip, target_.endpoint.port);
    auto channel = grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials());
    stub_ = meta_rpc::MetaService::NewStub(channel);
}

Status DirectMetaClient::create_db(const std::string& db_name, const std::string& zone, DatabaseID* db_id) const {
    RETURN_IF_INVALID_PARAM(target_)
    RETURN_IF_INVALID_CONDITION(db_id != nullptr, "db_id should not be nullptr")

    meta_rpc::CreateDBRequest request;
    request.set_db_name(db_name);
    request.set_zone(zone);

    meta_rpc::CreateDBResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + Milliseconds(target_.timeout_ms));

    grpc::Status grpc_status = stub_->CreateDB(&context, request, &response);
    if (!grpc_status.ok()) {
        return Status::ERROR(fmt::format("Meta CreateDB RPC failed, grpc code = {}, msg = {}",
                                         static_cast<int>(grpc_status.error_code()), grpc_status.error_message()));
    }
    RETURN_IF_INVALID_STATUS(decode_base_rsp_status(response.base_rsp()))
    *db_id = response.db_id();
    return Status::OK();
}

Status DirectMetaClient::drop_db(const std::string& db_name, DatabaseID* db_id) const {
    RETURN_IF_INVALID_PARAM(target_)
    RETURN_IF_INVALID_CONDITION(db_id != nullptr, "db_id should not be nullptr")

    meta_rpc::DropDBRequest request;
    request.set_db_name(db_name);

    meta_rpc::DropDBResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + Milliseconds(target_.timeout_ms));

    grpc::Status grpc_status = stub_->DropDB(&context, request, &response);
    if (!grpc_status.ok()) {
        return Status::ERROR(fmt::format("Meta DropDB RPC failed, grpc code = {}, msg = {}",
                                         static_cast<int>(grpc_status.error_code()), grpc_status.error_message()));
    }
    RETURN_IF_INVALID_STATUS(decode_base_rsp_status(response.base_rsp()))
    *db_id = response.db_id();
    return Status::OK();
}

Status DirectMetaClient::create_table(const std::string& db_name, const std::string& table_name, int32_t shard_count,
                                      int32_t replica_count, TableID* table_id, std::string resource,
                                      EngineType engine_type) const {
    RETURN_IF_INVALID_PARAM(target_)
    RETURN_IF_INVALID_CONDITION(table_id != nullptr, "table_id should not be nullptr")

    meta_rpc::CreateTableRequest request;
    request.set_db_name(db_name);
    request.set_table_name(table_name);
    request.set_shard_count(shard_count);
    request.set_replica_count(replica_count);
    request.set_resource_pool(resource);
    pb::EngineType engine_type_pb;
    RETURN_IF_INVALID_CONDITION(encode_pb_engine_type(engine_type, engine_type_pb), "engine_type is not valid")
    request.set_engine_type(engine_type_pb);

    meta_rpc::CreateTableResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + Milliseconds(target_.timeout_ms));

    grpc::Status grpc_status = stub_->CreateTable(&context, request, &response);
    if (!grpc_status.ok()) {
        return Status::ERROR(fmt::format("Meta CreateTable RPC failed, grpc code = {}, msg = {}",
                                         static_cast<int>(grpc_status.error_code()), grpc_status.error_message()));
    }
    RETURN_IF_INVALID_STATUS(decode_base_rsp_status(response.base_rsp()))
    *table_id = response.table_id();
    return Status::OK();
}

Status DirectMetaClient::alter_table_replica_count(const std::string& db_name, const std::string& table_name,
                                                   int32_t replica_count, TableID* table_id) const {
    RETURN_IF_INVALID_PARAM(target_)
    RETURN_IF_INVALID_CONDITION(table_id != nullptr, "table_id should not be nullptr")

    meta_rpc::AlterTableReplicaCountRequest request;
    request.set_db_name(db_name);
    request.set_table_name(table_name);
    request.set_replica_count(replica_count);

    meta_rpc::AlterTableReplicaCountResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + Milliseconds(target_.timeout_ms));

    grpc::Status grpc_status = stub_->AlterTableReplicaCount(&context, request, &response);
    if (!grpc_status.ok()) {
        return Status::ERROR(fmt::format("Meta AlterTableReplicaCount RPC failed, grpc code = {}, msg = {}",
                                         static_cast<int>(grpc_status.error_code()), grpc_status.error_message()));
    }
    RETURN_IF_INVALID_STATUS(decode_base_rsp_status(response.base_rsp()))
    *table_id = response.table_id();
    return Status::OK();
}

Status DirectMetaClient::get_table(const std::string& db_name, const std::string& table_name,
                                   TableInfo* table_info) const {
    RETURN_IF_INVALID_PARAM(target_)
    RETURN_IF_INVALID_CONDITION(table_info != nullptr, "table_info should not be nullptr")

    meta_rpc::GetTableRequest request;
    request.set_db_name(db_name);
    request.set_table_name(table_name);

    meta_rpc::GetTableResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + Milliseconds(target_.timeout_ms));

    grpc::Status grpc_status = stub_->GetTable(&context, request, &response);
    if (!grpc_status.ok()) {
        return Status::ERROR(fmt::format("Meta GetTable RPC failed, grpc code = {}, msg = {}",
                                         static_cast<int>(grpc_status.error_code()), grpc_status.error_message()));
    }
    RETURN_IF_INVALID_STATUS(decode_base_rsp_status(response.base_rsp()))
    table_info->db_id = response.db_id();
    table_info->table_id = response.table_id();
    table_info->shard_count = response.shard_count();
    table_info->replica_count = response.replica_count();
    RETURN_IF_INVALID_CONDITION(decode_pb_engine_type(response.engine_type(), table_info->engine_type),
                                "engine_type is not valid")
    RETURN_IF_INVALID_CONDITION(meta::decode_pb_meta_table_state(response.table_state(), table_info->table_state),
                                "table_state is not valid")
    table_info->last_error_msg = response.last_error_msg();
    return Status::OK();
}

}  // namespace adviskv::cli
