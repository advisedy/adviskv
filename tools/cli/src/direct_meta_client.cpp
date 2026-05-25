#include "direct_meta_client.h"

#include <fmt/format.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <chrono>

#include "common/define.h"

namespace adviskv::cli {

DirectMetaClient::DirectMetaClient(const MetaCliTarget& target)
    : target_(target) {
    const std::string endpoint =
        fmt::format("{}:{}", target_.endpoint.ip, target_.endpoint.port);
    auto channel =
        grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials());
    stub_ = rpc::MetaService::NewStub(channel);
}

Status DirectMetaClient::create_db(const std::string& db_name,
                                   const std::string& zone,
                                   DatabaseID* db_id) const {
    RETURN_IF_INVALID_PARAM(target_)
    RETURN_IF_INVALID_CONDITION(db_id != nullptr, "db_id should not be nullptr")

    rpc::CreateDBRequest request;
    request.set_db_name(db_name);
    request.set_zone(zone);

    rpc::CreateDBResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(target_.timeout_ms));

    grpc::Status grpc_status = stub_->CreateDB(&context, request, &response);
    if (!grpc_status.ok()) {
        return Status::ERROR(
            fmt::format("Meta CreateDB RPC failed, grpc code = {}, msg = {}",
                        static_cast<int>(grpc_status.error_code()),
                        grpc_status.error_message()));
    }
    if (response.base_rsp().code() != to_rpc_code(StatusCode::OK)) {
        return Status{static_cast<StatusCode>(response.base_rsp().code()),
                      response.base_rsp().msg()};
    }
    *db_id = response.db_id();
    return Status::OK();
}

Status DirectMetaClient::create_table(const std::string& db_name,
                                      const std::string& table_name,
                                      int32_t shard_count,
                                      int32_t replica_count, TableID* table_id,
                                      std::string resource) const {
    RETURN_IF_INVALID_PARAM(target_)
    RETURN_IF_INVALID_CONDITION(table_id != nullptr,
                                "table_id should not be nullptr")

    rpc::CreateTableRequest request;
    request.set_db_name(db_name);
    request.set_table_name(table_name);
    request.set_shard_count(shard_count);
    request.set_replica_count(replica_count);
    request.set_resource_pool(resource);

    rpc::CreateTableResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(target_.timeout_ms));

    grpc::Status grpc_status = stub_->CreateTable(&context, request, &response);
    if (!grpc_status.ok()) {
        return Status::ERROR(
            fmt::format("Meta CreateTable RPC failed, grpc code = {}, msg = {}",
                        static_cast<int>(grpc_status.error_code()),
                        grpc_status.error_message()));
    }
    if (response.base_rsp().code() != to_rpc_code(StatusCode::OK)) {
        return Status{static_cast<StatusCode>(response.base_rsp().code()),
                      response.base_rsp().msg()};
    }
    *table_id = response.table_id();
    return Status::OK();
}

Status DirectMetaClient::get_table(const std::string& db_name,
                                   const std::string& table_name,
                                   TableInfo* table_info) const {
    RETURN_IF_INVALID_PARAM(target_)
    RETURN_IF_INVALID_CONDITION(table_info != nullptr,
                                "table_info should not be nullptr")

    rpc::GetTableRequest request;
    request.set_db_name(db_name);
    request.set_table_name(table_name);

    rpc::GetTableResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(target_.timeout_ms));

    grpc::Status grpc_status = stub_->GetTable(&context, request, &response);
    if (!grpc_status.ok()) {
        return Status::ERROR(
            fmt::format("Meta GetTable RPC failed, grpc code = {}, msg = {}",
                        static_cast<int>(grpc_status.error_code()),
                        grpc_status.error_message()));
    }
    if (response.base_rsp().code() != to_rpc_code(StatusCode::OK)) {
        return Status{static_cast<StatusCode>(response.base_rsp().code()),
                      response.base_rsp().msg()};
    }
    table_info->db_id = response.db_id();
    table_info->table_id = response.table_id();
    table_info->shard_count = response.shard_count();
    table_info->replica_count = response.replica_count();
    table_info->table_state = response.table_state();
    table_info->last_error_msg = response.last_error_msg();
    return Status::OK();
}

}  // namespace adviskv::cli
