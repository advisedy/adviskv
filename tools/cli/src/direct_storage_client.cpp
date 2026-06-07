#include "direct_storage_client.h"

#include <chrono>

#include <fmt/format.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include "common/define.h"

namespace adviskv::cli {

rpc::StorageService::Stub* DirectStorageClient::make_stub(
    const Endpoint& endpoint) const {
    const std::string target = fmt::format("{}:{}", endpoint.ip, endpoint.port);
    std::scoped_lock locker(mutex_);
    auto it = stub_cache_.find(target);
    if (it != stub_cache_.end()) {
        return it->second.get();
    }

    auto channel =
        grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    auto stub = rpc::StorageService::NewStub(channel);
    auto* raw = stub.get();
    stub_cache_[target] = std::move(stub);
    return raw;
}

Status DirectStorageClient::put(const StorageCliTarget& target, const Key& key,
                                const Value& value) const {
    RETURN_IF_INVALID_PARAM(target)

    rpc::StorageService::Stub* stub = make_stub(target.endpoint);
    RETURN_IF_INVALID_CONDITION(stub != nullptr, "storage stub is nullptr")

    rpc::PutRequest request;
    request.set_table_id(target.table_id);
    request.set_shard_id(target.shard_id);
    request.set_key(key);
    request.set_value(value);

    rpc::PutResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(target.timeout_ms));

    grpc::Status grpc_status = stub->Put(&context, request, &response);
    if (!grpc_status.ok()) {
        return Status::ERROR(
            fmt::format("Storage Put RPC failed, grpc code = {}, msg = {}",
                        static_cast<int>(grpc_status.error_code()),
                        grpc_status.error_message()));
    }
    RETURN_IF_INVALID_STATUS(decode_base_rsp_status(response.base_rsp()))
    return Status::OK();
}

Status DirectStorageClient::get(const StorageCliTarget& target, const Key& key,
                                Value* value) const {
    RETURN_IF_INVALID_PARAM(target)
    RETURN_IF_INVALID_CONDITION(value != nullptr, "value should not be nullptr")

    rpc::StorageService::Stub* stub = make_stub(target.endpoint);
    RETURN_IF_INVALID_CONDITION(stub != nullptr, "storage stub is nullptr")

    rpc::GetRequest request;
    request.set_table_id(target.table_id);
    request.set_shard_id(target.shard_id);
    request.set_key(key);

    rpc::GetResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(target.timeout_ms));

    grpc::Status grpc_status = stub->Get(&context, request, &response);
    if (!grpc_status.ok()) {
        return Status::ERROR(
            fmt::format("Storage Get RPC failed, grpc code = {}, msg = {}",
                        static_cast<int>(grpc_status.error_code()),
                        grpc_status.error_message()));
    }
    RETURN_IF_INVALID_STATUS(decode_base_rsp_status(response.base_rsp()))
    *value = response.value();
    return Status::OK();
}

}  // namespace adviskv::cli
