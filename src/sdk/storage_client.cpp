#include "sdk/storage_client.h"

#include <fmt/format.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <chrono>

#include "common/define.h"
#include "common/metrics/metrics.h"
#include "common/proto/proto.h"
#include "sdk/log.h"

namespace adviskv::sdk {

StorageClient::StorageClient(const KVClientConf& conf) : conf_(conf) {}

Status StorageClient::select_leader_replica(const RouteInfo& route,
                                            RouteReplica* selected) {
    RETURN_IF_NULLPTR(selected, "selected replica should not be nullptr")
    RETURN_IF_INVALID_CONDITION(route.table_id >= 0, "route.table_id invalid")
    RETURN_IF_INVALID_CONDITION(route.shard_id >= 0, "route.shard_id invalid")
    RETURN_IF_INVALID_CONDITION(!route.replicas.empty(),
                                "route replicas should not empty")

    for (const RouteReplica& replica : route.replicas) {
        if (replica.role != ReplicaRole::LEADER) {
            continue;
        }
        RETURN_IF_INVALID_CONDITION(!replica.endpoint.ip.empty(),
                                    "leader replica ip invalid")
        RETURN_IF_INVALID_CONDITION(replica.endpoint.port > 0,
                                    "leader replica port invalid")
        *selected = replica;
        return Status::OK();
    }

    return Status{StatusCode::NOT_LEADER, "leader replica not found in route"};
}

rpc::StorageService::Stub* StorageClient::make_stub(
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

Status StorageClient::put(const RouteInfo& route, const Key& key,
                          const Value& value) const {
    ADVISKV_METRICS_TIMER("sdk_storage_put");
    ADVISKV_METRICS_COUNTER("sdk_storage_put_request");

    RouteReplica leader;
    Status status = select_leader_replica(route, &leader);
    if (status.fail()) {
        ADVISKV_METRICS_COUNTER("sdk_storage_put_select_endpoint_failure");
        ADVISKV_SDK_LOG(LogLevel::WARN,
                        "select leader replica for put failed, table_id={}, "
                        "shard_id={}, key={}, status={}",
                        route.table_id, route.shard_id, key,
                        status.to_string());
        return status;
    }

    rpc::StorageService::Stub* stub = make_stub(leader.endpoint);
    if (stub == nullptr) {
        ADVISKV_METRICS_COUNTER("sdk_storage_put_stub_missing");
        return Status{StatusCode::INVALID_ARGUMENT, "storage stub is nullptr"};
    }

    rpc::PutRequest request;
    encode_pb_replica_id(leader.replica_id, *request.mutable_replica_id());
    request.set_key(key);
    request.set_value(value);

    rpc::PutResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         Milliseconds(conf_.storage_timeout_ms));

    grpc::Status grpc_status;
    {
        ADVISKV_METRICS_TIMER("sdk_storage_put_rpc");
        grpc_status = stub->Put(&context, request, &response);
    }
    if (!grpc_status.ok()) {
        ADVISKV_METRICS_COUNTER("sdk_storage_put_rpc_failure");
        ADVISKV_SDK_LOG(LogLevel::ERROR,
                        "Storage Put RPC failed, endpoint={}:{}, table_id={}, "
                        "shard_id={}, replica_id={}, key={}, grpc_code={}, "
                        "msg={}",
                        leader.endpoint.ip, leader.endpoint.port,
                        route.table_id, route.shard_id,
                        leader.replica_id.to_string(), key,
                        static_cast<int>(grpc_status.error_code()),
                        grpc_status.error_message());
        return Status::ERROR(
            fmt::format("Storage Put RPC failed, grpc code = {}, msg = {}",
                        static_cast<int>(grpc_status.error_code()),
                        grpc_status.error_message()));
    }
    if (Status status = decode_base_rsp_status(response.base_rsp());
        status.fail()) {
        ADVISKV_SDK_LOG(LogLevel::WARN,
                        "Storage Put returns non-ok, endpoint={}:{}, "
                        "table_id={}, shard_id={}, replica_id={}, key={}, "
                        "code={}, msg={}",
                        leader.endpoint.ip, leader.endpoint.port,
                        route.table_id, route.shard_id,
                        leader.replica_id.to_string(), key,
                        response.base_rsp().code(), response.base_rsp().msg());
        return status;
    }

    return Status::OK();
}

Status StorageClient::del(const RouteInfo& route, const Key& key) const {
    ADVISKV_METRICS_TIMER("sdk_storage_delete");
    ADVISKV_METRICS_COUNTER("sdk_storage_delete_request");

    RouteReplica leader;
    Status status = select_leader_replica(route, &leader);
    if (status.fail()) {
        ADVISKV_METRICS_COUNTER("sdk_storage_delete_select_endpoint_failure");
        ADVISKV_SDK_LOG(LogLevel::WARN,
                        "select leader replica for delete failed, "
                        "table_id={}, shard_id={}, key={}, status={}",
                        route.table_id, route.shard_id, key,
                        status.to_string());
        return status;
    }

    rpc::StorageService::Stub* stub = make_stub(leader.endpoint);
    if (stub == nullptr) {
        ADVISKV_METRICS_COUNTER("sdk_storage_delete_stub_missing");
        return Status{StatusCode::INVALID_ARGUMENT, "storage stub is nullptr"};
    }

    rpc::DeleteRequest request;
    encode_pb_replica_id(leader.replica_id, *request.mutable_replica_id());
    request.set_key(key);

    rpc::DeleteResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         Milliseconds(conf_.storage_timeout_ms));

    grpc::Status grpc_status;
    {
        ADVISKV_METRICS_TIMER("sdk_storage_delete_rpc");
        grpc_status = stub->Delete(&context, request, &response);
    }
    if (!grpc_status.ok()) {
        ADVISKV_METRICS_COUNTER("sdk_storage_delete_rpc_failure");
        ADVISKV_SDK_LOG(LogLevel::ERROR,
                        "Storage Delete RPC failed, endpoint={}:{}, "
                        "table_id={}, shard_id={}, replica_id={}, key={}, "
                        "grpc_code={}, msg={}",
                        leader.endpoint.ip, leader.endpoint.port,
                        route.table_id, route.shard_id,
                        leader.replica_id.to_string(), key,
                        static_cast<int>(grpc_status.error_code()),
                        grpc_status.error_message());
        return Status::ERROR(
            fmt::format("Storage Delete RPC failed, grpc code = {}, msg = {}",
                        static_cast<int>(grpc_status.error_code()),
                        grpc_status.error_message()));
    }
    if (Status status = decode_base_rsp_status(response.base_rsp());
        status.fail()) {
        ADVISKV_SDK_LOG(LogLevel::WARN,
                        "Storage Delete returns non-ok, endpoint={}:{}, "
                        "table_id={}, shard_id={}, replica_id={}, key={}, "
                        "code={}, msg={}",
                        leader.endpoint.ip, leader.endpoint.port,
                        route.table_id, route.shard_id,
                        leader.replica_id.to_string(), key,
                        response.base_rsp().code(), response.base_rsp().msg());
        return status;
    }

    return Status::OK();
}

Status StorageClient::get(const RouteInfo& route, const Key& key,
                          Value* value) const {
    RETURN_IF_NULLPTR(value, "value should not be nullptr")
    RouteReplica leader;
    Status status = select_leader_replica(route, &leader);
    if (status.fail()) {
        ADVISKV_SDK_LOG(LogLevel::WARN,
                        "select leader replica for get failed, table_id={}, "
                        "shard_id={}, key={}, status={}",
                        route.table_id, route.shard_id, key,
                        status.to_string());
        return status;
    }

    rpc::StorageService::Stub* stub = make_stub(leader.endpoint);
    RETURN_IF_NULLPTR(stub, "storage stub is nullptr")

    rpc::GetRequest request;
    encode_pb_replica_id(leader.replica_id, *request.mutable_replica_id());
    request.set_key(key);

    rpc::GetResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         Milliseconds(conf_.storage_timeout_ms));

    grpc::Status grpc_status = stub->Get(&context, request, &response);
    if (!grpc_status.ok()) {
        ADVISKV_SDK_LOG(LogLevel::ERROR,
                        "Storage Get RPC failed, endpoint={}:{}, table_id={}, "
                        "shard_id={}, replica_id={}, key={}, grpc_code={}, "
                        "msg={}",
                        leader.endpoint.ip, leader.endpoint.port,
                        route.table_id, route.shard_id,
                        leader.replica_id.to_string(), key,
                        static_cast<int>(grpc_status.error_code()),
                        grpc_status.error_message());
        return Status::ERROR(
            fmt::format("Storage Get RPC failed, grpc code = {}, msg = {}",
                        static_cast<int>(grpc_status.error_code()),
                        grpc_status.error_message()));
    }

    if (Status status = decode_base_rsp_status(response.base_rsp());
        status.fail()) {
        ADVISKV_SDK_LOG(LogLevel::WARN,
                        "Storage Get returns non-ok, endpoint={}:{}, "
                        "table_id={}, shard_id={}, replica_id={}, key={}, "
                        "code={}, msg={}",
                        leader.endpoint.ip, leader.endpoint.port,
                        route.table_id, route.shard_id,
                        leader.replica_id.to_string(), key,
                        response.base_rsp().code(), response.base_rsp().msg());
        return status;
    }

    *value = response.value();
    return Status::OK();
}

}  // namespace adviskv::sdk
