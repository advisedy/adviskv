#include "sdk/storage_client.h"

#include <chrono>

#include <fmt/format.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include "common/define.h"
#include "common/metrics/metrics.h"
#include "common/proto/codec.h"
#include "sdk/log.h"

namespace adviskv::sdk {

StorageClient::StorageClient(const KVClientConf& conf) : conf_(conf) {}

Status StorageClient::select_leader_replica(const ShardRoute& route, RouteReplica* selected) {
    RETURN_IF_NULLPTR(selected, "selected replica should not be nullptr")
    RETURN_IF_INVALID_CONDITION(route.shard_id.table_id >= 0, "route.shard_id.table_id invalid")
    RETURN_IF_INVALID_CONDITION(route.shard_id.shard_index >= 0, "route.shard_id.shard_index invalid")
    RETURN_IF_INVALID_CONDITION(!route.replicas.empty(), "route replicas should not empty")

    for (const RouteReplica& replica : route.replicas) {
        if (replica.role != ReplicaRole::LEADER) {
            continue;
        }
        RETURN_IF_INVALID_CONDITION(!replica.endpoint.ip.empty(), "leader replica ip invalid")
        RETURN_IF_INVALID_CONDITION(replica.endpoint.port > 0, "leader replica port invalid")
        *selected = replica;
        return Status::OK();
    }

    return Status{StatusCode::NOT_LEADER, "leader replica not found in route"};
}

rpc::StorageService::Stub* StorageClient::make_stub(const Endpoint& endpoint) const {
    const std::string target = fmt::format("{}:{}", endpoint.ip, endpoint.port);
    std::scoped_lock locker(mutex_);
    auto it = stub_cache_.find(target);
    if (it != stub_cache_.end()) {
        return it->second.get();
    }
    auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    auto stub = rpc::StorageService::NewStub(channel);
    auto* raw = stub.get();
    stub_cache_[target] = std::move(stub);
    return raw;
}

Milliseconds StorageClient::effective_timeout(Milliseconds timeout) const {
    return timeout > Milliseconds::zero() ? timeout : Milliseconds(conf_.rpc_timeout_ms);
}
template <typename Response, typename Func>
Status StorageClient::call_leader(const ShardRoute& route, std::string_view op_name, const Key& key,
                                  Milliseconds timeout, Response* response, Func&& send_rpc) const {
    RouteReplica leader;
    Status status = select_leader_replica(route, &leader);
    if (status.fail()) {
        ADVISKV_METRICS_COUNTER(fmt::format("sdk_storage_{}_select_endpoint_failure", op_name));
        ADVISKV_SDK_LOG(LogLevel::WARN,
                        "select leader replica for {} failed, table_id={}, shard_index={}, key={}, status={}", op_name,
                        route.shard_id.table_id, route.shard_id.shard_index, key, status.to_string());
        return status;
    }

    rpc::StorageService::Stub* stub = make_stub(leader.endpoint);
    if (stub == nullptr) {
        ADVISKV_METRICS_COUNTER(fmt::format("sdk_storage_{}_stub_missing", op_name));
        return Status{StatusCode::INVALID_ARGUMENT, "storage stub is nullptr"};
    }

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + effective_timeout(timeout));

    grpc::Status grpc_status;
    {
        ADVISKV_METRICS_TIMER(fmt::format("sdk_storage_{}_rpc", op_name));
        grpc_status = send_rpc(leader, stub, &context, response);
    }
    if (!grpc_status.ok()) {
        ADVISKV_METRICS_COUNTER(fmt::format("sdk_storage_{}_rpc_failure", op_name));
        ADVISKV_SDK_LOG(LogLevel::ERROR,
                        "Storage {} RPC failed, endpoint={}:{}, table_id={}, "
                        "shard_index={}, replica_id={}, key={}, grpc_code={}, "
                        "msg={}",
                        op_name, leader.endpoint.ip, leader.endpoint.port, route.shard_id.table_id,
                        route.shard_id.shard_index, leader.replica_id.to_string(), key,
                        static_cast<int>(grpc_status.error_code()), grpc_status.error_message());
        return Status::RPC_ERROR(fmt::format("Storage {} RPC failed, grpc code = {}, msg = {}", op_name,
                                             static_cast<int>(grpc_status.error_code()), grpc_status.error_message()));
    }
    if (Status status = codec::base_rsp_to_status(response->base_rsp()); status.fail()) {
        ADVISKV_SDK_LOG(LogLevel::WARN,
                        "Storage {} returns non-ok, endpoint={}:{}, "
                        "table_id={}, shard_index={}, replica_id={}, key={}, "
                        "code={}, msg={}",
                        op_name, leader.endpoint.ip, leader.endpoint.port, route.shard_id.table_id,
                        route.shard_id.shard_index, leader.replica_id.to_string(), key, response->base_rsp().code(),
                        response->base_rsp().msg());
        return status;
    }

    return Status::OK();
}

Status StorageClient::put(const ShardRoute& route, const Key& key, const Value& value,
                          const std::string& request_id, Milliseconds timeout) const {
    ADVISKV_METRICS_TIMER("sdk_storage_put");
    ADVISKV_METRICS_COUNTER("sdk_storage_put_request");

    rpc::PutResponse response;
    return call_leader(
            route, "put", key, timeout, &response,
            [&](const RouteReplica& leader, rpc::StorageService::Stub* stub, grpc::ClientContext* context,
                rpc::PutResponse* out) {
                rpc::PutRequest request;
                codec::encode(leader.replica_id, *request.mutable_replica_id());
                request.set_key(key);
                request.set_value(value);
                request.set_request_id(request_id);
                return stub->Put(context, request, out);
            });
}

Status StorageClient::del(const ShardRoute& route, const Key& key, const std::string& request_id,
                          Milliseconds timeout) const {
    ADVISKV_METRICS_TIMER("sdk_storage_delete");
    ADVISKV_METRICS_COUNTER("sdk_storage_delete_request");

    rpc::DeleteResponse response;
    return call_leader(
            route, "delete", key, timeout, &response,
            [&](const RouteReplica& leader, rpc::StorageService::Stub* stub, grpc::ClientContext* context,
                rpc::DeleteResponse* out) {
                rpc::DeleteRequest request;
                codec::encode(leader.replica_id, *request.mutable_replica_id());
                request.set_key(key);
                request.set_request_id(request_id);
                return stub->Delete(context, request, out);
            });
}

Status StorageClient::cas(const ShardRoute& route, const Key& key, const Value& expected, const Value& value,
                          const std::string& request_id, Milliseconds timeout) const {
    ADVISKV_METRICS_TIMER("sdk_storage_cas");
    ADVISKV_METRICS_COUNTER("sdk_storage_cas_request");

    rpc::CasResponse response;
    return call_leader(
            route, "cas", key, timeout, &response,
            [&](const RouteReplica& leader, rpc::StorageService::Stub* stub, grpc::ClientContext* context,
                rpc::CasResponse* out) {
                rpc::CasRequest request;
                codec::encode(leader.replica_id, *request.mutable_replica_id());
                request.set_key(key);
                request.set_expected(expected);
                request.set_value(value);
                request.set_request_id(request_id);
                return stub->Cas(context, request, out);
            });
}

Status StorageClient::get(const ShardRoute& route, const Key& key, Value* value, Milliseconds timeout) const {
    ADVISKV_METRICS_TIMER("sdk_storage_get");
    ADVISKV_METRICS_COUNTER("sdk_storage_get_request");

    RETURN_IF_NULLPTR(value, "value should not be nullptr")

    rpc::GetResponse response;
    Status status = call_leader(route, "get", key, timeout, &response,
                                [&](const RouteReplica& leader, rpc::StorageService::Stub* stub,
                                    grpc::ClientContext* context, rpc::GetResponse* out) {
                                    rpc::GetRequest request;
                                    codec::encode(leader.replica_id, *request.mutable_replica_id());
                                    request.set_key(key);
                                    return stub->Get(context, request, out);
                                });
    if (status.ok()) {
        *value = response.value();
    }
    return status;
}

}  // namespace adviskv::sdk
