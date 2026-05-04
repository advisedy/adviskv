#include "sdm/client/storage_client.h"

#include <fmt/format.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include "common/define.h"
#include "common/log.h"
#include "common/status.h"

namespace adviskv::sdm {

// 这个里面如果没有的话，就去create，然后放到cache里面
rpc::StorageService::Stub* StorageClient::make_stub(const std::string& ip,
                                                    int32_t port) {
    std::string key = fmt::format("{}:{}", ip, port);
    auto it = stub_cache_.find(key);
    if (it != stub_cache_.end()) {
        return it->second.get();
    }
    auto channel = grpc::CreateChannel(key, grpc::InsecureChannelCredentials());
    auto stub = rpc::StorageService::NewStub(channel);
    auto* raw = stub.get();
    stub_cache_[key] = std::move(stub);
    return raw;
}

Status StorageClient::create_replica(const std::string& ip, int32_t port,
                                     const rpc::CreateReplicaRequest& request,
                                     rpc::CreateReplicaResponse& response) {
    rpc::StorageService::Stub* stub = make_stub(ip, port);
    if (!stub) {
        return Status::ERROR(
            fmt::format("failed to create stub for {}:{}", ip, port));
    }
    grpc::ClientContext context;
    grpc::Status grpc_status =
        stub->CreateReplica(&context, request, &response);
    if (!grpc_status.ok()) {
        return Status::ERROR(
            fmt::format("CreateReplica RPC failed for {}:{}, grpc error: {}",
                        ip, port, grpc_status.error_message()));
    }
    if (response.base_rsp().code() != 0) {
        return Status{static_cast<StatusCode>(response.base_rsp().code()),
                      response.base_rsp().msg()};
    }
    return Status::OK();
}

}  // namespace adviskv::sdm
