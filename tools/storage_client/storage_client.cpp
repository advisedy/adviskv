#include "tools/storage_client/storage_client.h"

#include <fmt/format.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include "common.pb.h"
#include "common/define.h"
#include "common/proto/codec.h"
#include "common/status.h"

namespace adviskv::tools {

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

Status StorageClient::create_replica(const CreateReplicaParam& param) {
    RETURN_IF_INVALID_PARAM(param)

    rpc::StorageService::Stub* stub =
        make_stub(param.endpoint.ip, param.endpoint.port);
    if (!stub) {
        return Status::NO_STUB(fmt::format("failed to create stub for {}:{}",
                                           param.endpoint.ip,
                                           param.endpoint.port));
    }

    rpc::CreateReplicaRequest request;
    codec::encode(param.replica_id, *request.mutable_replica_id());
    request.set_engine_type(static_cast<int32_t>(param.engine_type));
    for (const PeerMember& member : param.members) {
        codec::encode(member, *request.add_initial_members());
    }

    rpc::CreateReplicaResponse response;
    grpc::ClientContext context;
    grpc::Status grpc_status =
        stub->CreateReplica(&context, request, &response);
    if (!grpc_status.ok()) {
        return Status::RPC_ERROR(
            fmt::format("CreateReplica RPC failed for {}:{}, grpc error: {}",
                        param.endpoint.ip, param.endpoint.port,
                        grpc_status.error_message()));
    }
    RETURN_IF_INVALID_STATUS(codec::base_rsp_to_status(response.base_rsp()))
    return Status::OK();
}

Status StorageClient::delete_replica(const DeleteReplicaParam& param) {
    RETURN_IF_INVALID_PARAM(param)

    rpc::StorageService::Stub* stub =
        make_stub(param.endpoint.ip, param.endpoint.port);
    if (!stub) {
        return Status::NO_STUB(fmt::format("failed to create stub for {}:{}",
                                           param.endpoint.ip,
                                           param.endpoint.port));
    }

    rpc::DeleteReplicaRequest request;
    codec::encode(param.replica_id, *request.mutable_replica_id());

    rpc::DeleteReplicaResponse response;
    grpc::ClientContext context;
    grpc::Status grpc_status =
        stub->DeleteReplica(&context, request, &response);
    if (!grpc_status.ok()) {
        return Status::RPC_ERROR(
            fmt::format("DeleteReplica RPC failed for {}:{}, grpc error: {}",
                        param.endpoint.ip, param.endpoint.port,
                        grpc_status.error_message()));
    }
    RETURN_IF_INVALID_STATUS(codec::base_rsp_to_status(response.base_rsp()))
    return Status::OK();
}

Status StorageClient::get_replica_info(const GetReplicaInfoParam& param,
                                       StorageReplicaInfo& out, bool& exists) {
    RETURN_IF_INVALID_PARAM(param)

    rpc::StorageService::Stub* stub =
        make_stub(param.endpoint.ip, param.endpoint.port);
    if (!stub) {
        return Status::NO_STUB(fmt::format("failed to create stub for {}:{}",
                                           param.endpoint.ip,
                                           param.endpoint.port));
    }

    rpc::GetReplicaInfoRequest request;
    codec::encode(param.replica_id, *request.mutable_replica_id());

    rpc::GetReplicaInfoResponse response;
    grpc::ClientContext context;
    grpc::Status grpc_status =
        stub->GetReplicaInfo(&context, request, &response);
    if (!grpc_status.ok()) {
        return Status::RPC_ERROR(
            fmt::format("GetReplicaInfo RPC failed for {}:{}, grpc error: {}",
                        param.endpoint.ip, param.endpoint.port,
                        grpc_status.error_message()));
    }
    RETURN_IF_INVALID_STATUS(codec::base_rsp_to_status(response.base_rsp()))
    exists = response.exists();
    if (!response.exists()) return Status::OK();

    const auto& replica = response.replica();

    RETURN_IF_INVALID_CONDITION(codec::decode(replica.replica_id(), out.replica_id), "replica id is not valid")
    RETURN_IF_INVALID_CONDITION(is_valid_enum_value<ReplicaRole>(replica.role()), "replica role is not valid")
    out.raft_role = static_cast<ReplicaRole>(replica.role());
    RETURN_IF_INVALID_CONDITION(is_valid_enum_value<StorageReplicaStatus>(replica.status()),
                                "replica storage status is not valid")
    out.storage_status = static_cast<StorageReplicaStatus>(replica.status());
    RETURN_IF_INVALID_CONDITION(codec::decode(replica.endpoint(), out.endpoint), "replica endpoint is not valid")
    out.term = replica.term();
    return Status::OK();
}
}  // namespace adviskv::tools
