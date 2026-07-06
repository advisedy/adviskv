#include "tools/storage_client/storage_client.h"

#include <fmt/format.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include "common.pb.h"
#include "common/define.h"
#include "common/proto/proto.h"
#include "common/status.h"
#include "sdm/model/model.h"

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
    encode_pb_replica_id(param.replica_id, *request.mutable_replica_id());
    pb::EngineType engine_type_pb = pb::ENGINE_TYPE_UNSPECIFIED;
    RETURN_IF_INVALID_CONDITION(
        encode_pb_engine_type(param.engine_type, engine_type_pb),
        "engine_type is not valid")
    request.set_engine_type(engine_type_pb);
    for (const PeerMember& member : param.members) {
        encode_pb_peer_member(member, *request.add_initial_members());
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
    RETURN_IF_INVALID_STATUS(decode_base_rsp_status(response.base_rsp()))
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
    encode_pb_replica_id(param.replica_id, *request.mutable_replica_id());

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
    RETURN_IF_INVALID_STATUS(decode_base_rsp_status(response.base_rsp()))
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
    encode_pb_replica_id(param.replica_id, *request.mutable_replica_id());

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
    RETURN_IF_INVALID_STATUS(decode_base_rsp_status(response.base_rsp()))
    exists = response.exists();
    if (!response.exists()) return Status::OK();

    const auto& replica = response.replica();

    out.replica_id =
        ReplicaID{replica.table_id(), replica.shard_id(), replica.replica_seq()};
    ReplicaRole role = ReplicaRole::FOLLOWER;
    RETURN_IF_INVALID_CONDITION(decode_pb_raft_role(replica.role(), role),
                                "replica role is not valid")
    out.raft_role = role;
    StorageReplicaStatus storage_status = StorageReplicaStatus::INITIALIZING;
    RETURN_IF_INVALID_CONDITION(
        decode_pb_storage_replica_status(replica.status(), storage_status),
        "replica storage status is not valid")
    out.storage_status = storage_status;
    out.endpoint = Endpoint{replica.endpoint().ip(), replica.endpoint().port()};
    out.term = replica.term();
    return Status::OK();
}
}  // namespace adviskv::tools
