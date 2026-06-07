#include "sdm/client/storage_client.h"

#include <fmt/format.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include "common.pb.h"
#include "common/define.h"
#include "common/proto/raft_role_proto.h"
#include "common/proto/storage_replica_status_proto.h"
#include "common/status.h"
#include "sdm/model/store.h"

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
    request.set_table_id(param.replica_id.table_id);
    request.set_shard_index(param.replica_id.shard_index);
    request.set_replica_index(param.replica_id.replica_index);
    request.set_engine_type(static_cast<int32_t>(param.engine_type));
    for (const PeerMember& member : param.members) {
        auto* pb_member = request.add_members();
        pb_member->set_node_id(member.node_id);
        auto* rid = pb_member->mutable_replica_id();
        rid->set_table_id(member.replica_id.table_id);
        rid->set_shard_index(member.replica_id.shard_index);
        rid->set_replica_index(member.replica_id.replica_index);
        auto* ep = pb_member->mutable_endpoint();
        ep->set_ip(member.endpoint.ip);
        ep->set_port(member.endpoint.port);
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
    if (response.base_rsp().code() != 0) {
        return Status{static_cast<StatusCode>(response.base_rsp().code()),
                      response.base_rsp().msg()};
    }
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
    request.set_table_id(param.replica_id.table_id);
    request.set_shard_id(param.replica_id.shard_index);
    request.set_replica_id(param.replica_id.replica_index);

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
    if (response.base_rsp().code() != 0) {
        return Status{static_cast<StatusCode>(response.base_rsp().code()),
                      response.base_rsp().msg()};
    }
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
    request.set_table_id(param.replica_id.table_id);
    request.set_shard_id(param.replica_id.shard_index);
    request.set_replica_id(param.replica_id.replica_index);

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
    if (response.base_rsp().code() != 0) {
        return Status{static_cast<StatusCode>(response.base_rsp().code()),
                      response.base_rsp().msg()};
    }
    exists = response.exists();
    if (!response.exists()) return Status::OK();

    const auto& replica = response.replica();

    out.replica_id = ReplicaID{replica.table_id(), replica.shard_id(),
                               replica.replica_id()};
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
}  // namespace adviskv::sdm