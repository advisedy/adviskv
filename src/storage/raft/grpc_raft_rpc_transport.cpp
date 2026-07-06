#include "storage/raft/grpc_raft_rpc_transport.h"

#include <fmt/format.h>
#include <grpcpp/create_channel.h>

#include <chrono>

#include "common/proto/proto.h"
#include "storage/proto/storage_model_proto.h"

namespace adviskv::storage {
namespace {

auto get_system_clock_now() { return std::chrono::system_clock::now(); }

}  // namespace

Status GrpcRaftRpcTransport::request_vote(const PeerMember& target,
                                          const RequestVoteParam& param,
                                          int32_t timeout_ms,
                                          RequestVoteResult& result) const {
    rpc::RequestVoteRequest request;
    encode_pb_replica_id(param.from_replica_id, *request.mutable_from());
    encode_pb_replica_id(param.to_replica_id, *request.mutable_to());

    request.set_term(param.term);
    request.set_last_log_index(param.last_log_index);
    request.set_last_log_term(param.last_log_term);

    rpc::RequestVoteResponse response;
    grpc::ClientContext context;
    context.set_deadline(get_system_clock_now() + Milliseconds(timeout_ms));

    grpc::Status grpc_status =
        stub_for(target)->RequestVote(&context, request, &response);
    if (!grpc_status.ok()) {
        return Status::RPC_ERROR(
            fmt::format("grpc failed: {}", grpc_status.error_message()));
    }

    result.term = response.term();
    result.vote_granted = response.vote_granted();
    return Status::OK();
}

Status GrpcRaftRpcTransport::append_entries(const PeerMember& target,
                                            const AppendEntriesParam& param,
                                            int32_t timeout_ms,
                                            AppendEntriesResult& result) const {
    rpc::AppendEntriesRequest request;
    encode_pb_replica_id(param.from_replica_id, *request.mutable_from());
    encode_pb_replica_id(param.to_replica_id, *request.mutable_to());

    request.set_term(param.term);
    request.set_prev_log_index(param.prev_log_index);
    request.set_prev_log_term(param.prev_log_term);
    request.set_leader_commit(param.leader_commit);

    for (const LogEntry& entry : param.entries) {
        auto* out_entry = request.add_entries();
        if (!encode_pb_log_entry(entry, *out_entry)) {
            return Status::INVALID_ARGUMENT("invalid append entries log entry");
        }
    }

    rpc::AppendEntriesResponse response;
    grpc::ClientContext context;
    context.set_deadline(get_system_clock_now() + Milliseconds(timeout_ms));

    grpc::Status grpc_status =
        stub_for(target)->AppendEntries(&context, request, &response);
    if (!grpc_status.ok()) {
        return Status::RPC_ERROR(grpc_status.error_message());
    }

    result.term = response.term();
    result.success = response.success();
    result.last_log_index = response.last_log_index();
    return Status::OK();
}

Status GrpcRaftRpcTransport::install_snapshot_chunk(
    const PeerMember& target, const InstallSnapshotParam& param,
    int32_t timeout_ms, InstallSnapshotResult& result) const {
    rpc::InstallSnapshotRequest request;
    encode_pb_replica_id(param.from_replica_id, *request.mutable_from());
    encode_pb_replica_id(param.to_replica_id, *request.mutable_to());

    request.set_term(param.term);
    request.set_apply_index(param.snapshot_index);
    request.set_apply_term(param.snapshot_term);
    request.set_offset(param.offset);
    request.set_data(param.data);
    request.set_done(param.done);

    rpc::InstallSnapshotResponse response;
    grpc::ClientContext context;
    context.set_deadline(get_system_clock_now() + Milliseconds(timeout_ms));

    grpc::Status grpc_status =
        stub_for(target)->InstallSnapshot(&context, request, &response);
    if (!grpc_status.ok()) {
        return Status::RPC_ERROR(grpc_status.error_message());
    }

    result.term = response.term();
    result.status =
        Status{response.base_rsp().code(), response.base_rsp().msg()};
    result.snapshot_watermark = response.snapshot_watermark();
    return Status::OK();
}

std::string GrpcRaftRpcTransport::target_of(const PeerMember& member) {
    return member.endpoint.ip + ":" + std::to_string(member.endpoint.port);
}

rpc::StorageService::StubInterface* GrpcRaftRpcTransport::stub_for(
    const PeerMember& member) const {
    const std::string target = target_of(member);
    std::scoped_lock lock(mutex_);
    auto it = stub_pool_.find(target);
    if (it != stub_pool_.end()) {
        return it->second.get();
    }

    auto channel =
        grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    auto stub = rpc::StorageService::NewStub(channel);
    auto* raw = stub.get();
    stub_pool_[target] = std::move(stub);
    return raw;
}

}  // namespace adviskv::storage
