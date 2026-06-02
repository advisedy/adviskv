#include "storage/raft/raft_callback.h"

#include <fmt/format.h>
#include <grpcpp/create_channel.h>

#include <mutex>

#include "common/metrics/metrics.h"
#include "common/status.h"
#include "common/type.h"

namespace adviskv::storage {

RaftSender::RaftSender() = default;

Status RaftSender::send_request_vote(const PeerMember& member,
                                     const RequestVoteParam& param,
                                     RequestVoteResult& result) const {
    rpc::RequestVoteRequest request;
    request.mutable_from()->set_table_id(param.from_replica_id.table_id);
    request.mutable_from()->set_shard_index(param.from_replica_id.shard_index);
    request.mutable_from()->set_replica_index(
        param.from_replica_id.replica_index);

    request.mutable_to()->set_table_id(param.to_replica_id.table_id);
    request.mutable_to()->set_shard_index(param.to_replica_id.shard_index);
    request.mutable_to()->set_replica_index(param.to_replica_id.replica_index);

    request.set_term(param.term);
    request.set_last_log_index(param.last_log_index);
    request.set_last_log_term(param.last_log_term);

    rpc::RequestVoteResponse response;
    grpc::ClientContext context;
    grpc::Status grpc_status =
        stub_for(member)->RequestVote(&context, request, &response);
    if (!grpc_status.ok()) {
        return Status::RPC_ERROR(
            fmt::format("grpc failed: {}", grpc_status.error_message()));
    }

    result.term = response.term();
    result.vote_granted = response.vote_granted();
    return Status::OK();
}

Status RaftSender::send_append_entries(const PeerMember& member,
                                       const AppendEntriesParam& param,
                                       AppendEntriesResult& result) const {
    ADVISKV_METRICS_TIMER("storage_raft_append_entries_rpc");
    ADVISKV_METRICS_COUNTER("storage_raft_append_entries_rpc_request");
    if (param.entries.empty()) {
        ADVISKV_METRICS_COUNTER("storage_raft_append_entries_rpc_heartbeat");
    } else {
        ADVISKV_METRICS_COUNTER("storage_raft_append_entries_rpc_log");
        ADVISKV_METRICS_COUNTER("storage_raft_append_entries_rpc_entry",
                                static_cast<int64_t>(param.entries.size()));
    }

    rpc::AppendEntriesRequest request;
    request.mutable_from()->set_table_id(param.from_replica_id.table_id);
    request.mutable_from()->set_shard_index(param.from_replica_id.shard_index);
    request.mutable_from()->set_replica_index(
        param.from_replica_id.replica_index);

    request.mutable_to()->set_table_id(param.to_replica_id.table_id);
    request.mutable_to()->set_shard_index(param.to_replica_id.shard_index);
    request.mutable_to()->set_replica_index(param.to_replica_id.replica_index);

    request.set_term(param.term);
    request.set_prev_log_index(param.prev_log_index);
    request.set_prev_log_term(param.prev_log_term);
    request.set_leader_commit(param.leader_commit);

    for (const LogEntry& entry : param.entries) {
        auto one = request.add_entries();
        one->set_index(entry.index);
        one->set_term(entry.term);
        one->set_op_type(static_cast<int32_t>(entry.op_type));
        one->set_key(entry.key);
        one->set_value(entry.value);
    }

    rpc::AppendEntriesResponse response;
    grpc::ClientContext context;
    grpc::Status grpc_status =
        stub_for(member)->AppendEntries(&context, request, &response);
    if (!grpc_status.ok()) {
        ADVISKV_METRICS_COUNTER("storage_raft_append_entries_rpc_failure");
        return Status{StatusCode::RPC_ERROR, grpc_status.error_message()};
    }

    result.term = response.term();
    result.success = response.success();
    ADVISKV_METRICS_COUNTER("storage_raft_append_entries_rpc_success");
    if (result.success) {
        ADVISKV_METRICS_COUNTER("storage_raft_append_entries_rpc_accepted");
    } else {
        ADVISKV_METRICS_COUNTER("storage_raft_append_entries_rpc_rejected");
    }
    return Status::OK();
}

Status RaftSender::send_install_snapshot(const PeerMember& member,
                                         const InstallSnapshotParam& param,
                                         const PersistEngine& persist,
                                         InstallSnapshotResult& result) const {
    {
        std::lock_guard locker{in_flight_mutex_};
        do {
            if (auto it = in_flight_snapshots_.find(member.replica_id);
                it != in_flight_snapshots_.end()) {
                const InFlightSnapshot& snapshot = it->second;
                if (snapshot.snapshot_index != param.snapshot_index) break;
                if (snapshot.snapshot_term != param.snapshot_term) break;
                return Status::ALREADY_EXIST("snapshot have been send");
            }
        } while (false);
        in_flight_snapshots_[member.replica_id] = InFlightSnapshot{
            member.replica_id, param.snapshot_index, param.snapshot_term};
    }

    constexpr size_t kChunkSize = 1 << 20;
    uint64 offset = 0;
    rpc::StorageService::StubInterface* stub = stub_for(member);
    result.term = param.term;
    result.success = false;
    while (true) {
        std::string data;
        bool eof = false;
        RETURN_IF_INVALID_STATUS(
            persist.read_snapshot_chunk(offset, kChunkSize, data, eof))

        rpc::InstallSnapshotRequest request;
        request.mutable_from()->set_table_id(param.from_replica_id.table_id);
        request.mutable_from()->set_shard_index(
            param.from_replica_id.shard_index);
        request.mutable_from()->set_replica_index(
            param.from_replica_id.replica_index);

        request.mutable_to()->set_table_id(param.to_replica_id.table_id);
        request.mutable_to()->set_shard_index(param.to_replica_id.shard_index);
        request.mutable_to()->set_replica_index(
            param.to_replica_id.replica_index);

        request.set_term(param.term);
        request.set_apply_index(param.snapshot_index);
        request.set_apply_term(param.snapshot_term);
        request.set_offset(offset);
        request.set_data(data);
        request.set_done(eof);

        rpc::InstallSnapshotResponse response;
        grpc::ClientContext context;
        grpc::Status grpc_status =
            stub->InstallSnapshot(&context, request, &response);
        RETURN_IF_INVALID_CONDITION(grpc_status.ok(),
                                    grpc_status.error_message())

        result.term = response.term();
        if (response.base_rsp().code() !=
            static_cast<int32_t>(StatusCode::OK)) {
            result.success = false;
            return Status::OK();
        }

        if (eof) {
            result.success = true;
            break;
        }
        offset += data.size();
    }
    return Status::OK();
}

std::string RaftSender::target_of(const PeerMember& member) {
    return member.endpoint.ip + ":" + std::to_string(member.endpoint.port);
}

rpc::StorageService::StubInterface* RaftSender::stub_for(
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