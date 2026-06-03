#include "storage/raft/raft_sender.h"

#include "common/defer.h"
#include "common/define.h"
#include "common/metrics/metrics.h"
#include "storage/raft/grpc_raft_rpc_transport.h"

namespace adviskv::storage {

RaftSender::RaftSender(int32_t timeout_ms)
    : RaftSender(std::make_unique<GrpcRaftRpcTransport>(), timeout_ms) {}

RaftSender::RaftSender(std::unique_ptr<IRaftRpcTransport> transport,
                       int32_t timeout_ms)
    : transport_(std::move(transport)) {
    if (!transport_) {
        transport_ = std::make_unique<GrpcRaftRpcTransport>();
    }
    set_timeout_ms(timeout_ms);
}

void RaftSender::set_timeout_ms(int32_t timeout_ms) {
    timeout_ms_ = timeout_ms > 0 ? timeout_ms : 1000;
}

Status RaftSender::send_request_vote(const PeerMember& member,
                                     const RequestVoteParam& param,
                                     RequestVoteResult& result) const {
    return transport_->request_vote(member, param, timeout_ms_, result);
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

    Status status =
        transport_->append_entries(member, param, timeout_ms_, result);
    if (status.fail()) {
        ADVISKV_METRICS_COUNTER("storage_raft_append_entries_rpc_failure");
        return status;
    }
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
                return Status::ALREADY_EXIST(
                    "snapshot is sending the same one");
            }
        } while (false);
        in_flight_snapshots_[member.replica_id] = InFlightSnapshot{
            member.replica_id, param.snapshot_index, param.snapshot_term};
    }
    auto clear_in_flight = Defer([this, replica_id = member.replica_id]() {
        std::lock_guard locker{in_flight_mutex_};
        in_flight_snapshots_.erase(replica_id);
    });

    constexpr size_t kChunkSize = 1 << 20;
    uint64 offset = 0;
    result.term = param.term;
    result.success = false;
    while (true) {
        std::string data;
        bool eof = false;
        RETURN_IF_INVALID_STATUS(
            persist.read_snapshot_chunk(offset, kChunkSize, data, eof))

        InstallSnapshotParam chunk_param = param;
        chunk_param.offset = offset;
        chunk_param.data = data;
        chunk_param.done = eof;

        Status status = transport_->install_snapshot_chunk(member, chunk_param,
                                                           timeout_ms_, result);
        RETURN_IF_INVALID_STATUS(status)
        if (!result.success) {
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

}  // namespace adviskv::storage