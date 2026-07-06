#include "storage/raft/raft_sender.h"

#include "common/define.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "common/status.h"
#include "storage/model/model.h"
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
    LOG_DEBUG(
        "[Raft Sender] candidate replica:{} send request vote to replica:{}, msg:[term:{}, "
        "last_log_index:{}, last_log_term:{} ]",
        param.from_replica_id.to_string(), param.to_replica_id.to_string(),
        param.term, param.last_log_index, param.last_log_term);

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

    LOG_DEBUG(
        "[Raft Sender] leader replica:{} sned append entires to replica:{}, msg:[term:{}, "
        "prev_log_index:{}, prev_log_term:{}, leader_commit:{}, "
        "entries_size:{}]",
        param.from_replica_id.to_string(), param.to_replica_id.to_string(),
        param.term, param.prev_log_index, param.prev_log_term,
        param.leader_commit, param.entries.size());

    for (int i = 0, siz = param.entries.size(); i < siz; i++) {
        LOG_DEBUG(
            "[Raft Sender] leader replica:{} send append entires to replica:{}, "
            "entries[{}]:{}",
            param.from_replica_id.to_string(), param.to_replica_id.to_string(),
            i, param.entries[i].to_string());
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

// 这个函数返回的是OK，代表的是'接收'到了下载快照的Response,并不保证Response的返回码是OK。
Status RaftSender::send_install_snapshot(const PeerMember& member,
                                         const InstallSnapshotParam& param,
                                         const PersistEngine& persist,
                                         InstallSnapshotResult& result) const {
    LOG_INFO(
        "[Raft Sender] replica_id:{}, start to send install snapshot to replica_id:{}, "
        "snapshot_index:{}, snapshot_term:{}",
        param.from_replica_id.to_string(), param.to_replica_id.to_string(),
        param.snapshot_index, param.snapshot_term);

    constexpr size_t kChunkSize = 1 << 20;
    uint64 offset = 0;

    // 这里的result代表的是发送单次read_snapshot_chunk的结果
    result = InstallSnapshotResult{};
    result.term = param.term;
    result.status = Status::OK();
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
        if (result.status.fail()) {
            LOG_WARN(
                "[Raft Sender] replica_id:{}, transport install snapshot chunk result "
                "failed, status:{}",
                param.from_replica_id.to_string(), result.status.to_string());
            return Status::OK();
        }

        if (eof) {
            result.status = Status::OK();
            break;
        }
        offset += data.size();
    }

    LOG_INFO(
        "[Raft Sender] replica_id:{}, finish send install snapshot to replica_id:{}, "
        "snapshot_index:{}, snapshot_term:{}",
        param.from_replica_id.to_string(), param.to_replica_id.to_string(),
        param.snapshot_index, param.snapshot_term);

    return Status::OK();
}

}  // namespace adviskv::storage
