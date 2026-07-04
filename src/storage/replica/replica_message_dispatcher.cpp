#include "storage/replica/replica_message_dispatcher.h"

#include <mutex>
#include <utility>

#include "common/log.h"
#include "common/metrics/metrics.h"
#include "storage/persist/persist_engine.h"

namespace adviskv::storage {

namespace {

constexpr size_t kReplicaRaftRpcWorkerCount = 8;

}  // namespace

ReplicaMessageDispatcher::ReplicaMessageDispatcher(int32 raft_rpc_timeout_ms,
                                                   PersistEngine& persist,
                                                   std::mutex& persist_snapshot_mutex,
                                                   EventCallback event_callback)
    : sender_(raft_rpc_timeout_ms),
      persist_(persist),
      persist_snapshot_mutex_(persist_snapshot_mutex),
      event_callback_(std::move(event_callback)) {}

ReplicaMessageDispatcher::~ReplicaMessageDispatcher() {
    stop();
}

void ReplicaMessageDispatcher::start() {
    rpc_pool_.start(kReplicaRaftRpcWorkerCount);
}

void ReplicaMessageDispatcher::stop() {
    rpc_pool_.stop();
}

Status ReplicaMessageDispatcher::async_send(std::vector<RaftMessage> messages) {
    for (const RaftMessage& msg : messages) {
        RETURN_IF_INVALID_STATUS(async_send_one(msg))
    }
    return Status::OK();
}

Status ReplicaMessageDispatcher::async_send_one(const RaftMessage& msg) {
    if (!rpc_pool_.started()) {
        return Status::ERROR("raft rpc workers are not started");
    }

    rpc_pool_.submit([this, msg]() { send_task(msg); });
    return Status::OK();
}

Status ReplicaMessageDispatcher::sync_send_append_entries(
    const PeerMember& target, const AppendEntriesParam& param,
    AppendEntriesResult& result) {
    return sender_.send_append_entries(target, param, result);
}

Status ReplicaMessageDispatcher::sync_send_install_snapshot(
    const PeerMember& target, const InstallSnapshotParam& param,
    InstallSnapshotResult& result) {
    std::lock_guard locker{persist_snapshot_mutex_};
    return sender_.send_install_snapshot(target, param, persist_, result);
}

void ReplicaMessageDispatcher::send_task(RaftMessage msg) {
    switch (msg.type) {
        case RaftMessageType::REQUEST_VOTE: {
            ADVISKV_METRICS_TIMER("storage_raft_flush_messages_request_vote");

            RequestVoteResult result;
            Status status = sender_.send_request_vote(msg.target, msg.vote_param, result);
            if (status.ok()) {
                callback(VoteResponseEvent{msg.target.replica_id, result});
            } else {
                LOG_WARN("[flush_messages] send_request_vote failed: {}", status.msg());
            }
            break;
        }
        case RaftMessageType::APPEND_ENTRIES: {
            AppendEntriesResult result;
            Status status = sender_.send_append_entries(msg.target, msg.append_param, result);
            if (status.ok()) {
                callback(AppendResponseEvent{
                    msg.target.replica_id, msg.append_param, result});
            } else {
                LOG_WARN("storage raft append entries failed, status:{}", status.to_string());
                callback(AppendSendFailedEvent{
                    msg.target.replica_id, msg.append_param, status});
            }
            break;
        }
        case RaftMessageType::INSTALL_SNAPSHOT: {
            ADVISKV_METRICS_TIMER("storage_raft_flush_messages_install_snapshot");

            InstallSnapshotResult result;
            Status status;
            {
                std::lock_guard locker{persist_snapshot_mutex_};
                status = sender_.send_install_snapshot(
                    msg.target, msg.snapshot_param, persist_, result);
            }
            if (status.ok()) {
                callback(SnapshotResponseEvent{
                    msg.target.replica_id, msg.snapshot_param, result});
            } else {
                LOG_WARN("storage raft send install snapshot failed, status:{}", status.to_string());
                callback(SnapshotSendFailedEvent{
                    msg.target.replica_id, msg.snapshot_param, status});
            }
            break;
        }
    }
}

void ReplicaMessageDispatcher::callback(Event event) {
    if (!event_callback_) {
        LOG_WARN("drop raft response event because event sink is not set");
        return;
    }
    event_callback_(std::move(event));
}

}  // namespace adviskv::storage