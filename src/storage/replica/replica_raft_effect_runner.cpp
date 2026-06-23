#include "storage/replica/replica_raft_effect_runner.h"

#include <mutex>
#include <utility>

#include "common/define.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/raft_node.h"
#include "storage/raft/raft_sender.h"

namespace adviskv::storage {

ReplicaRaftEffectRunner::ReplicaRaftEffectRunner(ReplicaContext& context,
                                                 int32 raft_rpc_timeout_ms)
    : context_(context), raft_sender_(raft_rpc_timeout_ms) {}

ReplicaRaftEffectRunner::~ReplicaRaftEffectRunner() { stop_rpc_workers(); }

void ReplicaRaftEffectRunner::set_response_handlers(ResponseHandlers handlers) {
    response_handlers_ = std::move(handlers);
}

void ReplicaRaftEffectRunner::start_rpc_workers(size_t worker_count) {
    rpc_pool_.start(worker_count);
}

void ReplicaRaftEffectRunner::stop_rpc_workers() { rpc_pool_.stop(); }

Status ReplicaRaftEffectRunner::run_raft_step(RaftStepFunc&& step) {
    RaftEffects effects;
    {
        std::unique_lock lock(context_.raft_step_mutex);
        Status status = step(effects);
        RETURN_IF_INVALID_STATUS(persist_raft_effects(effects))
        RETURN_IF_INVALID_STATUS(status)
    }
    return send_raft_messages(std::move(effects.messages));
}

Status ReplicaRaftEffectRunner::persist_raft_effects(
    const RaftEffects& effects) {
    if (effects.entries_to_rewrite.has_value() &&
        !effects.entries_to_append.empty()) {
        return Status::INVALID_ARGUMENT("cannot both rewrite and append");
    }

    if (effects.hard_state.has_value()) {
        RETURN_IF_INVALID_STATUS(context_.fault_if_fail(
            context_.persist.save_raft_meta(*effects.hard_state)))
    }

    if (effects.entries_to_rewrite.has_value()) {
        RETURN_IF_INVALID_STATUS(context_.fault_if_fail(
            context_.persist.rewrite_wal(*effects.entries_to_rewrite)))
    }

    if (!effects.entries_to_append.empty()) {
        RETURN_IF_INVALID_STATUS(context_.fault_if_fail(
            context_.persist.append_wal_batch(effects.entries_to_append)))
    }

    return Status::OK();
}

Status ReplicaRaftEffectRunner::sync_send_append_entries(
    const PeerMember& member, const AppendEntriesParam& param,
    AppendEntriesResult& result) {
    return raft_sender_.send_append_entries(member, param, result);
}

Status ReplicaRaftEffectRunner::sync_send_install_snapshot(
    const PeerMember& member, const InstallSnapshotParam& param,
    InstallSnapshotResult& result) {
    std::lock_guard locker{context_.persist_snapshot_mutex};
    return raft_sender_.send_install_snapshot(member, param, context_.persist,
                                              result);
}

Status ReplicaRaftEffectRunner::send_raft_messages(
    std::vector<RaftMessage> messages) {
    for (const RaftMessage& msg : messages) {
        RETURN_IF_INVALID_STATUS(send_raft_message(msg))
    }
    return Status::OK();
}

Status ReplicaRaftEffectRunner::send_raft_message(const RaftMessage& msg) {
    if (!rpc_pool_.started()) {
        return Status::ERROR("raft rpc workers are not started");
    }

    rpc_pool_.submit([this, msg] { send_raft_message_task(msg); });
    return Status::OK();
}

void ReplicaRaftEffectRunner::send_raft_message_task(RaftMessage msg) {
    switch (msg.type) {
        case RaftMessageType::REQUEST_VOTE: {
            ADVISKV_METRICS_TIMER("storage_raft_flush_messages_request_vote");

            RequestVoteResult result;
            Status status = raft_sender_.send_request_vote(
                msg.target, msg.vote_param, result);
            if (status.ok()) {
                if (response_handlers_.request_vote) {
                    response_handlers_.request_vote(msg.target.replica_id,
                                                    result);
                }
            } else {
                LOG_WARN("[flush_messages] send_request_vote failed: {}",
                         status.msg());
            }
            break;
        }
        case RaftMessageType::APPEND_ENTRIES: {
            AppendEntriesResult result;
            Status status = raft_sender_.send_append_entries(
                msg.target, msg.append_param, result);
            if (status.ok()) {
                if (response_handlers_.append_entries) {
                    response_handlers_.append_entries(msg.target.replica_id,
                                                      msg.append_param, result);
                }
            } else {
                LOG_WARN("storage raft append entries failed, status:{}",
                         status.to_string());
                if (response_handlers_.append_entries_failed) {
                    response_handlers_.append_entries_failed(
                        msg.target.replica_id, msg.append_param, status);
                }
            }
            break;
        }
        case RaftMessageType::INSTALL_SNAPSHOT: {
            ADVISKV_METRICS_TIMER(
                "storage_raft_flush_messages_install_snapshot");

            InstallSnapshotResult result;
            Status status;
            {
                std::lock_guard locker{context_.persist_snapshot_mutex};
                status = raft_sender_.send_install_snapshot(
                    msg.target, msg.snapshot_param, context_.persist, result);
            }
            if (status.ok()) {
                if (response_handlers_.install_snapshot) {
                    response_handlers_.install_snapshot(
                        msg.target.replica_id, msg.snapshot_param, result);
                }
            } else {
                LOG_WARN("storage raft send install snapshot failed, status:{}",
                         status.to_string());

                response_handlers_.install_snapshot_failed(
                    msg.target.replica_id, msg.snapshot_param, status);
            }
            break;
        }
    }
}

}  // namespace adviskv::storage