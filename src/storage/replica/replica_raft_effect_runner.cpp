#include "storage/replica/replica_raft_effect_runner.h"

#include <mutex>

#include "common/define.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/raft_node.h"
#include "storage/raft/raft_sender.h"

namespace adviskv::storage {

ReplicaRaftEffectRunner::ReplicaRaftEffectRunner(ReplicaContext& context)
    : context_(context) {}

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

Status ReplicaRaftEffectRunner::send_raft_messages(
    std::vector<RaftMessage> messages) {
    for (const RaftMessage& msg : messages) {
        RETURN_IF_INVALID_STATUS(send_raft_message(msg))
    }
    return Status::OK();
}

Status ReplicaRaftEffectRunner::send_raft_message(const RaftMessage& msg) {
    switch (msg.type) {
        case RaftMessageType::REQUEST_VOTE: {
            ADVISKV_METRICS_TIMER("storage_raft_flush_messages_request_vote");

            RequestVoteResult result;
            Status status = context_.raft_sender.send_request_vote(
                msg.target, msg.vote_param, result);
            if (status.ok()) {
                RETURN_IF_INVALID_STATUS(
                    run_raft_step([&](RaftEffects& effects) {
                        context_.raft_node.handle_vote_response(
                            msg.target.replica_id, result, effects);
                        return Status::OK();
                    }))
            } else {
                LOG_WARN("[flush_messages] send_request_vote failed: {}",
                         status.msg());
            }
            break;
        }
        case RaftMessageType::APPEND_ENTRIES: {
            AppendEntriesResult result;
            Status status = context_.raft_sender.send_append_entries(
                msg.target, msg.append_param, result);
            if (status.ok()) {
                RETURN_IF_INVALID_STATUS(
                    run_raft_step([&](RaftEffects& effects) {
                        IGNORE_RESULT(context_.raft_node.handle_append_response(
                            msg.target.replica_id, msg.append_param, result,
                            effects));
                        return Status::OK();
                    }))
            } else {
                LOG_WARN("storage raft append entries failed, status:{}",
                         status.to_string());
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
                status = context_.raft_sender.send_install_snapshot(
                    msg.target, msg.snapshot_param, context_.persist, result);
            }
            if (status.ok()) {
                RETURN_IF_INVALID_STATUS(
                    run_raft_step([&](RaftEffects& effects) {
                        context_.raft_node.handle_install_snapshot_response(
                            msg.target.replica_id, msg.snapshot_param, result,
                            effects);
                        return Status::OK();
                    }))
            } else {
                LOG_WARN("storage raft send install snapshot failed, status:{}",
                         status.to_string());
                context_.raft_node.handle_install_snapshot_send_failed(
                    msg.target.replica_id, msg.snapshot_param, status);
            }
            break;
        }
    }

    return Status::OK();
}

}  // namespace adviskv::storage