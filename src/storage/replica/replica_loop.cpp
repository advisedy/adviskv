#include "storage/replica/replica_loop.h"

#include <cstddef>
#include <mutex>
#include <utility>

#include "common/define.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "common/status.h"
#include "storage/model/param.h"
#include "storage/raft/core/raft_core.h"
#include "storage/replica/raft_step.h"
#include "storage/replica/proposal_tracker.h"

namespace adviskv::storage {

namespace {

constexpr size_t K_MAX_PROPOSAL_BATCH_SIZE = 64;

}  // namespace

ReplicaLoop::ReplicaLoop(ReplicaContext& context)
        : context_(context), proposal_tracker_(context_, completions_), queue_(proposal_tracker_) {}

ReplicaLoop::~ReplicaLoop() { stop(); }

void ReplicaLoop::start() {
    queue_.start(K_MAX_PROPOSAL_BATCH_SIZE);
}

void ReplicaLoop::stop() {
    queue_.stop();
    Status stopped = Status::ERROR("raft loop is stopped");
    proposal_tracker_.fail_all(stopped);
    completions_.fail_all(stopped);
}

void ReplicaLoop::async_submit(Event event) {
    bool res = std::visit(
            [this](auto concrete_event) -> bool {
                return queue_.post_task([this, event = std::move(concrete_event)]() {
                    Status status = handle_event(event);
                    if (status.fail()) {
                        LOG_WARN("async raft event failed, status:{}", status.to_string());
                    }
                });
            },
            std::move(event));
    if (!res) {
        LOG_WARN("drop event because raft loop is not running");
    }
}

template <typename T>
bool ReplicaLoop::enqueue_call(T& call, CompletionManager::Ticket ticket) {
    return queue_.post_task([this, &call, ticket]() {
        Status status = handle_call(call);
        IGNORE_RESULT(completions_.finish(ticket, status))
    });
}

bool ReplicaLoop::enqueue_call(ProposeCall& call, CompletionManager::Ticket ticket) {
    return queue_.post_write(std::move(call.param), ticket);
}

template <typename T>
Status ReplicaLoop::wait_call(const T& call, CompletionManager::Ticket ticket) {
    UNUSED(call);
    return completions_.wait(ticket);
}

Status ReplicaLoop::wait_call(const ProposeCall& call, CompletionManager::Ticket ticket) {
    return completions_.wait_for(ticket, call.timeout,
                                 Status::NOT_YET_COMMIT("proposal is not committed before timeout"));
}

void ReplicaLoop::sync_submit(Call call) {
    std::visit(
            [this](auto* concrete_call) {
                if (concrete_call == nullptr) {
                    LOG_WARN("drop null raft loop call");
                    return;
                }

                CompletionManager::Ticket ticket = completions_.create();
                if (!enqueue_call(*concrete_call, ticket)) {
                    IGNORE_RESULT(completions_.finish(ticket, Status::ERROR("raft loop is not running")))
                }
                concrete_call->status = wait_call(*concrete_call, ticket);
            },
            call);
}

Status ReplicaLoop::handle_event(const TickEvent& event) {
    UNUSED(event);
    Status status = run_raft_step(context_, [this](RaftEffects& effects) {
        context_.raft_core.tick(effects);
        return Status::OK();
    });
    proposal_tracker_.advance();
    return status;
}

Status ReplicaLoop::handle_event(const AppendResponseEvent& event) {
    Status status = run_raft_step(context_, [this, &event](RaftEffects& effects) {
        return context_.raft_core.handle_append_response(event.from, event.sent_param, event.result, effects);
    });
    proposal_tracker_.advance();
    return status;
}

Status ReplicaLoop::handle_event(const VoteResponseEvent& event) {
    Status status = run_raft_step(context_, [this, &event](RaftEffects& effects) {
        context_.raft_core.handle_vote_response(event.from, event.result, effects);
        return Status::OK();
    });
    proposal_tracker_.advance();
    return status;
}

Status ReplicaLoop::handle_event(const PreVoteResponseEvent& event) {
    Status status = run_raft_step(context_, [this, &event](RaftEffects& effects) {
        context_.raft_core.handle_pre_vote_response(event.from, event.result, effects);
        return Status::OK();
    });
    proposal_tracker_.advance();
    return status;
}

Status ReplicaLoop::handle_event(const SnapshotResponseEvent& event) {
    Status status = run_raft_step(context_, [this, &event](RaftEffects& effects) {
        context_.raft_core.handle_install_snapshot_response(event.from, event.sent_param, event.result, effects);
        return Status::OK();
    });
    proposal_tracker_.advance();
    return status;
}

Status ReplicaLoop::handle_event(const AppendSendFailedEvent& event) {
    Status status = run_raft_step(context_, [this, &event](RaftEffects&) {
        context_.raft_core.handle_append_send_failed(event.from, event.sent_param, event.status);
        return Status::OK();
    });
    proposal_tracker_.advance();
    return status;
}

Status ReplicaLoop::handle_event(const SnapshotSendFailedEvent& event) {
    Status status = run_raft_step(context_, [this, &event](RaftEffects&) {
        context_.raft_core.handle_install_snapshot_send_failed(event.from, event.sent_param, event.status);
        return Status::OK();
    });
    proposal_tracker_.advance();
    return status;
}

Status ReplicaLoop::handle_call(RequestVoteCall& call) {
    return run_raft_step(context_, [this, &call](RaftEffects& effects) {
        LOG_DEBUG(
                "[ReplicaLoop] handle RequestVote, replica_id:{}, from:{}, term:{}, last_log_index:{}, "
                "last_log_term:{}",
                context_.replica_id.to_string(), call.param.from_replica_id.to_string(), call.param.term,
                call.param.last_log_index, call.param.last_log_term);
        context_.raft_core.handle_request_vote(call.param, call.result, effects);
        LOG_DEBUG(
                "[ReplicaLoop] handle RequestVote finished, replica_id:{}, from:{}, request_term:{}, "
                "response_term:{}, vote_granted:{}",
                context_.replica_id.to_string(), call.param.from_replica_id.to_string(), call.param.term,
                call.result.term, call.result.vote_granted);
        return Status::OK();
    });
}

Status ReplicaLoop::handle_call(PreVoteCall& call) {
    return run_raft_step(context_, [this, &call](RaftEffects& effects) {
        LOG_DEBUG(
                "[ReplicaLoop] handle PreVote, replica_id:{}, from:{}, term:{}, last_log_index:{}, "
                "last_log_term:{}",
                context_.replica_id.to_string(), call.param.from_replica_id.to_string(), call.param.term,
                call.param.last_log_index, call.param.last_log_term);
        context_.raft_core.handle_pre_vote(call.param, call.result, effects);
        LOG_DEBUG(
                "[ReplicaLoop] handle PreVote finished, replica_id:{}, from:{}, request_term:{}, "
                "response_term:{}, vote_granted:{}",
                context_.replica_id.to_string(), call.param.from_replica_id.to_string(), call.param.term,
                call.result.term, call.result.vote_granted);
        return Status::OK();
    });
}

Status ReplicaLoop::handle_call(AppendEntriesCall& call) {
    ADVISKV_METRICS_TIMER("storage_replica_append_entries_raft_step_execute");
    return run_raft_step(context_, [this, &call](RaftEffects& effects) {
        context_.raft_core.handle_append_entries(call.param, call.result, effects);
        return Status::OK();
    });
}

Status ReplicaLoop::handle_call(BuildReadIndexCall& call) {
    std::lock_guard lock(context_.raft_core_mutex);
    return context_.raft_core.build_append_entries_for_read(call.effects, call.read_index, call.read_term);
}

Status ReplicaLoop::handle_call(PrepareInstallSnapshotCall& call) {
    return run_raft_step(context_, [this, &call](RaftEffects& effects) {
        return context_.raft_core.prepare_install_snapshot(call.param, effects);
    });
}

Status ReplicaLoop::handle_call(CommitInstallSnapshotCall& call) {
    return run_raft_step(context_, [this, &call](RaftEffects& effects) {
        context_.raft_core.commit_install_snapshot(call.context, effects);
        return Status::OK();
    });
}

Status ReplicaLoop::handle_call(AppendResponseCall& call) { return handle_event(call.event); }

Status ReplicaLoop::handle_call(SnapshotResponseCall& call) { return handle_event(call.event); }

Status ReplicaLoop::handle_call(SnapshotSendFailedCall& call) { return handle_event(call.event); }

Status ReplicaLoop::handle_call(AddMemberCall& call) {
    Status status = run_raft_step(context_, [this, &call](RaftEffects& effects) {
        return context_.raft_core.ensure_add_learner(call.member, effects);
    });
    proposal_tracker_.advance();
    return status;
}

Status ReplicaLoop::handle_call(RemoveMemberCall& call) {
    Status status = run_raft_step(context_, [this, &call](RaftEffects& effects) {
        return context_.raft_core.ensure_remove_member(call.replica_id, effects);
    });
    proposal_tracker_.advance();
    return status;
}

}  // namespace adviskv::storage
