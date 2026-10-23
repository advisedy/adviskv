#include "storage/replica/replica_loop.h"

#include <cstddef>
#include <utility>
#include <variant>

#include "common/constants.h"
#include "common/defer.h"
#include "common/define.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "common/status.h"
#include "storage/model/model.h"
#include "storage/model/param.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/core/raft_core.h"
#include "storage/replica/proposal_tracker.h"
#include "storage/replica/raft_step.h"
#include "storage/replica/replica_message_dispatcher.h"

namespace adviskv::storage {

namespace {

thread_local const ReplicaLoop* current_replica_loop = nullptr;

}  // namespace

ReplicaLoop::ReplicaLoop(ReplicaID replica_id, RaftCore& raft_core, PersistEngine& persist, StateMachine& state_machine,
                         ReplicaMessageDispatcher& message_dispatcher, FaultReporter fault_reporter,
                         int32_t raft_do_snapshot_limit, std::function<void()> runtime_info_publisher)
        : replica_id_(std::move(replica_id)),
          raft_core_(raft_core),
          persist_(persist),
          state_machine_(state_machine),
          message_dispatcher_(message_dispatcher),
          fault_reporter_(std::move(fault_reporter)),
          raft_effect_executor_(persist, message_dispatcher, fault_reporter_),
          proposal_tracker_(raft_core, state_machine, raft_effect_executor_),
          read_tracker_(raft_core, state_machine, message_dispatcher_, fault_reporter_),
          snapshot_tracker_(replica_id_, raft_core, state_machine, persist, raft_effect_executor_, fault_reporter_,
                            raft_do_snapshot_limit,
                            [this](LogIndex capture_index, Term capture_term, Status status) {
                                on_snapshot_written(capture_index, capture_term, std::move(status));
                            }),
          runtime_info_publisher_(std::move(runtime_info_publisher)) {}

ReplicaLoop::~ReplicaLoop() { stop(); }

void ReplicaLoop::start() {
    if (started_.exchange(true)) {
        return;
    }
    active_.store(true);
    worker_ = std::thread(&ReplicaLoop::run, this);
}

bool ReplicaLoop::post(PendingTask task) {
    if (!active_.load()) {
        return false;
    }
    return queue_.push(std::move(task));
}

void ReplicaLoop::run() {
    current_replica_loop = this;
    auto cleanup = Defer([this]() {
        next_task_.reset();
        fail_all_pending(faulted_ ? Status::IS_FAULTED("replica is faulted") : Status::ERROR("raft loop is stopped"));
        current_replica_loop = nullptr;
    });

    while (active_.load()) {
        PendingTask next;
        if (next_task_.has_value()) {
            next = std::move(*next_task_);
            next_task_.reset();
        } else if (!queue_.pop(next)) {
            break;
        }

        if (!active_.load()) {
            break;
        }

        Status status = std::visit([this](auto& item) -> Status { return dispatch(item); }, next);
        if (status.fail()) {
            LOG_WARN("raft work failed, replica_id:{}, status:{}", replica_id_.to_string(), status.to_string());
        }
        if (!active_.load()) {
            break;
        }
        advance_trackers();
    }
}

std::vector<QueuedCall<ProposeCall>> ReplicaLoop::collect_write_batch(QueuedCall<ProposeCall> first) {
    std::vector<QueuedCall<ProposeCall>> calls;
    calls.reserve(K_REPLICA_MAX_PROPOSAL_BATCH_SIZE);
    calls.push_back(std::move(first));

    while (calls.size() < K_REPLICA_MAX_PROPOSAL_BATCH_SIZE) {
        PendingTask next;
        if (!queue_.try_pop(next)) {
            break;
        }
        if (auto* call = std::get_if<QueuedCall<ProposeCall>>(&next)) {
            calls.push_back(std::move(*call));
            continue;
        }
        next_task_ = std::move(next);
        break;
    }
    return calls;
}

Status ReplicaLoop::finish_dispatch(Status status) {
    if (!faulted_) {
        Status apply_status = apply_committed_entries_inline();
        if (apply_status.fail()) {
            if (status.ok()) {
                status = std::move(apply_status);
            } else {
                LOG_WARN("apply committed entries failed after a failed request, status:{}", apply_status.to_string());
            }
        }
    }

    publish_runtime_info_snapshot();
    if (status.ok() && faulted_) {
        return Status::IS_FAULTED("replica is faulted");
    }
    return status;
}

void ReplicaLoop::close() {
    started_.store(true);
    active_.store(false);
    queue_.close();
    completion_registry_.close(Status::ERROR("raft loop is stopped"));
}

void ReplicaLoop::stop() {
    close();
    if (worker_.joinable() && worker_.get_id() != std::this_thread::get_id()) {
        worker_.join();
    }
    snapshot_tracker_.stop_writer();
}

void ReplicaLoop::submit_async(Event event) {
    std::visit(
            [this](auto& concrete) {
                if (!post(PendingTask{std::move(concrete)})) {
                    LOG_WARN("drop event because raft loop is not running");
                }
            },
            event);
}

void ReplicaLoop::advance_trackers() {
    proposal_tracker_.advance();
    read_tracker_.advance();
    if (!faulted_) {
        snapshot_tracker_.advance();
    }
}

void ReplicaLoop::on_snapshot_written(LogIndex capture_index, Term capture_term, Status status) {
    submit_async(SnapshotDoneEvent{capture_index, capture_term, std::move(status)});
}

Status ReplicaLoop::handle_event(const TickEvent& event) {
    UNUSED(event);
    return raft_effect_executor_.execute([this](RaftEffects& effects) {
        raft_core_.tick(effects);
        return Status::OK();
    });
}

Status ReplicaLoop::handle_event(const AppendResponseEvent& event) {
    return raft_effect_executor_.execute([this, &event](RaftEffects& effects) {
        return raft_core_.handle_append_response(event.from, event.sent_param, event.result, effects);
    });
}

Status ReplicaLoop::handle_event(const VoteResponseEvent& event) {
    return raft_effect_executor_.execute([this, &event](RaftEffects& effects) {
        raft_core_.handle_vote_response(event.from, event.result, effects);
        return Status::OK();
    });
}

Status ReplicaLoop::handle_event(const PreVoteResponseEvent& event) {
    return raft_effect_executor_.execute([this, &event](RaftEffects& effects) {
        raft_core_.handle_pre_vote_response(event.from, event.result, effects);
        return Status::OK();
    });
}

Status ReplicaLoop::handle_event(const SnapshotResponseEvent& event) {
    return raft_effect_executor_.execute([this, &event](RaftEffects& effects) {
        raft_core_.handle_install_snapshot_response(event.from, event.sent_param, event.result, effects);
        return Status::OK();
    });
}

Status ReplicaLoop::handle_event(const AppendSendFailedEvent& event) {
    return raft_effect_executor_.execute([this, &event](RaftEffects&) {
        raft_core_.handle_append_send_failed(event.from, event.sent_param, event.status);
        return Status::OK();
    });
}

Status ReplicaLoop::handle_event(const SnapshotSendFailedEvent& event) {
    return raft_effect_executor_.execute([this, &event](RaftEffects&) {
        raft_core_.handle_install_snapshot_send_failed(event.from, event.sent_param, event.status);
        return Status::OK();
    });
}

Status ReplicaLoop::handle_event(const FaultEvent& event) {
    LOG_WARN("[ReplicaLoop] fault event, replica_id:{}, status:{}", replica_id_.to_string(), event.status.to_string());
    enter_fault();
    return Status::OK();
}

Status ReplicaLoop::handle_event(const ReadQuorumResponseEvent& event) {
    read_tracker_.on_read_quorum_response(event.from, event.read_seq, event.response_term);
    return Status::OK();
}

Status ReplicaLoop::handle_event(const SnapshotDoneEvent& event) { return snapshot_tracker_.on_snapshot_done(event); }

void ReplicaLoop::enter_fault() {
    active_.store(false);
    queue_.close();
    faulted_ = true;
}

void ReplicaLoop::fail_all_pending(const Status& status) {
    read_tracker_.fail_all(status);
    proposal_tracker_.fail_all(status);
    completion_registry_.close(status);
}

bool ReplicaLoop::on_loop_thread() const { return current_replica_loop == this; }

Status ReplicaLoop::apply_one_entry(const LogEntry& entry) {
    switch (entry.op_type) {
        case WriteOpType::PUT:
        case WriteOpType::DEL:
        case WriteOpType::CAS:
        case WriteOpType::NONE: {
            Status status = state_machine_.apply(entry);
            if (status.fail()) {
                ADVISKV_METRICS_COUNTER("storage_replica_apply_entry_failure");
                LOG_WARN("apply kv log entry failed, index={}, msg={}", entry.index, status.msg());
                return status;
            }
            ADVISKV_METRICS_COUNTER("storage_replica_apply_entry_success");
            raft_core_.advance_last_applied(entry.index);
            return Status::OK();
        }
        case WriteOpType::ADD_LEARNER:
        case WriteOpType::PROMOTE_VOTER:
        case WriteOpType::REMOVE_MEMBER: {
            Status status = raft_core_.apply_config_entry(entry);
            if (status.fail()) {
                ADVISKV_METRICS_COUNTER("storage_replica_apply_entry_failure");
                LOG_WARN("apply config log entry failed, index={}, msg={}", entry.index, status.msg());
                return status;
            }
            status = state_machine_.apply(entry);
            if (status.fail()) {
                ADVISKV_METRICS_COUNTER("storage_replica_apply_entry_failure");
                LOG_WARN("apply config log entry state machine failed, index={}, msg={}", entry.index, status.msg());
                return status;
            }
            ADVISKV_METRICS_COUNTER("storage_replica_apply_entry_success");
            return Status::OK();
        }
    }

    return Status{StatusCode::ERROR, "unsupported raft log entry type"};
}

Status ReplicaLoop::apply_committed_entries_inline() {
    ADVISKV_METRICS_TIMER("storage_replica_apply_committed_entries");
    ADVISKV_METRICS_COUNTER("storage_replica_apply_committed_entries_request");

    while (true) {
        bool has_more = false;
        std::vector<LogEntry> entries =
                raft_core_.extract_committed_entries(K_REPLICA_INLINE_APPLY_BATCH_SIZE, 0, &has_more);
        if (entries.empty()) {
            break;
        }
        ADVISKV_METRICS_COUNTER("storage_replica_apply_entry", to<int64_t>(entries.size()));

        for (const LogEntry& entry : entries) {
            Status status = fault_reporter_.fail_if(apply_one_entry(entry));
            RETURN_IF_INVALID_STATUS(status)
        }
        if (!has_more) {
            break;
        }
    }

    return Status::OK();
}

CompletionResult<RequestVoteResult> ReplicaLoop::handle_call(const RequestVoteCall& call) {
    RequestVoteResult result;
    Status status = raft_effect_executor_.execute([this, &call, &result](RaftEffects& effects) {
        raft_core_.handle_request_vote(call.param, result, effects);
        return Status::OK();
    });
    return status.ok() ? CompletionResult<RequestVoteResult>::ok(std::move(result))
                       : CompletionResult<RequestVoteResult>::fail(status);
}

CompletionResult<PreVoteResult> ReplicaLoop::handle_call(const PreVoteCall& call) {
    PreVoteResult result;
    Status status = raft_effect_executor_.execute([this, &call, &result](RaftEffects& effects) {
        raft_core_.handle_pre_vote(call.param, result, effects);
        return Status::OK();
    });
    return status.ok() ? CompletionResult<PreVoteResult>::ok(std::move(result))
                       : CompletionResult<PreVoteResult>::fail(status);
}

CompletionResult<AppendEntriesResult> ReplicaLoop::handle_call(const AppendEntriesCall& call) {
    ADVISKV_METRICS_TIMER("storage_replica_append_entries_raft_step_execute");
    AppendEntriesResult result;
    Status status = raft_effect_executor_.execute([this, &call, &result](RaftEffects& effects) {
        raft_core_.handle_append_entries(call.param, result, effects);
        return Status::OK();
    });
    return status.ok() ? CompletionResult<AppendEntriesResult>::ok(std::move(result))
                       : CompletionResult<AppendEntriesResult>::fail(status);
}

CompletionResult<void> ReplicaLoop::handle_call(const InstallSnapshotCall& call) {
    Status status = snapshot_tracker_.on_install_snapshot_chunk(call.param);
    return status.ok() ? CompletionResult<void>::ok() : CompletionResult<void>::fail(status);
}

CompletionResult<void> ReplicaLoop::handle_call(const AddMemberCall& call) {
    Status status = raft_effect_executor_.execute(
            [this, &call](RaftEffects& effects) { return raft_core_.ensure_add_learner(call.member, effects); });
    return status.ok() ? CompletionResult<void>::ok() : CompletionResult<void>::fail(status);
}

CompletionResult<void> ReplicaLoop::handle_call(const RemoveMemberCall& call) {
    Status status = raft_effect_executor_.execute(
            [this, &call](RaftEffects& effects) { return raft_core_.ensure_remove_member(call.replica_id, effects); });
    return status.ok() ? CompletionResult<void>::ok() : CompletionResult<void>::fail(status);
}

CompletionResult<void> ReplicaLoop::handle_call(const RecoverCall& call) {
    UNUSED(call);
    PersistEngine::RecoverResult result;
    Status recover_status = persist_.recover(result);
    if (recover_status.fail()) {
        LOG_WARN("[ReplicaLoop] persist recover failed, replica_id:{}, status:{}", replica_id_.to_string(),
                 recover_status.to_string());
        return CompletionResult<void>::fail(recover_status);
    }

    if (result.snapshot) {
        std::unique_ptr<SnapshotFileReader> reader;
        bool exists = false;
        Status status = fault_reporter_.fail_if(persist_.snapshot().open_reader(reader, exists));
        if (status.fail()) {
            return CompletionResult<void>::fail(status);
        }
        if (exists && reader) {
            status = fault_reporter_.fail_if(state_machine_.restore(
                    result.snapshot,
                    [&reader](const KvVisitor& visitor) -> Status { return reader->for_each_kv(visitor); },
                    [&reader](const RequestVisitor& visitor) -> Status { return reader->for_each_request(visitor); }));
            if (status.fail()) {
                return CompletionResult<void>::fail(status);
            }
        }
        RaftEffects unused_effects;
        raft_core_.install_snapshot(
                SnapshotHeader{result.snapshot->apply_index, result.snapshot->apply_term, result.snapshot->members},
                unused_effects);
    }

    raft_core_.update_raft_meta(result.raft_meta);
    raft_core_.update_log_entries(result.wal_entries);

    if (result.need_recover) {
        raft_core_.enter_recovering();
    }

    return CompletionResult<void>::ok();
}

}  // namespace adviskv::storage
