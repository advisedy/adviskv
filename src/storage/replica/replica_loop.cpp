#include "storage/replica/replica_loop.h"

#include <cstddef>
#include <utility>

#include "common/define.h"
#include "common/defer.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "common/status.h"
#include "storage/model/model.h"
#include "storage/model/param.h"
#include "storage/raft/core/raft_core.h"
#include "storage/replica/raft_step.h"
#include "storage/replica/proposal_tracker.h"
#include "storage/replica/replica_message_dispatcher.h"
#include "storage/persist/persist_engine.h"
namespace adviskv::storage {

namespace {

constexpr size_t K_MAX_PROPOSAL_BATCH_SIZE = 64;
constexpr size_t K_INLINE_APPLY_BATCH = 256;
thread_local const ReplicaLoop* current_replica_loop = nullptr;

}  // namespace

ReplicaLoop::ReplicaLoop(ReplicaID replica_id, RaftCore& raft_core, PersistEngine& persist,
                         StateMachine& state_machine, ReplicaMessageDispatcher& message_dispatcher,
                         FaultReporter fault_reporter, int32_t raft_do_snapshot_limit,
                         std::function<void()> runtime_info_publisher)
        : replica_id_(std::move(replica_id)),
          raft_core_(raft_core),
          persist_(persist),
          state_machine_(state_machine),
          message_dispatcher_(message_dispatcher),
          fault_reporter_(std::move(fault_reporter)),
          raft_effect_executor_(persist, message_dispatcher, fault_reporter_),
          proposal_tracker_(raft_core, state_machine, raft_effect_executor_, completions_),
          read_tracker_(raft_core, state_machine, message_dispatcher_, completions_, fault_reporter_),
          snapshot_tracker_(replica_id_, raft_core, state_machine, persist, raft_effect_executor_, fault_reporter_,
                            raft_do_snapshot_limit, [this]() { read_tracker_.note_membership_changed(); },
                            [this](LogIndex capture_index, Term capture_term, Status status) {
                                on_snapshot_written(capture_index, capture_term, std::move(status));
                            }),
          runtime_info_publisher_(std::move(runtime_info_publisher)) {}

ReplicaLoop::~ReplicaLoop() {
    stop_snapshot_writer();
    stop();
}

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
    if (const auto* callback = std::get_if<Task>(&task); callback != nullptr && !*callback) {
        return false;
    }
    return queue_.push(std::move(task));
}

void ReplicaLoop::run() {
    current_replica_loop = this;
    auto cleanup = Defer([this]() {
        finish_fault_stop();
        current_replica_loop = nullptr;
    });

    std::optional<PendingTask> pending;
    while (active_.load()) {
        PendingTask next;
        if (pending.has_value()) {
            next = std::move(*pending);
            pending.reset();
        } else if (!queue_.pop(next)) {
            break;
        }

        if (!active_.load()) {
            break;
        }
        if (auto* task = std::get_if<Task>(&next)) {
            (*task)();
            continue;
        }

        std::vector<ProposalItem> items = collect_write_batch(std::move(std::get<ProposalItem>(next)), pending);
        if (!active_.load()) {
            break;
        }
        Status status = handle_write_batch(std::move(items));
        if (status.fail()) {
            LOG_WARN("proposal batch failed, status:{}", status.to_string());
        }
    }
}

std::vector<ProposalItem> ReplicaLoop::collect_write_batch(ProposalItem first,
                                                            std::optional<PendingTask>& pending) {
    std::vector<ProposalItem> items;
    items.reserve(K_MAX_PROPOSAL_BATCH_SIZE);
    items.push_back(std::move(first));

    while (items.size() < K_MAX_PROPOSAL_BATCH_SIZE) {
        PendingTask next;
        if (!queue_.try_pop(next)) {
            break;
        }
        if (auto* item = std::get_if<ProposalItem>(&next)) {
            items.push_back(std::move(*item));
            continue;
        }
        pending.emplace(std::move(next));
        break;
    }
    return items;
}

Status ReplicaLoop::handle_write_batch(std::vector<ProposalItem> items) {
    Status status = proposal_tracker_.submit_batch(std::move(items));
    if (!fault_pending_) {
        Status apply_status = apply_committed_entries_inline();
        if (status.ok()) {
            status = std::move(apply_status);
        }
    }
    finish_work_unit();
    return status;
}

void ReplicaLoop::stop() {
    started_.store(true);
    active_.store(false);
    queue_.close();
    if (worker_.joinable()) {
        if (worker_.get_id() == std::this_thread::get_id()) {
            return;
        }
        worker_.join();
    }
    fail_all_pending(Status::ERROR("raft loop is stopped"));
}

void ReplicaLoop::submit_async(Event event) {
    bool res = std::visit(
            [this](auto concrete_event) -> bool {
                return post(Task{[this, event = std::move(concrete_event)]() {
                    Status status = handle_event(event);
                    if (status.fail()) {
                        LOG_WARN("async raft event failed, status:{}", status.to_string());
                    }
                    finish_work_unit();
                }});
            },
            std::move(event));
    if (!res) {
        LOG_WARN("drop event because raft loop is not running");
    }
}

template <typename T>
bool ReplicaLoop::enqueue_call(T& call, CompletionManager::Ticket ticket) {
    return post(Task{[this, &call, ticket]() {
        Status status = handle_call(call);
        finish_work_unit(ticket, status);
    }});
}

bool ReplicaLoop::enqueue_call(ProposeCall& call, CompletionManager::Ticket ticket) {
    return post(PendingTask{ProposalItem{std::move(call.param), ticket}});
}

bool ReplicaLoop::enqueue_call(SubmitReadCall& call, CompletionManager::Ticket ticket) {
    call.state = read_tracker_.new_request(call.key, ticket);
    auto state = call.state;
    return post(Task{[this, state, ticket]() {
        Status status = read_tracker_.establish(state);
        finish_work_unit(status.fail() ? ticket : 0, status);
    }});
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

Status ReplicaLoop::wait_call(const SubmitReadCall& call, CompletionManager::Ticket ticket) {
    UNUSED(call);
    return completions_.wait_for(ticket, ReadTracker::wait_deadline(),
                                 Status::NOT_LEADER("read index quorum not reached before timeout"));
}

void ReplicaLoop::submit_and_wait(Call call) {
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
                LOG_WARN("apply config log entry state machine failed, index={}, msg={}", entry.index,
                         status.msg());
                return status;
            }
            read_tracker_.note_membership_changed();
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
                raft_core_.extract_committed_entries(K_INLINE_APPLY_BATCH, 0, &has_more);
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

void ReplicaLoop::advance_pending_work() {
    proposal_tracker_.advance();
    read_tracker_.advance();
    if (!fault_pending_) {
        snapshot_tracker_.advance();
    }
}

void ReplicaLoop::finish_work_unit(CompletionManager::Ticket ticket, const Status& status) {
    publish_runtime_info_snapshot();
    if (ticket != 0 && (status.fail() || !fault_pending_)) {
        IGNORE_RESULT(completions_.finish(ticket, status))
    }
    finish_fault_stop();
    if (active_.load()) {
        advance_pending_work();
    }
    finish_fault_stop();
}

void ReplicaLoop::on_snapshot_written(LogIndex capture_index, Term capture_term, Status status) {
    post(Task{[this, capture_index, capture_term, status = std::move(status)]() mutable {
        Status handle_status = handle_event(SnapshotDoneEvent{capture_index, capture_term, std::move(status)});
        if (handle_status.fail()) {
            LOG_WARN("snapshot done event failed, status:{}", handle_status.to_string());
        }
        finish_work_unit();
    }});
}

Status ReplicaLoop::handle_event(const TickEvent& event) {
    UNUSED(event);
    Status status = raft_effect_executor_.execute([this](RaftEffects& effects) {
        raft_core_.tick(effects);
        return Status::OK();
    });
    Status apply_status = apply_committed_entries_inline();
    if (apply_status.fail()) {
        return apply_status;
    }
    return status;
}

Status ReplicaLoop::handle_event(const AppendResponseEvent& event) {
    Status status = raft_effect_executor_.execute([this, &event](RaftEffects& effects) {
        return raft_core_.handle_append_response(event.from, event.sent_param, event.result, effects);
    });
    Status apply_status = apply_committed_entries_inline();
    if (apply_status.fail()) {
        return apply_status;
    }
    return status;
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
    LOG_WARN("[ReplicaLoop] fault event, replica_id:{}, status:{}", replica_id_.to_string(),
             event.status.to_string());
    fault_stop();
    return Status::OK();
}

Status ReplicaLoop::handle_event(const ReadQuorumResponseEvent& event) {
    read_tracker_.on_read_quorum_response(event.from, event.read_seq, event.response_term);
    return Status::OK();
}

Status ReplicaLoop::handle_event(const SnapshotDoneEvent& event) {
    return snapshot_tracker_.on_snapshot_done(event);
}

void ReplicaLoop::fault_stop() {
    active_.store(false);
    queue_.close();
    fault_pending_ = true;
}

void ReplicaLoop::finish_fault_stop() {
    if (!fault_pending_) {
        return;
    }
    fault_pending_ = false;
    fail_all_pending(Status::IS_FAULTED("replica is faulted"));
}

void ReplicaLoop::fail_all_pending(const Status& status) {
    read_tracker_.fail_all(status);
    proposal_tracker_.fail_all(status);
    completions_.fail_all(status);
}

bool ReplicaLoop::on_loop_thread() const {
    return current_replica_loop == this;
}

Status ReplicaLoop::handle_call(RequestVoteCall& call) {
    return raft_effect_executor_.execute([this, &call](RaftEffects& effects) {
        LOG_DEBUG(
                "[ReplicaLoop] handle RequestVote, replica_id:{}, from:{}, term:{}, last_log_index:{}, "
                "last_log_term:{}",
                replica_id_.to_string(), call.param.from_replica_id.to_string(), call.param.term,
                call.param.last_log_index, call.param.last_log_term);
        raft_core_.handle_request_vote(call.param, call.result, effects);
        LOG_DEBUG(
                "[ReplicaLoop] handle RequestVote finished, replica_id:{}, from:{}, request_term:{}, "
                "response_term:{}, vote_granted:{}",
                replica_id_.to_string(), call.param.from_replica_id.to_string(), call.param.term,
                call.result.term, call.result.vote_granted);
        return Status::OK();
    });
}

Status ReplicaLoop::handle_call(PreVoteCall& call) {
    return raft_effect_executor_.execute([this, &call](RaftEffects& effects) {
        LOG_DEBUG(
                "[ReplicaLoop] handle PreVote, replica_id:{}, from:{}, term:{}, last_log_index:{}, "
                "last_log_term:{}",
                replica_id_.to_string(), call.param.from_replica_id.to_string(), call.param.term,
                call.param.last_log_index, call.param.last_log_term);
        raft_core_.handle_pre_vote(call.param, call.result, effects);
        LOG_DEBUG(
                "[ReplicaLoop] handle PreVote finished, replica_id:{}, from:{}, request_term:{}, "
                "response_term:{}, vote_granted:{}",
                replica_id_.to_string(), call.param.from_replica_id.to_string(), call.param.term,
                call.result.term, call.result.vote_granted);
        return Status::OK();
    });
}

Status ReplicaLoop::handle_call(AppendEntriesCall& call) {
    ADVISKV_METRICS_TIMER("storage_replica_append_entries_raft_step_execute");
    Status status = raft_effect_executor_.execute([this, &call](RaftEffects& effects) {
        raft_core_.handle_append_entries(call.param, call.result, effects);
        return Status::OK();
    });
    if (status.fail()) {
        return status;
    }
    return apply_committed_entries_inline();
}

Status ReplicaLoop::handle_call(InstallSnapshotCall& call) {
    return snapshot_tracker_.on_install_snapshot_chunk(call.param);
}

Status ReplicaLoop::handle_call(RecoverCall& call) {
    PersistEngine::RecoverResult result;
    Status recover_status = persist_.recover(result);
    if (recover_status.fail()) {
        LOG_WARN("[ReplicaLoop] persist recover failed, replica_id:{}, status:{}", replica_id_.to_string(),
                 recover_status.to_string());
        return recover_status;
    }

    if (result.snapshot) {
        std::unique_ptr<SnapshotFileReader> reader;
        bool exists = false;
        RETURN_IF_INVALID_STATUS(fault_reporter_.fail_if(persist_.snapshot().open_reader(reader, exists)))
        if (exists && reader) {
            RETURN_IF_INVALID_STATUS(fault_reporter_.fail_if(state_machine_.restore(
                    result.snapshot,
                    [&reader](const KvVisitor& visitor) -> Status { return reader->for_each_kv(visitor); },
                    [&reader](const RequestVisitor& visitor) -> Status {
                        return reader->for_each_request(visitor);
                    })))
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

    call.need_recover = result.need_recover;
    return Status::OK();
}

Status ReplicaLoop::handle_call(AddMemberCall& call) {
    Status status = raft_effect_executor_.execute([this, &call](RaftEffects& effects) {
        return raft_core_.ensure_add_learner(call.member, effects);
    });
    if (status.fail()) {
        return status;
    }
    Status apply_status = apply_committed_entries_inline();
    if (apply_status.fail()) {
        return apply_status;
    }
    return status;
}

Status ReplicaLoop::handle_call(RemoveMemberCall& call) {
    Status status = raft_effect_executor_.execute([this, &call](RaftEffects& effects) {
        return raft_core_.ensure_remove_member(call.replica_id, effects);
    });
    if (status.fail()) {
        return status;
    }
    Status apply_status = apply_committed_entries_inline();
    if (apply_status.fail()) {
        return apply_status;
    }
    return status;
}

}  // namespace adviskv::storage
