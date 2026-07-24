#include "storage/replica/replica_loop.h"

#include <chrono>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/crash_injection.h"
#include "common/define.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "common/status.h"
#include "storage/model/param.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/core/raft_core.h"
#include "storage/replica/replica_message_dispatcher.h"

namespace adviskv::storage {

namespace {

constexpr size_t K_MAX_PROPOSAL_BATCH_SIZE = 64;
constexpr auto K_PROPOSAL_BATCH_DELAY = Microseconds(200);

}  // namespace

ReplicaLoop::ReplicaLoop(ReplicaContext& context) : context_(context) {}

ReplicaLoop::~ReplicaLoop() { stop(); }

void ReplicaLoop::start() {
    std::lock_guard lock(mutex_);
    if (running_) {
        return;
    }
    running_ = true;
    runner_.start();
    submit_queue_.start(K_PROPOSAL_BATCH_DELAY, K_MAX_PROPOSAL_BATCH_SIZE,
                        [this](std::vector<SubmitQueueItem> items) { on_submit_queue_ready(std::move(items)); });
}

void ReplicaLoop::stop() {
    std::deque<SubmitQueueItem> queued_submissions;
    {
        std::lock_guard lock(mutex_);
        running_ = false;
    }
    queued_submissions = submit_queue_.stop();
    runner_.stop();

    Status stopped = Status::ERROR("raft loop is stopped");
    for (auto& item : queued_submissions) {
        fail_submit_queue_item(item, stopped);
    }
    fail_all_pending_proposals(stopped);
}

void ReplicaLoop::async_submit(Event event) {
    bool res = std::visit(
            [this](auto concrete_event) -> bool {
                return enqueue([this, event = std::move(concrete_event)]() {
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

void ReplicaLoop::sync_submit(Call call) {
    std::visit(
            [this](auto* concrete_call) {
                if (concrete_call == nullptr) {
                    LOG_WARN("drop null raft loop call");
                    return;
                }

                using T = std::remove_pointer_t<std::decay_t<decltype(concrete_call)>>;
                if constexpr (std::is_same_v<T, ProposeCall> || std::is_same_v<T, AddMemberCall> ||
                              std::is_same_v<T, RemoveMemberCall>) {
                    concrete_call->status = handle_call(*concrete_call);
                } else {
                    // 创建waiter，把这个task给enqueue
                    Status status = call_in_loop_task([this, concrete_call]() {
                        concrete_call->status = handle_call(*concrete_call);
                        return Status::OK();
                    });
                    if (status.fail()) {
                        concrete_call->status = status;
                    }
                }
            },
            call);
}

Status ReplicaLoop::call_in_loop_task(TaskFunc&& task) {
    if (!task) {
        return Status::INVALID_ARGUMENT("empty raft loop task");
    }

    auto waiter = std::make_shared<Waiter>();
    bool res = enqueue([task = std::move(task), waiter]() {
        Status status = task();
        complete_loop_submit(waiter, status);
    });

    if (!res) {
        return Status::ERROR("raft loop is not running");
    }

    std::unique_lock lock(waiter->mutex);
    waiter->cv.wait(lock, [&]() { return waiter->done; });
    return waiter->status;
}

Status ReplicaLoop::enqueue_config_change_and_wait(RaftStepFunc&& step) {
    if (!step) {
        return Status::INVALID_ARGUMENT("empty config change step");
    }

    auto waiter = std::make_shared<Waiter>();
    {
        std::lock_guard lock(mutex_);
        if (!running_) {
            return Status::ERROR("raft loop is not running");
        }
    }
    if (!submit_queue_.push(ConfigChangeItem{std::move(step), waiter})) {
        return Status::ERROR("raft loop is not running");
    }
    std::unique_lock lock(waiter->mutex);
    waiter->cv.wait(lock, [&]() { return waiter->done; });
    return waiter->status;
}

Status ReplicaLoop::enqueue_proposal_and_wait(ProposeParam param, Milliseconds timeout) {
    auto waiter = std::make_shared<TimeoutWaiter>();

    {
        std::lock_guard lock(mutex_);
        if (!running_) {
            return Status::ERROR("raft loop is not running");
        }
    }

    if (!submit_queue_.push(ProposalBatchItem{std::move(param), waiter})) {
        return Status::ERROR("raft loop is not running");
    }

    std::unique_lock lock(waiter->mutex);
    if (!waiter->cv.wait_for(lock, timeout, [&]() { return waiter->done; })) {
        waiter->cancelled = true;
        return Status::NOT_YET_COMMIT("proposal is not committed before timeout");
    }

    return waiter->status;
}

Status ReplicaLoop::handle_event(const TickEvent& event) {
    UNUSED(event);
    Status status = run_step([this](RaftEffects& effects) {
        context_.raft_core.tick(effects);
        return Status::OK();
    });
    resolve_pending_proposals();
    return status;
}

Status ReplicaLoop::handle_event(const AppendResponseEvent& event) {
    Status status = run_step([this, &event](RaftEffects& effects) {
        return context_.raft_core.handle_append_response(event.from, event.sent_param, event.result, effects);
    });
    resolve_pending_proposals();
    return status;
}

Status ReplicaLoop::handle_event(const VoteResponseEvent& event) {
    Status status = run_step([this, &event](RaftEffects& effects) {
        context_.raft_core.handle_vote_response(event.from, event.result, effects);
        return Status::OK();
    });
    resolve_pending_proposals();
    return status;
}

Status ReplicaLoop::handle_event(const SnapshotResponseEvent& event) {
    Status status = run_step([this, &event](RaftEffects& effects) {
        context_.raft_core.handle_install_snapshot_response(event.from, event.sent_param, event.result, effects);
        return Status::OK();
    });
    resolve_pending_proposals();
    return status;
}

Status ReplicaLoop::handle_event(const AppendSendFailedEvent& event) {
    Status status = run_step([this, &event](RaftEffects&) {
        context_.raft_core.handle_append_send_failed(event.from, event.sent_param, event.status);
        return Status::OK();
    });
    resolve_pending_proposals();
    return status;
}

Status ReplicaLoop::handle_event(const SnapshotSendFailedEvent& event) {
    Status status = run_step([this, &event](RaftEffects&) {
        context_.raft_core.handle_install_snapshot_send_failed(event.from, event.sent_param, event.status);
        return Status::OK();
    });
    resolve_pending_proposals();
    return status;
}

Status ReplicaLoop::handle_event(const PublishSnapshotEvent& event) {
    {
        std::lock_guard lock(context_.raft_core_mutex);
        if (event.snapshot_index <= context_.raft_core.snapshot_index()) {
            return Status::OK();
        }
        Status status = context_.raft_core.truncate_log(event.snapshot_index);
        if (status.fail()) {
            LOG_WARN(
                    "[ReplicaLoop] handle publish snapshot failed, "
                    "snapshot_index:{}, status:{}",
                    event.snapshot_index, status.to_string());
            context_.enter_faulted();
            return status;
        }
    }
    resolve_pending_proposals();
    return Status::OK();
}

Status ReplicaLoop::handle_call(RequestVoteCall& call) {
    return run_step([this, &call](RaftEffects& effects) {
        context_.raft_core.handle_request_vote(call.param, call.result, effects);
        return Status::OK();
    });
}

Status ReplicaLoop::handle_call(AppendEntriesCall& call) {
    return run_step([this, &call](RaftEffects& effects) {
        context_.raft_core.handle_append_entries(call.param, call.result, effects);
        return Status::OK();
    });
}

Status ReplicaLoop::handle_call(BuildReadIndexCall& call) {
    std::lock_guard lock(context_.raft_core_mutex);
    return context_.raft_core.build_append_entries_for_read(call.effects, call.read_index, call.read_term);
}

Status ReplicaLoop::handle_call(PrepareInstallSnapshotCall& call) {
    return run_step([this, &call](RaftEffects& effects) {
        return context_.raft_core.prepare_install_snapshot(call.param, effects);
    });
}

Status ReplicaLoop::handle_call(CommitInstallSnapshotCall& call) {
    return run_step([this, &call](RaftEffects& effects) {
        context_.raft_core.commit_install_snapshot(call.context, effects);
        return Status::OK();
    });
}

Status ReplicaLoop::handle_call(AppendResponseCall& call) { return handle_event(call.event); }

Status ReplicaLoop::handle_call(SnapshotResponseCall& call) { return handle_event(call.event); }

Status ReplicaLoop::handle_call(SnapshotSendFailedCall& call) { return handle_event(call.event); }

Status ReplicaLoop::handle_call(ProposeCall& call) {
    return enqueue_proposal_and_wait(std::move(call.param), call.timeout);
}

Status ReplicaLoop::handle_call(AddMemberCall& call) {
    return enqueue_config_change_and_wait([this, member = call.member](RaftEffects& effects) {
        return context_.raft_core.ensure_add_learner(member, effects);
    });
}

Status ReplicaLoop::handle_call(RemoveMemberCall& call) {
    return enqueue_config_change_and_wait([this, replica_id = call.replica_id](RaftEffects& effects) {
        return context_.raft_core.ensure_remove_member(replica_id, effects);
    });
}

Status ReplicaLoop::run_step(RaftStepFunc&& step) {
    ADVISKV_METRICS_TIMER("storage_replica_raft_loop_step");
    ADVISKV_METRICS_COUNTER("storage_replica_raft_loop_step_request");

    RaftEffects effects;
    {
        std::lock_guard lock(context_.raft_core_mutex);
        Status status = step(effects);
        bool has_persistent_effect = effects.hard_state.has_value() || effects.entries_to_rewrite.has_value() ||
                                           !effects.entries_to_append.empty();
        RETURN_IF_INVALID_STATUS(persist_raft_effects(effects))
        if (has_persistent_effect) {
            testhook::crash_point("replica.raft_step.after_persist_before_send");
        }
        RETURN_IF_INVALID_STATUS(status)
    }
    Status send_status = context_.message_dispatcher.async_send(std::move(effects.messages));
    if (send_status.fail()) {
        ADVISKV_METRICS_COUNTER("storage_replica_raft_loop_step_failure");
        return send_status;
    }
    ADVISKV_METRICS_COUNTER("storage_replica_raft_loop_step_success");
    return Status::OK();
}

Status ReplicaLoop::persist_raft_effects(const RaftEffects& effects) {
    if (effects.entries_to_rewrite.has_value() && !effects.entries_to_append.empty()) {
        return Status::INVALID_ARGUMENT("cannot both rewrite and append");
    }

    if (effects.hard_state.has_value()) {
        RETURN_IF_INVALID_STATUS(context_.fault_if_fail(context_.persist.save_raft_meta(*effects.hard_state)))
    }

    if (effects.entries_to_rewrite.has_value()) {
        RETURN_IF_INVALID_STATUS(context_.fault_if_fail(context_.persist.rewrite_wal(*effects.entries_to_rewrite)))
    }

    if (!effects.entries_to_append.empty()) {
        RETURN_IF_INVALID_STATUS(context_.fault_if_fail(context_.persist.append_wal_batch(effects.entries_to_append)))
    }

    return Status::OK();
}
// 只有在runner内部访问，目前并没有并发方面的危险
void ReplicaLoop::resolve_pending_proposals() {
    LogIndex commit_index;
    {
        std::lock_guard lock(context_.raft_core_mutex);
        commit_index = context_.raft_core.commit_index();
    }

    for (auto it = pending_proposals_.begin(); it != pending_proposals_.end();) {
        LogIndex log_index = it->first;
        std::shared_ptr<TimeoutWaiter>& waiter = it->second;

        bool cancelled = false;
        {
            std::lock_guard lock(waiter->mutex);
            cancelled = waiter->cancelled;
        }
        if (cancelled) {
            it = pending_proposals_.erase(it);
            continue;
        }
        if (log_index > commit_index) {
            break;
        }
        complete_proposal_commit(waiter, Status::OK());
        it = pending_proposals_.erase(it);
    }

    bool is_leader;
    {
        std::lock_guard lock(context_.raft_core_mutex);
        is_leader = context_.raft_core.is_leader();
    }
    if (!is_leader) {
        fail_all_pending_proposals(Status::NOT_LEADER("leader changed before proposal committed"));
    }
}

void ReplicaLoop::fail_all_pending_proposals(const Status& status) {
    for (auto& [_, waiter] : pending_proposals_) {
        complete_proposal_commit(waiter, status);
    }
    pending_proposals_.clear();
}

void ReplicaLoop::on_submit_queue_ready(std::vector<SubmitQueueItem> items) {
    if (items.empty()) {
        return;
    }
    if (!enqueue([this, items = std::move(items)]() mutable { drain_submit_queue(std::move(items)); })) {
        Status stopped = Status::ERROR("raft loop is stopped");
        for (auto& item : items) {
            fail_submit_queue_item(item, stopped);
        }
    }
}

void ReplicaLoop::drain_submit_queue(std::vector<SubmitQueueItem> items) {
    bool stopped = false;
    {
        std::lock_guard lock(mutex_);
        stopped = !running_;
    }
    if (stopped) {
        Status status = Status::ERROR("raft loop is stopped");
        for (auto& item : items) {
            fail_submit_queue_item(item, status);
        }
        return;
    }

    std::vector<ProposalBatchItem> pending_write_items;
    pending_write_items.reserve(items.size());
    for (size_t i = 0; i < items.size(); i++) {
        if (auto* write_item = std::get_if<ProposalBatchItem>(&items[i])) {
            pending_write_items.push_back(std::move(*write_item));
            continue;
        }

        if (Status status = propose_batch_from_queue(pending_write_items); status.fail()) {
            fail_submit_queue_items(items, i, status);
            return;
        }

        ConfigChangeItem& config_item = std::get<ConfigChangeItem>(items[i]);
        Status status = run_step(std::move(config_item.step));
        resolve_pending_proposals();
        complete_loop_submit(config_item.waiter, status);
        if (status.fail()) {
            fail_submit_queue_items(items, i + 1, status);
            return;
        }
    }

    IGNORE_RESULT(propose_batch_from_queue(pending_write_items))
}

Status ReplicaLoop::propose_batch_from_queue(std::vector<ProposalBatchItem>& active_items) {
    if (active_items.empty()) {
        return Status::OK();
    }

    std::vector<ProposeParam> params;
    params.reserve(active_items.size());
    for (const ProposalBatchItem& item : active_items) {
        params.push_back(item.param);
    }

    std::vector<std::pair<Status, LogIndex>> results;
    Status step_status = run_step([&](RaftEffects& effects) {
        results = context_.raft_core.propose_batch(params, effects);
        return Status::OK();
    });

    if (step_status.fail()) {
        for (auto& item : active_items) {
            complete_proposal_commit(item.waiter, step_status);
        }
        resolve_pending_proposals();
        active_items.clear();
        return step_status;
    }

    if (results.size() != active_items.size()) {
        Status status = Status::ERROR("proposal batch result size mismatch");
        for (auto& item : active_items) {
            IGNORE_RESULT(complete_proposal_commit(item.waiter, status))
        }
        resolve_pending_proposals();
        active_items.clear();
        return status;
    }

    LogIndex commit_index;
    {
        std::lock_guard lock(context_.raft_core_mutex);
        commit_index = context_.raft_core.commit_index();
    }

    for (size_t i = 0; i < active_items.size(); i++) {
        ProposalBatchItem& item = active_items[i];
        Status status = results[i].first;
        LogIndex index = results[i].second;
        {
            std::lock_guard lock(item.waiter->mutex);
            item.waiter->index = index;
        }

        if (status.fail()) {
            complete_proposal_commit(item.waiter, status);
        } else if (index <= 0) {
            complete_proposal_commit(item.waiter, Status::ERROR("invalid proposed log index"));
        } else if (commit_index >= index) {
            complete_proposal_commit(item.waiter, Status::OK());
        } else {
            pending_proposals_.emplace(index, item.waiter);
        }
    }

    resolve_pending_proposals();
    active_items.clear();
    return Status::OK();
}

void ReplicaLoop::complete_loop_submit(const std::shared_ptr<Waiter>& waiter, const Status& status) {
    {
        std::lock_guard lock(waiter->mutex);
        waiter->status = status;
        waiter->done = true;
    }
    waiter->cv.notify_one();
}

bool ReplicaLoop::complete_proposal_commit(const std::shared_ptr<TimeoutWaiter>& waiter, const Status& status) {
    {
        std::lock_guard lock(waiter->mutex);
        if (waiter->cancelled) {
            return false;
        }
        waiter->status = status;
        waiter->done = true;
    }
    waiter->cv.notify_one();
    return true;
}

void ReplicaLoop::fail_submit_queue_item(SubmitQueueItem& item, const Status& status) {
    if (auto* write_item = std::get_if<ProposalBatchItem>(&item)) {
        IGNORE_RESULT(complete_proposal_commit(write_item->waiter, status))
        return;
    }

    ConfigChangeItem& config_item = std::get<ConfigChangeItem>(item);
    complete_loop_submit(config_item.waiter, status);
}

void ReplicaLoop::fail_submit_queue_items(std::vector<SubmitQueueItem>& items, size_t start_idx, const Status& status) {
    for (size_t i = start_idx; i < items.size(); i++) {
        fail_submit_queue_item(items[i], status);
    }
}

bool ReplicaLoop::enqueue(QueueTask&& task) {
    {
        std::lock_guard lock(mutex_);
        if (!running_) {
            return false;
        }
    }
    return runner_.submit(std::move(task));
}

}  // namespace adviskv::storage
