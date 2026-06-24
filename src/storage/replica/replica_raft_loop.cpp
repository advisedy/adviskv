#include "storage/replica/replica_raft_loop.h"

#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "common/define.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "common/status.h"
#include "storage/model/param.h"
#include "storage/raft/raft_node.h"
#include "storage/replica/replica_raft_effect_runner.h"

namespace adviskv::storage {

namespace {

constexpr size_t kMaxProposalBatchSize = 64;
constexpr auto kProposalBatchCoalesceDelay = Microseconds(200);
constexpr size_t kReplicaRaftRpcWorkerCount = 4;

}  // namespace

ProposalQueue::~ProposalQueue() { IGNORE_RESULT(stop()); }

void ProposalQueue::start(std::chrono::steady_clock::duration coalesce_delay,
                          std::size_t max_batch_size,
                          DispatchCallback on_dispatch) {
    std::lock_guard lock(mutex_);
    state = State::NORMAL;

    coalesce_delay_ = coalesce_delay;
    max_batch_size_ = max_batch_size;
    on_dispatch_ = std::move(on_dispatch);
    timer_thread_ = std::thread(&ProposalQueue::timer_loop, this);
}

std::deque<ProposalBatchItem> ProposalQueue::stop() {
    std::deque<ProposalBatchItem> items;
    {
        std::lock_guard lock(mutex_);
        state = State::STOPED;
        items.swap(items_);
    }
    cv_.notify_all();

    if (timer_thread_.joinable()) {
        timer_thread_.join();
    }
    return items;
}

bool ProposalQueue::push(ProposalBatchItem item) {
    std::lock_guard lock(mutex_);
    if (state == State::STOPED) {
        return false;
    }
    items_.push_back(std::move(item));

    if (state == State::WAITING_TIMER) {
        if (enable_enter_dispatch()) {
            enter_dispatch_unlocked();
        }
        return true;
    }

    if (state == State::NORMAL) {
        schedule_timer_unlocked();
    }
    return true;
}
bool ProposalQueue::enable_enter_dispatch() const {
    return items_.size() >= max_batch_size_;
}

void ProposalQueue::enter_dispatch_unlocked() {
    if (state != State::WAITING_TIMER) {
        LOG_WARN(
            "[ProposalQueue] enter_dispatch_unlocked: state != "
            "State::WAITING_TIMER, state:{}",
            to<int8>(state))
        return;
    }
    std::vector<ProposalBatchItem> batch;
    while (!items_.empty()) {
        batch.emplace_back(std::move(items_.front()));
        items_.pop_front();
    }

    state = State::NORMAL;
    cv_.notify_one();

    on_dispatch_(std::move(batch));
}

void ProposalQueue::schedule_timer_unlocked() {
    if (state == State::STOPED) {
        return;
    }
    state = State::WAITING_TIMER;
    timer_deadline_ = std::chrono::steady_clock::now() + coalesce_delay_;
    cv_.notify_one();
}

void ProposalQueue::timer_loop() {
    while (true) {
        std::unique_lock lock(mutex_);
        cv_.wait(lock, [this] {
            // 如果是WAITNG_TIMER的话，就可以到下面开始触发定时
            return state == State::STOPED || state == State::WAITING_TIMER;
        });
        if (state == State::STOPED) {
            break;
        }

        cv_.wait_until(lock, timer_deadline_, [this] {
            // 如果不是WAITING_TIMER，变成了NORMAL的话，就代表没到点，但
            // 是超过max_batch_size了，所以就不用enter_dispatch了
            return state == State::STOPED || state == State::NORMAL;
        });
        if (state == State::STOPED) break;
        if (state == State::NORMAL) continue;
        enter_dispatch_unlocked();
    }
}

ReplicaRaftLoop::ReplicaRaftLoop(ReplicaContext& context,
                                 int32 raft_rpc_timeout_ms)
    : context_(context), effect_runner_(context, raft_rpc_timeout_ms) {
    effect_runner_.set_response_handlers(
        ReplicaRaftEffectRunner::ResponseHandlers{
            [this](const ReplicaID& from, const RequestVoteResult& result) {
                on_request_vote_response_from_rpc_worker(from, result);
            },
            [this](const ReplicaID& from, const AppendEntriesParam& sent_param,
                   const AppendEntriesResult& result) {
                on_append_response_from_rpc_worker(from, sent_param, result);
            },
            [this](const ReplicaID& from, const AppendEntriesParam& sent_param,
                   const Status& status) {
                on_append_send_failed_from_rpc_worker(from, sent_param, status);
            },
            [this](const ReplicaID& from,
                   const InstallSnapshotParam& sent_param,
                   const InstallSnapshotResult& result) {
                on_install_snapshot_response_from_rpc_worker(from, sent_param,
                                                             result);
            },
            [this](const ReplicaID& from,
                   const InstallSnapshotParam& sent_param,
                   const Status& status) {
                on_install_snapshot_send_failed_from_rpc_worker(
                    from, sent_param, status);
            },
        });
}

ReplicaRaftLoop::~ReplicaRaftLoop() { stop(); }

void ReplicaRaftLoop::start() {
    std::lock_guard lock(mutex_);
    if (running_) {
        return;
    }
    running_ = true;
    effect_runner_.start_rpc_workers(kReplicaRaftRpcWorkerCount);
    runner_.start();
    proposals_.start(kProposalBatchCoalesceDelay, kMaxProposalBatchSize,
                     [this](std::vector<ProposalBatchItem> items) {
                         on_proposal_batch_ready(std::move(items));
                     });
}

void ReplicaRaftLoop::stop() {
    effect_runner_.stop_rpc_workers();

    std::deque<ProposalBatchItem> queued_proposals;
    {
        std::lock_guard lock(mutex_);
        running_ = false;
    }
    queued_proposals = proposals_.stop();
    runner_.stop();

    Status stopped = Status::ERROR("raft loop is stopped");
    for (const auto& item : queued_proposals) {
        complete_proposal(item.waiter, stopped);
    }
    fail_all_pending_proposals(stopped);
}

Status ReplicaRaftLoop::sync_submit_task(TaskFunc&& task) {
    if (!task) {
        return Status::INVALID_ARGUMENT("empty raft loop task");
    }

    if (runner_.runs_in_current_thread()) {  // 如果是RaftLoop内部调用的话
        return task();
    }

    auto waiter = std::make_shared<SyncWaiter>();
    if (!enqueue([task = std::move(task), waiter] {
            complete_sync(waiter, task());
        })) {
        return Status::ERROR("raft loop is not running");
    }

    std::unique_lock lock(waiter->mutex);
    waiter->cv.wait(lock, [&] { return waiter->done; });
    return waiter->status;
}

void ReplicaRaftLoop::async_submit_task(TaskFunc&& task) {
    if (!task) {
        return;
    }

    if (!enqueue([task = std::move(task)]() mutable {
            Status status = task();
            if (status.fail()) {
                LOG_WARN("async raft task failed, status:{}",
                         status.to_string());
            }
        })) {
        LOG_WARN("drop async raft task because raft loop is not running");
    }
}

Status ReplicaRaftLoop::sync_submit_step(RaftStepFunc&& step) {
    if (!step) {
        return Status::INVALID_ARGUMENT("empty raft step");
    }

    if (runner_.runs_in_current_thread()) {
        Status status = run_step(std::move(step));
        complete_committed_proposals();
        return status;
    }

    auto waiter = std::make_shared<SyncWaiter>();
    if (!enqueue([this, step = std::move(step), waiter]() mutable {
            Status status = run_step(std::move(step));
            complete_committed_proposals();
            complete_sync(waiter, status);
        })) {
        return Status::ERROR("raft loop is not running");
    }

    std::unique_lock lock(waiter->mutex);
    waiter->cv.wait(lock, [&] { return waiter->done; });
    return waiter->status;
}

void ReplicaRaftLoop::async_submit_step(RaftStepFunc&& step) {
    if (!step) {
        return;
    }

    if (!enqueue([this, step = std::move(step)]() mutable {
            Status status = run_step(std::move(step));
            if (status.fail()) {
                LOG_WARN("async raft step failed, status:{}",
                         status.to_string());
            }
            complete_committed_proposals();
        })) {
        LOG_WARN("drop async raft step because raft loop is not running");
    }
}

Status ReplicaRaftLoop::propose_and_wait(ProposeParam param,
                                         Milliseconds timeout) {
    auto waiter = std::make_shared<ProposalWaiter>();

    {
        std::lock_guard lock(mutex_);
        if (!running_) {
            return Status::ERROR("raft loop is not running");
        }
    }

    if (!proposals_.push(ProposalBatchItem{std::move(param), waiter})) {
        return Status::ERROR("raft loop is not running");
    }

    std::unique_lock lock(waiter->mutex);
    if (!waiter->cv.wait_for(lock, timeout, [&] { return waiter->done; })) {
        waiter->cancelled = true;
        return Status::NOT_YET_COMMIT(
            "proposal is not committed before timeout");
    }

    return waiter->status;
}

///////////
// ReadIndex Checker
///////////
Status ReplicaRaftLoop::sync_handle_append_response(
    const ReplicaID& from, const AppendEntriesParam& sent_param,
    const AppendEntriesResult& result) {
    return sync_submit_step(
        [this, from, sent_param, result](RaftEffects& effects) {
            IGNORE_RESULT(context_.raft_node.handle_append_response(
                from, sent_param, result, effects));
            return Status::OK();
        });
}

Status ReplicaRaftLoop::sync_handle_install_snapshot_response(
    const ReplicaID& from, const InstallSnapshotParam& sent_param,
    const InstallSnapshotResult& result) {
    return sync_submit_step(
        [this, from, sent_param, result](RaftEffects& effects) {
            context_.raft_node.handle_install_snapshot_response(
                from, sent_param, result, effects);
            return Status::OK();
        });
}

Status ReplicaRaftLoop::sync_handle_install_snapshot_send_failed(
    const ReplicaID& from, const InstallSnapshotParam& sent_param,
    const Status& status) {
    return sync_submit_task([this, from, sent_param, status] {
        context_.raft_node.handle_install_snapshot_send_failed(from, sent_param,
                                                               status);
        complete_committed_proposals();
        return Status::OK();
    });
}

Status ReplicaRaftLoop::sync_send_append_entries(
    const PeerMember& member, const AppendEntriesParam& param,
    AppendEntriesResult& result) {
    return effect_runner_.sync_send_append_entries(member, param, result);
}

Status ReplicaRaftLoop::sync_send_install_snapshot(
    const PeerMember& member, const InstallSnapshotParam& param,
    InstallSnapshotResult& result) {
    return effect_runner_.sync_send_install_snapshot(member, param, result);
}

///////////
// RPC worker
///////////

void ReplicaRaftLoop::on_request_vote_response_from_rpc_worker(
    const ReplicaID& from, const RequestVoteResult& result) {
    async_submit_step([this, from, result](RaftEffects& effects) {
        context_.raft_node.handle_vote_response(from, result, effects);
        return Status::OK();
    });
}

void ReplicaRaftLoop::on_append_response_from_rpc_worker(
    const ReplicaID& from, const AppendEntriesParam& sent_param,
    const AppendEntriesResult& result) {
    async_submit_step([this, from, sent_param, result](RaftEffects& effects) {
        IGNORE_RESULT(context_.raft_node.handle_append_response(
            from, sent_param, result, effects));
        return Status::OK();
    });
}

void ReplicaRaftLoop::on_append_send_failed_from_rpc_worker(
    const ReplicaID& from, const AppendEntriesParam& sent_param,
    const Status& status) {
    async_submit_task([this, from, sent_param, status] {
        context_.raft_node.handle_append_send_failed(from, sent_param, status);
        complete_committed_proposals();
        return Status::OK();
    });
}

void ReplicaRaftLoop::on_install_snapshot_response_from_rpc_worker(
    const ReplicaID& from, const InstallSnapshotParam& sent_param,
    const InstallSnapshotResult& result) {
    async_submit_step([this, from, sent_param, result](RaftEffects& effects) {
        context_.raft_node.handle_install_snapshot_response(from, sent_param,
                                                            result, effects);
        return Status::OK();
    });
}

void ReplicaRaftLoop::on_install_snapshot_send_failed_from_rpc_worker(
    const ReplicaID& from, const InstallSnapshotParam& sent_param,
    const Status& status) {
    async_submit_task([this, from, sent_param, status] {
        context_.raft_node.handle_install_snapshot_send_failed(from, sent_param,
                                                               status);
        complete_committed_proposals();
        return Status::OK();
    });
}

void ReplicaRaftLoop::async_submit_tick() {
    async_submit_step([this](RaftEffects& effects) {
        context_.raft_node.tick(effects);
        return Status::OK();
    });
}

bool ReplicaRaftLoop::enqueue(QueueTask&& task) {
    {
        std::lock_guard lock(mutex_);
        if (!running_) {
            return false;
        }
    }
    return runner_.submit(std::move(task));
}

Status ReplicaRaftLoop::run_step(RaftStepFunc&& step) {
    ADVISKV_METRICS_TIMER("storage_replica_raft_loop_step");
    ADVISKV_METRICS_COUNTER("storage_replica_raft_loop_step_request");
    Status status = effect_runner_.run_raft_step(std::move(step));
    if (status.ok()) {
        ADVISKV_METRICS_COUNTER("storage_replica_raft_loop_step_success");
    } else {
        ADVISKV_METRICS_COUNTER("storage_replica_raft_loop_step_failure");
    }
    return status;
}

void ReplicaRaftLoop::on_proposal_batch_ready(
    std::vector<ProposalBatchItem> items) {
    if (items.empty()) {
        return;
    }
    if (!runner_.submit([this, items = std::move(items)]() mutable {
            drain_proposals(std::move(items));
        })) {
        Status stopped = Status::ERROR("raft loop is stopped");
        for (auto& item : items) {
            complete_proposal(item.waiter, stopped);
        }
    }
}

void ReplicaRaftLoop::drain_proposals(std::vector<ProposalBatchItem> items) {
    bool stopped = false;
    {
        std::lock_guard lock(mutex_);
        stopped = !running_;
    }
    if (stopped) {
        Status status = Status::ERROR("raft loop is stopped");
        for (auto& item : items) {
            complete_proposal(item.waiter, status);
        }
        return;
    }

    std::vector<ProposalBatchItem> active_items;
    std::vector<ProposeParam> params;
    active_items.reserve(items.size());
    params.reserve(items.size());
    for (ProposalBatchItem& item : items) {
        params.push_back(item.param);
        active_items.push_back(std::move(item));
    }

    if (active_items.empty()) {
        complete_committed_proposals();
        return;
    }

    std::vector<std::pair<Status, LogIndex>> results;
    Status step_status = run_step([&](RaftEffects& effects) {
        return context_.raft_node.propose_batch(params, results, effects);
    });

    if (step_status.fail()) {
        for (auto& item : active_items) {
            complete_proposal(item.waiter, step_status);
        }
        complete_committed_proposals();
        return;
    }

    if (results.size() != active_items.size()) {
        Status status = Status::ERROR("proposal batch result size mismatch");
        for (auto& item : active_items) {
            IGNORE_RESULT(complete_proposal(item.waiter, status))
        }
        complete_committed_proposals();
        return;
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
            complete_proposal(item.waiter, status);
        } else if (index <= 0) {
            complete_proposal(item.waiter,
                              Status::ERROR("invalid proposed log index"));
        } else if (context_.raft_node.commit_index() >= index) {
            complete_proposal(item.waiter, Status::OK());
        } else {
            pending_proposals_.emplace(index, item.waiter);
        }
    }

    complete_committed_proposals();
}

void ReplicaRaftLoop::complete_sync(const std::shared_ptr<SyncWaiter>& waiter,
                                    const Status& status) {
    {
        std::lock_guard lock(waiter->mutex);
        waiter->status = status;
        waiter->done = true;
    }
    waiter->cv.notify_one();
}

// 把一个操作设置status，然后给结束
bool ReplicaRaftLoop::complete_proposal(
    const std::shared_ptr<ProposalWaiter>& waiter, const Status& status) {
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

// 把pending_commit队列里面已经commit的写操作给结束掉
void ReplicaRaftLoop::complete_committed_proposals() {
    LogIndex commit_index = context_.raft_node.commit_index();
    for (auto it = pending_proposals_.begin();
         it != pending_proposals_.end();) {
        LogIndex log_index = it->first;
        std::shared_ptr<ProposalWaiter>& waiter = it->second;

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
        complete_proposal(waiter, Status::OK());
        it = pending_proposals_.erase(it);
    }

    if (!context_.raft_node.is_leader()) {
        fail_all_pending_proposals(
            Status::NOT_LEADER("leader changed before proposal committed"));
    }
}

void ReplicaRaftLoop::fail_all_pending_proposals(const Status& status) {
    for (auto& [_, waiter] : pending_proposals_) {
        complete_proposal(waiter, status);
    }
    pending_proposals_.clear();
}

}  // namespace adviskv::storage
