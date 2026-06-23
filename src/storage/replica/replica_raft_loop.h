#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "common/define.h"
#include "common/serial_task_runner.h"
#include "common/status.h"
#include "common/type.h"
#include "storage/model/param.h"
#include "storage/replica/replica.h"
#include "storage/replica/replica_raft_effect_runner.h"

namespace adviskv::storage {

struct SyncWaiter {
    std::mutex mutex;
    std::condition_variable cv;
    bool done{false};
    Status status{Status::OK()};
};

struct ProposalWaiter {
    std::mutex mutex;
    std::condition_variable cv;
    bool done{false};
    bool cancelled{false};
    LogIndex index{0};
    Status status{Status::OK()};
};

struct ProposalBatchItem {
    ProposeParam param;
    std::shared_ptr<ProposalWaiter> waiter;
};

/////////////////
// ProposalQueue
/////////////////

class ProposalQueue {
   public:
    using DispatchCallback =
        std::function<void(std::vector<ProposalBatchItem>)>;

    ProposalQueue() = default;
    ~ProposalQueue();

    DISALLOW_COPY_AND_ASSIGN(ProposalQueue)

    void start(std::chrono::steady_clock::duration coalesce_delay,
               std::size_t max_batch_size, DispatchCallback on_dispatch);
    std::deque<ProposalBatchItem> stop();

    bool push(ProposalBatchItem item);

   private:
    void schedule_timer_unlocked();
    void timer_loop();
    void enter_dispatch_unlocked();
    bool enable_enter_dispatch() const;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<ProposalBatchItem> items_;
    DispatchCallback on_dispatch_;
    std::chrono::steady_clock::duration coalesce_delay_{};
    std::chrono::steady_clock::time_point timer_deadline_{};
    std::size_t max_batch_size_{0};

    enum class State : int8 { NORMAL = 0, WAITING_TIMER = 1, STOPED = 2 };
    State state{State::STOPED};
    std::thread timer_thread_;
};

/////////////////
// ReplicaRaftLoop
/////////////////

class ReplicaRaftLoop {
   public:
    using RaftStepFunc = std::function<Status(RaftEffects&)>;
    using TaskFunc = std::function<Status()>;

    ReplicaRaftLoop(ReplicaContext& context, int32 raft_rpc_timeout_ms);
    ~ReplicaRaftLoop();

    DISALLOW_COPY_AND_ASSIGN(ReplicaRaftLoop)

    void start();
    void stop();

    Status sync_submit_task(TaskFunc&& task);
    void async_submit_task(TaskFunc&& task);
    Status sync_submit_step(RaftStepFunc&& step);
    void async_submit_step(RaftStepFunc&& step);

    Status propose_and_wait(ProposeParam param, Milliseconds timeout);

    // ReplicaReadIndexChecker
    Status sync_handle_append_response(const ReplicaID& from,
                                       const AppendEntriesParam& sent_param,
                                       const AppendEntriesResult& result);
    Status sync_handle_install_snapshot_response(
        const ReplicaID& from, const InstallSnapshotParam& sent_param,
        const InstallSnapshotResult& result);
    Status sync_handle_install_snapshot_send_failed(
        const ReplicaID& from, const InstallSnapshotParam& sent_param,
        const Status& status);
    Status sync_send_append_entries(const PeerMember& member,
                                    const AppendEntriesParam& param,
                                    AppendEntriesResult& result);
    Status sync_send_install_snapshot(const PeerMember& member,
                                      const InstallSnapshotParam& param,
                                      InstallSnapshotResult& result);

    // Replica::on_tick
    void async_submit_tick();

   private:
    using QueueTask = SerialTaskRunner::Task;

    bool enqueue(QueueTask&& task);
    Status run_step(RaftStepFunc&& step);
    void drain_proposals(std::vector<ProposalBatchItem> items);
    void on_proposal_batch_ready(std::vector<ProposalBatchItem> items);

    // ReplicaRaftEffectRunner
    void on_request_vote_response_from_rpc_worker(
        const ReplicaID& from, const RequestVoteResult& result);
    void on_append_response_from_rpc_worker(
        const ReplicaID& from, const AppendEntriesParam& sent_param,
        const AppendEntriesResult& result);
    void on_append_send_failed_from_rpc_worker(
        const ReplicaID& from, const AppendEntriesParam& sent_param,
        const Status& status);
    void on_install_snapshot_response_from_rpc_worker(
        const ReplicaID& from, const InstallSnapshotParam& sent_param,
        const InstallSnapshotResult& result);
    void on_install_snapshot_send_failed_from_rpc_worker(
        const ReplicaID& from, const InstallSnapshotParam& sent_param,
        const Status& status);

    static void complete_sync(const std::shared_ptr<SyncWaiter>& waiter,
                              const Status& status);
    static bool complete_proposal(const std::shared_ptr<ProposalWaiter>& waiter,
                                  const Status& status);

    void complete_committed_proposals();
    void fail_all_pending_proposals(const Status& status);

    ReplicaContext& context_;
    ReplicaRaftEffectRunner effect_runner_;
    SerialTaskRunner runner_;

    std::mutex mutex_;
    ProposalQueue proposals_;
    bool running_{false};

    std::multimap<LogIndex, std::shared_ptr<ProposalWaiter>>
        pending_proposals_;  // 对于已经propose的写请求，但是还没有达到commit_index
};

}  // namespace adviskv::storage