#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <utility>
#include <variant>
#include <vector>

#include "common/batch_dispatch_queue.h"
#include "common/define.h"
#include "common/serial_task_runner.h"
#include "common/status.h"
#include "common/type.h"
#include "storage/model/param.h"
#include "storage/replica/replica.h"
#include "storage/replica/replica_raft_effect_runner.h"

namespace adviskv::storage {

struct LoopSubmitWaiter {
    std::mutex mutex;
    std::condition_variable cv;
    bool done{false};
    Status status{Status::OK()};
};

struct ProposalCommitWaiter {
    std::mutex mutex;
    std::condition_variable cv;
    bool done{false};
    bool cancelled{false};
    LogIndex index{0};
    Status status{Status::OK()};
};

struct ProposalBatchItem {
    ProposeParam param;
    std::shared_ptr<ProposalCommitWaiter> waiter;
};

using RaftLoopStepFunc = ReplicaRaftEffectRunner::RaftStepFunc;

struct ConfigChangeItem {
    RaftLoopStepFunc step;
    std::shared_ptr<LoopSubmitWaiter> waiter;
};

using SubmitQueueItem = std::variant<ProposalBatchItem, ConfigChangeItem>;

/////////////////
// ReplicaRaftLoop
/////////////////

class ReplicaRaftLoop {
public:
    using RaftStepFunc = RaftLoopStepFunc;
    using TaskFunc = std::function<Status()>;

    ReplicaRaftLoop(ReplicaContext& context, int32 raft_rpc_timeout_ms);
    ~ReplicaRaftLoop();

    DISALLOW_COPY_AND_ASSIGN(ReplicaRaftLoop)

    void start();
    void stop();

    Status sync_submit_task(TaskFunc&& task);
    Status sync_submit_step(RaftStepFunc&& step);

    Status submit_propose_and_wait(ProposeParam param, Milliseconds timeout);
    Status submit_config_change_and_wait(RaftStepFunc&& step);

    // 由 ReplicaReadIndexChecker 直接调用
    Status sync_handle_append_response(const ReplicaID& from, const AppendEntriesParam& sent_param,
                                       const AppendEntriesResult& result);
    Status sync_handle_install_snapshot_response(const ReplicaID& from, const InstallSnapshotParam& sent_param,
                                                 const InstallSnapshotResult& result);
    Status sync_handle_install_snapshot_send_failed(const ReplicaID& from, const InstallSnapshotParam& sent_param,
                                                    const Status& status);
    Status sync_send_append_entries(const PeerMember& member, const AppendEntriesParam& param,
                                    AppendEntriesResult& result);
    Status sync_send_install_snapshot(const PeerMember& member, const InstallSnapshotParam& param,
                                      InstallSnapshotResult& result);

    // Replica::on_tick
    void async_submit_tick();

private:
    using QueueTask = SerialTaskRunner::Task;

    void async_submit_task(TaskFunc&& task);
    void async_submit_step(RaftStepFunc&& step);

    bool enqueue(QueueTask&& task);
    Status run_step(RaftStepFunc&& step);
    Status drain_write_batch(std::vector<ProposalBatchItem>& items);
    void drain_submit_queue(std::vector<SubmitQueueItem> items);
    void on_submit_queue_ready(std::vector<SubmitQueueItem> items);

    ////////////////
    // 给ReplicaRaftEffectRunner做回调用的，rpc_worker那边发送完消息之后，
    // 会对于那些会产生effects的response进行回调，这样可以继续放到RaftLoop这边把操作串行化起来
    void on_request_vote_response_from_rpc_worker(const ReplicaID& from, const RequestVoteResult& result);
    void on_append_response_from_rpc_worker(const ReplicaID& from, const AppendEntriesParam& sent_param,
                                            const AppendEntriesResult& result);
    void on_append_send_failed_from_rpc_worker(const ReplicaID& from, const AppendEntriesParam& sent_param,
                                               const Status& status);
    void on_install_snapshot_response_from_rpc_worker(const ReplicaID& from, const InstallSnapshotParam& sent_param,
                                                      const InstallSnapshotResult& result);
    void on_install_snapshot_send_failed_from_rpc_worker(const ReplicaID& from, const InstallSnapshotParam& sent_param,
                                                         const Status& status);
    ////////////////

    static void complete_loop_submit(const std::shared_ptr<LoopSubmitWaiter>& waiter, const Status& status);
    static bool complete_proposal_commit(const std::shared_ptr<ProposalCommitWaiter>& waiter, const Status& status);
    static void fail_submit_queue_item(SubmitQueueItem& item, const Status& status);
    static void fail_submit_queue_items(std::vector<SubmitQueueItem>& items, size_t start_idx, const Status& status);

    // 负责去检查一下已经propose的写请求，如果
    // commit_index已经推进到了他们的index，就可以释放他们的waiter了
    void resolve_pending_proposals();
    void fail_all_pending_proposals(const Status& status);

    ReplicaContext& context_;
    ReplicaRaftEffectRunner effect_runner_;
    SerialTaskRunner runner_;

    std::mutex mutex_;
    BatchDispatchQueue<SubmitQueueItem> submit_queue_;
    bool running_{false};

    std::multimap<LogIndex, std::shared_ptr<ProposalCommitWaiter>>
            pending_proposals_;  // 对于已经propose的写请求，但是还没有达到commit_index
};

}  // namespace adviskv::storage