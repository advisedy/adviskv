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

namespace adviskv::storage {

/*
大概说一下，这里，Event代表的意思就是我们异步去提交一些事件，我们并不在乎它的这个
返回值什么时候到来。而这个Call的话，代表的就是同步去做一些事情。然后里面会有一些
字段是用于那个就是输出，就代表是我们调用之后，然后里面那些字段就可用了。
*/

struct TickEvent {};

struct AppendResponseEvent {
    ReplicaID from;
    AppendEntriesParam sent_param;
    AppendEntriesResult result;
};

struct VoteResponseEvent {
    ReplicaID from;
    RequestVoteResult result;
};

struct SnapshotResponseEvent {
    ReplicaID from;
    InstallSnapshotParam sent_param;
    InstallSnapshotResult result;
};

struct AppendSendFailedEvent {
    ReplicaID from;
    AppendEntriesParam sent_param;
    Status status;
};

struct SnapshotSendFailedEvent {
    ReplicaID from;
    InstallSnapshotParam sent_param;
    Status status;
};

struct PublishSnapshotEvent {
    LogIndex snapshot_index;
    Term snapshot_term;
    std::vector<RaftMember> members;
};

using Event = std::variant<TickEvent, AppendResponseEvent, VoteResponseEvent, SnapshotResponseEvent,
                           AppendSendFailedEvent, SnapshotSendFailedEvent, PublishSnapshotEvent>;

struct RequestVoteCall {
    RequestVoteParam param;
    RequestVoteResult result{};
    Status status{Status::OK()};
};

struct AppendEntriesCall {
    AppendEntriesParam param;
    AppendEntriesResult result{};
    Status status{Status::OK()};
};

struct BuildReadIndexCall {
    RaftEffects effects;
    LogIndex read_index{0};
    Term read_term{0};
    Status status{Status::OK()};
};

struct PrepareInstallSnapshotCall {
    InstallSnapshotParam param;
    Status status{Status::OK()};
};

struct CommitInstallSnapshotCall {
    InstallSnapshotContext context;
    Status status{Status::OK()};
};

struct AppendResponseCall {
    AppendResponseEvent event;
    Status status{Status::OK()};
};

struct SnapshotResponseCall {
    SnapshotResponseEvent event;
    Status status{Status::OK()};
};

struct SnapshotSendFailedCall {
    SnapshotSendFailedEvent event;
    Status status{Status::OK()};
};

struct ProposeCall {
    ProposeParam param;
    Milliseconds timeout{0};
    Status status{Status::OK()};
};

struct AddMemberCall {
    PeerMember member;
    Status status{Status::OK()};
};

struct RemoveMemberCall {
    ReplicaID replica_id;
    Status status{Status::OK()};
};

using Call = std::variant<RequestVoteCall*, AppendEntriesCall*, BuildReadIndexCall*, PrepareInstallSnapshotCall*,
                          CommitInstallSnapshotCall*, AppendResponseCall*, SnapshotResponseCall*,
                          SnapshotSendFailedCall*, ProposeCall*, AddMemberCall*, RemoveMemberCall*>;

struct Waiter {
    std::mutex mutex;
    std::condition_variable cv;
    bool done{false};
    Status status{Status::OK()};
};

struct TimeoutWaiter {
    std::mutex mutex;
    std::condition_variable cv;
    bool done{false};
    bool cancelled{false};
    LogIndex index{0};
    Status status{Status::OK()};
};

struct ProposalBatchItem {
    ProposeParam param;
    std::shared_ptr<TimeoutWaiter> waiter;
};

struct ConfigChangeItem {
    std::function<Status(RaftEffects&)> step;
    std::shared_ptr<Waiter> waiter;
};

using SubmitQueueItem = std::variant<ProposalBatchItem, ConfigChangeItem>;

class ReplicaLoop {
public:
    using RaftStepFunc = std::function<Status(RaftEffects&)>;
    using TaskFunc = std::function<Status()>;

    explicit ReplicaLoop(ReplicaContext& context);
    ~ReplicaLoop();

    DISALLOW_COPY_AND_ASSIGN(ReplicaLoop)

    void start();
    void stop();

    // Event不等待结果
    void async_submit(Event event);
    // Call会回填结果
    void sync_submit(Call call);

private:
    using QueueTask = SerialTaskRunner::Task;

    // 同步进入 loop。
    Status call_in_loop_task(TaskFunc&& task);

    Status handle_event(const TickEvent& event);
    Status handle_event(const AppendResponseEvent& event);
    Status handle_event(const VoteResponseEvent& event);
    Status handle_event(const SnapshotResponseEvent& event);
    Status handle_event(const AppendSendFailedEvent& event);
    Status handle_event(const SnapshotSendFailedEvent& event);
    Status handle_event(const PublishSnapshotEvent& event);

    // 关于handle_call的返回值最终会回落到call->status里
    Status handle_call(RequestVoteCall& call);
    Status handle_call(AppendEntriesCall& call);
    Status handle_call(BuildReadIndexCall& call);
    Status handle_call(PrepareInstallSnapshotCall& call);
    Status handle_call(CommitInstallSnapshotCall& call);
    Status handle_call(AppendResponseCall& call);
    Status handle_call(SnapshotResponseCall& call);
    Status handle_call(SnapshotSendFailedCall& call);
    Status handle_call(ProposeCall& call);
    Status handle_call(AddMemberCall& call);
    Status handle_call(RemoveMemberCall& call);

    Status run_step(RaftStepFunc&& step);
    Status persist_raft_effects(const RaftEffects& effects);

    void resolve_pending_proposals();
    void fail_all_pending_proposals(const Status& status);

    Status enqueue_proposal_and_wait(ProposeParam param, Milliseconds timeout);
    Status enqueue_config_change_and_wait(RaftStepFunc&& step);
    Status propose_batch_from_queue(std::vector<ProposalBatchItem>& items);
    void drain_submit_queue(std::vector<SubmitQueueItem> items);
    void on_submit_queue_ready(std::vector<SubmitQueueItem> items);

    static void complete_loop_submit(const std::shared_ptr<Waiter>& waiter, const Status& status);
    static bool complete_proposal_commit(const std::shared_ptr<TimeoutWaiter>& waiter, const Status& status);
    static void fail_submit_queue_item(SubmitQueueItem& item, const Status& status);
    static void fail_submit_queue_items(std::vector<SubmitQueueItem>& items, size_t start_idx, const Status& status);

    bool enqueue(QueueTask&& task);

    ReplicaContext& context_;
    SerialTaskRunner runner_;

    std::mutex mutex_;  // running_
    BatchDispatchQueue<SubmitQueueItem> submit_queue_;
    bool running_{false};

    std::multimap<LogIndex, std::shared_ptr<TimeoutWaiter>> pending_proposals_;
};

}  // namespace adviskv::storage
