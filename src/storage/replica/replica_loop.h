#pragma once

#include <chrono>
#include <cstddef>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "common/define.h"
#include "common/model/type.h"
#include "common/status.h"
#include "storage/model/param.h"
#include "storage/replica/completion_manager.h"
#include "storage/replica/replica_loop_queue.h"
#include "storage/replica/proposal_tracker.h"
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

struct PreVoteResponseEvent {
    ReplicaID from;
    PreVoteResult result;
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

using Event = std::variant<TickEvent, AppendResponseEvent, VoteResponseEvent, PreVoteResponseEvent,
                           SnapshotResponseEvent, AppendSendFailedEvent, SnapshotSendFailedEvent>;

struct RequestVoteCall {
    RequestVoteParam param;
    RequestVoteResult result{};
    Status status{Status::OK()};
};

struct PreVoteCall {
    PreVoteParam param;
    PreVoteResult result{};
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

using Call =
        std::variant<RequestVoteCall*, PreVoteCall*, AppendEntriesCall*, BuildReadIndexCall*,
                     PrepareInstallSnapshotCall*, CommitInstallSnapshotCall*, AppendResponseCall*,
                     SnapshotResponseCall*, SnapshotSendFailedCall*, ProposeCall*, AddMemberCall*, RemoveMemberCall*>;

class ReplicaLoop {
public:
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
    // 普通请求交给工作线程执行，写请求交给队列组批。
    template <typename T>
    bool enqueue_call(T& call, CompletionManager::Ticket ticket);
    bool enqueue_call(ProposeCall& call, CompletionManager::Ticket ticket);

    // 等待都在调用线程里，只有写请求需要超时返回。
    template <typename T>
    Status wait_call(const T& call, CompletionManager::Ticket ticket);
    Status wait_call(const ProposeCall& call, CompletionManager::Ticket ticket);

    Status handle_event(const TickEvent& event);
    Status handle_event(const AppendResponseEvent& event);
    Status handle_event(const VoteResponseEvent& event);
    Status handle_event(const PreVoteResponseEvent& event);
    Status handle_event(const SnapshotResponseEvent& event);
    Status handle_event(const AppendSendFailedEvent& event);
    Status handle_event(const SnapshotSendFailedEvent& event);

    // 关于handle_call的返回值最终会回落到call->status里
    Status handle_call(RequestVoteCall& call);
    Status handle_call(PreVoteCall& call);
    Status handle_call(AppendEntriesCall& call);
    Status handle_call(BuildReadIndexCall& call);
    Status handle_call(PrepareInstallSnapshotCall& call);
    Status handle_call(CommitInstallSnapshotCall& call);
    Status handle_call(AppendResponseCall& call);
    Status handle_call(SnapshotResponseCall& call);
    Status handle_call(SnapshotSendFailedCall& call);
    Status handle_call(AddMemberCall& call);
    Status handle_call(RemoveMemberCall& call);

    ReplicaContext& context_;
    CompletionManager completions_;
    ProposalTracker proposal_tracker_;
    ReplicaLoopQueue queue_;
};

}  // namespace adviskv::storage
