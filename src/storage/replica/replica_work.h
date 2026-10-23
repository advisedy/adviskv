#pragma once

#include <memory>
#include <optional>
#include <variant>

#include "common/completion.h"
#include "storage/model/model.h"
#include "storage/model/param.h"

namespace adviskv::storage {

struct WaitPolicy {
    std::optional<Milliseconds> timeout;
    Status timeout_status{Status::ERROR("request timed out")};

    // 对于写操作定制的
    static WaitPolicy proposal(Milliseconds timeout) {
        return {timeout, Status::NOT_YET_COMMIT("proposal is not committed before timeout")};
    }
    // 对于读操作定制的
    static WaitPolicy read(Milliseconds timeout) {
        return {timeout, Status::NOT_LEADER("read index quorum not reached before timeout")};
    }
};

struct RequestVoteCall {
    using Result = RequestVoteResult;
    RequestVoteParam param;
};

struct PreVoteCall {
    using Result = PreVoteResult;
    PreVoteParam param;
};

struct AppendEntriesCall {
    using Result = AppendEntriesResult;
    AppendEntriesParam param;
};

struct ProposeCall {
    using Result = void;
    ProposeParam param;
};

struct SubmitReadCall {
    using Result = Value;
    Key key;
};

struct InstallSnapshotCall {
    using Result = void;
    InstallSnapshotParam param;
};

struct AddMemberCall {
    using Result = void;
    PeerMember member;
};

struct RemoveMemberCall {
    using Result = void;
    ReplicaID replica_id;
};

struct RecoverCall {
    using Result = void;
};

// 请求和它的回执位绑在一起。这层壳不是为了包得好看：它让"写请求绑上读的
// completion"变成编译错误，返回什么类型从请求类型就能推出来。
template <typename Request>
struct QueuedCall {
    Request request;
    std::shared_ptr<Completion<typename Request::Result>> completion;
};

// 请求通道：有等待者的入站请求。
using Call = std::variant<QueuedCall<RequestVoteCall>, QueuedCall<PreVoteCall>,
                          QueuedCall<AppendEntriesCall>, QueuedCall<ProposeCall>, QueuedCall<SubmitReadCall>,
                          QueuedCall<InstallSnapshotCall>, QueuedCall<AddMemberCall>,
                          QueuedCall<RemoveMemberCall>, QueuedCall<RecoverCall>>;

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

struct FaultEvent {
    Status status;
};

struct ReadQuorumResponseEvent {
    ReplicaID from;
    uint64 read_seq{0};
    Term response_term{0};
};

struct SnapshotDoneEvent {
    LogIndex capture_index{0};
    Term capture_term{0};
    Status status{Status::OK()};
};

// 事件通道：没人等的通知，所以没有 completion，也不需要套壳。
using Event = std::variant<TickEvent, AppendResponseEvent, VoteResponseEvent, PreVoteResponseEvent,
                           SnapshotResponseEvent, AppendSendFailedEvent, SnapshotSendFailedEvent,
                           SnapshotDoneEvent, ReadQuorumResponseEvent, FaultEvent>;

// 把两个 variant 的 alternative 拼成一个。
// 用它是因为队列要"平铺"：请求和事件都是 PendingTask 的直接 alternative，不能再套一层
// variant<Call, Event>。但两边的类型名只该写一遍，所以让 PendingTask 从 Call / Event 拼出来，
// 而不是手抄 19 个类型名 —— 抄一遍就等于埋一个"加事件时忘了改第二处"的坑。
template <typename... Variants>
struct VariantConcat;

template <typename... Left, typename... Right>
struct VariantConcat<std::variant<Left...>, std::variant<Right...>> {
    using type = std::variant<Left..., Right...>;
};

// 队列里只有一个 variant。这样 loop 分发只需要一次 visit，攒批也只需要一层 get_if；
// 而 Call / Event 仍然各自保留，作为两个通道的类型约束（谁带 completion、谁不带）。
using PendingTask = typename VariantConcat<Call, Event>::type;

}  // namespace adviskv::storage
