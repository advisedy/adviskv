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

template <typename Request>
struct QueuedCall {
    Request request;
    std::shared_ptr<Completion<typename Request::Result>> completion;
};

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

using Event = std::variant<TickEvent, AppendResponseEvent, VoteResponseEvent, PreVoteResponseEvent,
                           SnapshotResponseEvent, AppendSendFailedEvent, SnapshotSendFailedEvent,
                           SnapshotDoneEvent, ReadQuorumResponseEvent, FaultEvent>;

template <typename... Variants>
struct VariantConcat;

template <typename... Left, typename... Right>
struct VariantConcat<std::variant<Left...>, std::variant<Right...>> {
    using type = std::variant<Left..., Right...>;
};

using PendingTask = typename VariantConcat<Call, Event>::type;

}  // namespace adviskv::storage
