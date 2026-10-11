#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "common/blocking_queue.h"
#include "common/define.h"
#include "common/model/model.h"
#include "common/status.h"
#include "common/types.h"
#include "storage/model/model.h"
#include "storage/model/param.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/core/raft_core.h"
#include "storage/raft/state_machine/state_machine.h"
#include "storage/replica/completion_manager.h"
#include "storage/replica/fault_reporter.h"
#include "storage/replica/proposal_tracker.h"
#include "storage/replica/read_tracker.h"
#include "storage/replica/raft_step.h"
#include "storage/replica/snapshot_tracker.h"
#include "storage/replica/snapshot_writer.h"

namespace adviskv::storage {

class ReplicaMessageDispatcher;

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
                           SnapshotResponseEvent, AppendSendFailedEvent, SnapshotSendFailedEvent, SnapshotDoneEvent,
                           ReadQuorumResponseEvent, FaultEvent>;

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

struct SubmitReadCall {
    Key key;
    std::shared_ptr<ReadRequestState> state;
    Status status{Status::OK()};
};

struct InstallSnapshotCall {
    InstallSnapshotParam param;
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

struct RecoverCall {
    bool need_recover{false};
    Status status{Status::OK()};
};

using Call = std::variant<RequestVoteCall*, PreVoteCall*, AppendEntriesCall*, SubmitReadCall*, InstallSnapshotCall*,
                          ProposeCall*, AddMemberCall*, RemoveMemberCall*, RecoverCall*>;

class ReplicaLoopTestPeer;

class ReplicaLoop {
public:
    ReplicaLoop(ReplicaID replica_id, RaftCore& raft_core, PersistEngine& persist, StateMachine& state_machine,
                ReplicaMessageDispatcher& message_dispatcher, FaultReporter fault_reporter,
                int32_t raft_do_snapshot_limit, std::function<void()> runtime_info_publisher);
    ~ReplicaLoop();

    DISALLOW_COPY_AND_ASSIGN(ReplicaLoop)

    void start();
    void stop();
    void stop_snapshot_writer() { snapshot_tracker_.stop_writer(); }

    void submit_async(Event event);
    void submit_and_wait(Call call);

    bool on_loop_thread() const;

    void fault_stop();

private:
    friend class ReplicaLoopTestPeer;

    using Task = std::function<void()>;
    using PendingTask = std::variant<Task, ProposalItem>;

    void run();
    std::vector<ProposalItem> collect_write_batch(ProposalItem first, std::optional<PendingTask>& pending);
    Status handle_write_batch(std::vector<ProposalItem> items);
    bool post(PendingTask task);

    void finish_fault_stop();
    bool fault_pending_{false};

    template <typename T>
    bool enqueue_call(T& call, CompletionManager::Ticket ticket);
    bool enqueue_call(ProposeCall& call, CompletionManager::Ticket ticket);
    bool enqueue_call(SubmitReadCall& call, CompletionManager::Ticket ticket);

    template <typename T>
    Status wait_call(const T& call, CompletionManager::Ticket ticket);
    Status wait_call(const ProposeCall& call, CompletionManager::Ticket ticket);
    Status wait_call(const SubmitReadCall& call, CompletionManager::Ticket ticket);

    void advance_pending_work();
    void finish_work_unit(CompletionManager::Ticket ticket = 0, const Status& status = Status::OK());

    Status handle_event(const TickEvent& event);
    Status handle_event(const AppendResponseEvent& event);
    Status handle_event(const VoteResponseEvent& event);
    Status handle_event(const PreVoteResponseEvent& event);
    Status handle_event(const SnapshotResponseEvent& event);
    Status handle_event(const AppendSendFailedEvent& event);
    Status handle_event(const SnapshotSendFailedEvent& event);
    Status handle_event(const SnapshotDoneEvent& event);

    void on_snapshot_written(LogIndex capture_index, Term capture_term, Status status);
    Status handle_event(const FaultEvent& event);
    Status handle_event(const ReadQuorumResponseEvent& event);

    Status apply_committed_entries_inline();
    Status apply_one_entry(const LogEntry& entry);

    Status handle_call(RequestVoteCall& call);
    Status handle_call(PreVoteCall& call);
    Status handle_call(AppendEntriesCall& call);
    Status handle_call(InstallSnapshotCall& call);
    Status handle_call(AddMemberCall& call);
    Status handle_call(RemoveMemberCall& call);
    Status handle_call(RecoverCall& call);

    void fail_all_pending(const Status& status);

    void publish_runtime_info_snapshot() {
        if (runtime_info_publisher_) {
            runtime_info_publisher_();
        }
    }

    ReplicaID replica_id_;
    RaftCore& raft_core_;
    PersistEngine& persist_;
    StateMachine& state_machine_;
    ReplicaMessageDispatcher& message_dispatcher_;
    FaultReporter fault_reporter_;

    CompletionManager completions_;
    RaftEffectExecutor raft_effect_executor_;
    ProposalTracker proposal_tracker_;
    ReadTracker read_tracker_;

    BlockingQueue<PendingTask> queue_;
    std::thread worker_;
    std::atomic<bool> started_{false};
    std::atomic<bool> active_{false};

    SnapshotTracker snapshot_tracker_;

    std::function<void()> runtime_info_publisher_;
};

}  // namespace adviskv::storage
