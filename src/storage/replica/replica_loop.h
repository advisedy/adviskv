#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <optional>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include "common/blocking_queue.h"
#include "common/completion.h"
#include "common/defer.h"
#include "common/define.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/core/raft_core.h"
#include "storage/raft/state_machine/state_machine.h"
#include "storage/replica/completion_registry.h"
#include "storage/replica/fault_reporter.h"
#include "storage/replica/proposal_tracker.h"
#include "storage/replica/raft_step.h"
#include "storage/replica/read_tracker.h"
#include "storage/replica/replica_work.h"
#include "storage/replica/snapshot_tracker.h"

namespace adviskv::storage {

class ReplicaMessageDispatcher;
class ReplicaLoopTestPeer;

class ReplicaLoop {
public:
    ReplicaLoop(ReplicaID replica_id, RaftCore& raft_core, PersistEngine& persist, StateMachine& state_machine,
                ReplicaMessageDispatcher& message_dispatcher, FaultReporter fault_reporter,
                int32_t raft_do_snapshot_limit, std::function<void()> runtime_info_publisher);
    ~ReplicaLoop();

    DISALLOW_COPY_AND_ASSIGN(ReplicaLoop)

    void start();
    void close();
    void stop();
    void submit_async(Event event);

    template <typename Request>
    CompletionResult<typename Request::Result> submit_and_wait(Request request, const WaitPolicy& policy = {}) {
        auto completion = std::make_shared<Completion<typename Request::Result>>();
        if (!completion_registry_.add(completion)) {
            completion->fail(Status::ERROR("raft loop is not running"));
            return completion->wait();
        }
        auto remove_completion_defer = Defer([&] { completion_registry_.remove(completion); });
        if (!post(QueuedCall<Request>{std::move(request), completion})) {
            completion->fail(Status::ERROR("raft loop is not running"));
        }
        if (policy.timeout.has_value()) {
            return completion->wait_for(*policy.timeout, policy.timeout_status);
        }
        return completion->wait();
    }

    bool on_loop_thread() const;
    void enter_fault();

private:
    friend class ReplicaLoopTestPeer;

    void run();
    std::vector<QueuedCall<ProposeCall>> collect_write_batch(QueuedCall<ProposeCall> first);
    bool post(PendingTask task);
    bool faulted_{false};
    void advance_trackers();

    Status finish_dispatch(Status status);

    template <typename Request>
    Status dispatch(QueuedCall<Request>& call) {
        auto result = handle_call(call.request);
        Status status = finish_dispatch(result.status);
        if (status.fail()) {
            result = CompletionResult<typename Request::Result>::fail(status);
        }
        IGNORE_RESULT(call.completion->complete(std::move(result)))
        return status;
    }

    Status dispatch(QueuedCall<ProposeCall>& call) {
        std::vector<QueuedCall<ProposeCall>> calls = collect_write_batch(std::move(call));
        if (!active_.load()) {
            return Status::OK();
        }
        return finish_dispatch(proposal_tracker_.submit_batch(std::move(calls)));
    }

    Status dispatch(QueuedCall<SubmitReadCall>& call) {
        Status status = finish_dispatch(read_tracker_.establish(call.request.key, call.completion));
        if (status.fail()) {
            call.completion->fail(status);
        }
        return status;
    }

    template <typename EventType>
    Status dispatch(const EventType& event) {
        return finish_dispatch(handle_event(event));
    }

    Status handle_event(const TickEvent& event);
    Status handle_event(const AppendResponseEvent& event);
    Status handle_event(const VoteResponseEvent& event);
    Status handle_event(const PreVoteResponseEvent& event);
    Status handle_event(const SnapshotResponseEvent& event);
    Status handle_event(const AppendSendFailedEvent& event);
    Status handle_event(const SnapshotSendFailedEvent& event);
    Status handle_event(const SnapshotDoneEvent& event);
    Status handle_event(const FaultEvent& event);
    Status handle_event(const ReadQuorumResponseEvent& event);
    void on_snapshot_written(LogIndex capture_index, Term capture_term, Status status);

    Status apply_committed_entries_inline();
    Status apply_one_entry(const LogEntry& entry);

    CompletionResult<RequestVoteResult> handle_call(const RequestVoteCall& call);
    CompletionResult<PreVoteResult> handle_call(const PreVoteCall& call);
    CompletionResult<AppendEntriesResult> handle_call(const AppendEntriesCall& call);
    CompletionResult<void> handle_call(const InstallSnapshotCall& call);
    CompletionResult<void> handle_call(const AddMemberCall& call);
    CompletionResult<void> handle_call(const RemoveMemberCall& call);
    CompletionResult<void> handle_call(const RecoverCall& call);
    void fail_all_pending(const Status& status);

    void publish_runtime_info_snapshot() {
        if (runtime_info_publisher_) runtime_info_publisher_();
    }

    ReplicaID replica_id_;
    RaftCore& raft_core_;
    PersistEngine& persist_;
    StateMachine& state_machine_;
    ReplicaMessageDispatcher& message_dispatcher_;
    FaultReporter fault_reporter_;
    CompletionRegistry completion_registry_;
    RaftEffectExecutor raft_effect_executor_;
    ProposalTracker proposal_tracker_;
    ReadTracker read_tracker_;
    BlockingQueue<PendingTask> queue_;
    std::optional<PendingTask> next_task_;
    std::thread worker_;
    std::atomic<bool> started_{false};
    std::atomic<bool> active_{false};
    SnapshotTracker snapshot_tracker_;
    std::function<void()> runtime_info_publisher_;
};

}  // namespace adviskv::storage
