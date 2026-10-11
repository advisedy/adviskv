#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <future>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "common/model/model.h"
#include "storage/model/model.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/core/raft_core.h"
#include "storage/raft/state_machine/kv_state_machine.h"
#include "storage/replica/replica_loop.h"
#include "storage/replica/replica_message_dispatcher.h"
#include "test/storage/raft/core/raft_core_test_access.h"

namespace adviskv::storage {

class ReplicaLoopTestPeer {
public:
    using Ticket = CompletionManager::Ticket;

    static bool post_task(ReplicaLoop& loop, ReplicaLoop::Task task) {
        return loop.post(ReplicaLoop::PendingTask{std::move(task)});
    }

    static bool post_empty_task(ReplicaLoop& loop) {
        return loop.post(ReplicaLoop::PendingTask{ReplicaLoop::Task{}});
    }

    static Ticket create_ticket(ReplicaLoop& loop) { return loop.completions_.create(); }

    static Status wait_for(ReplicaLoop& loop, Ticket ticket, Milliseconds timeout, const Status& timeout_status) {
        return loop.completions_.wait_for(ticket, timeout, timeout_status);
    }

    static bool finish(ReplicaLoop& loop, Ticket ticket, const Status& status) {
        return loop.completions_.finish(ticket, status);
    }

    static void finish_work_unit(ReplicaLoop& loop, Ticket ticket, const Status& status) {
        loop.finish_work_unit(ticket, status);
    }

    static std::shared_ptr<ReadRequestState> new_read(ReplicaLoop& loop, Ticket ticket) {
        return loop.read_tracker_.new_request("k", ticket);
    }

    static Status establish_read(ReplicaLoop& loop, const std::shared_ptr<ReadRequestState>& state) {
        return loop.read_tracker_.establish(state);
    }

    static std::thread::id worker_id(ReplicaLoop& loop) { return loop.worker_.get_id(); }

    static bool active(ReplicaLoop& loop) { return loop.active_.load(); }

    static bool post_proposal(ReplicaLoop& loop, Ticket ticket) {
        return loop.post(ReplicaLoop::PendingTask{
                ProposalItem{ProposeParam::write(WriteOpType::PUT, "k", "v"), ticket}});
    }

    static std::vector<size_t> collect_batch_sizes(ReplicaLoop& loop, size_t item_count) {
        for (size_t i = 0; i < item_count; ++i) {
            loop.queue_.push(ReplicaLoop::PendingTask{
                    ProposalItem{ProposeParam::write(WriteOpType::PUT, "k", "v"), i + 1}});
        }

        std::vector<size_t> sizes;
        std::optional<ReplicaLoop::PendingTask> pending;
        while (true) {
            ReplicaLoop::PendingTask next;
            if (pending.has_value()) {
                next = std::move(*pending);
                pending.reset();
            } else if (!loop.queue_.try_pop(next)) {
                break;
            }
            auto items = loop.collect_write_batch(std::move(std::get<ProposalItem>(next)), pending);
            sizes.push_back(items.size());
        }
        return sizes;
    }
};

namespace {

class LoopHarness {
public:
    explicit LoopHarness(const std::function<void()>& runtime_info_publisher = [] {})
            : id_{301, 4, 0},
              persist_("unused", id_),
              state_machine_(EngineType::MAP),
              raft_core_(id_, {PeerMember{"self", id_, Endpoint{"127.0.0.1", 19090}}}),
              dispatcher_(1000, persist_, [](Event) {}),
              fault_reporter_([this](Status status) {
                  if (status.fail() && loop_ptr_ != nullptr) {
                      loop_ptr_->fault_stop();
                  }
                  return status;
              }, [this](Status) {
                  if (loop_ptr_ != nullptr) {
                      loop_ptr_->fault_stop();
                  }
              }),
              loop_(id_, raft_core_, persist_, state_machine_, dispatcher_, fault_reporter_, 1000,
                    runtime_info_publisher) {
        loop_ptr_ = &loop_;
    }

    ~LoopHarness() {
        loop_.stop_snapshot_writer();
        loop_.stop();
    }

    DISALLOW_COPY_AND_ASSIGN(LoopHarness)

    ReplicaLoop& loop() { return loop_; }
    RaftCore& raft_core() { return raft_core_; }

private:
    ReplicaID id_;
    PersistEngine persist_;
    KvStateMachine state_machine_;
    RaftCore raft_core_;
    ReplicaMessageDispatcher dispatcher_;
    ReplicaLoop* loop_ptr_{nullptr};
    FaultReporter fault_reporter_;
    ReplicaLoop loop_;
};

class Gate {
public:
    ~Gate() { release(); }

    void arrive_and_wait() {
        std::unique_lock lock(mutex_);
        arrived_ = true;
        cv_.notify_all();
        cv_.wait(lock, [this] { return released_; });
    }

    bool wait_until_arrived() {
        std::unique_lock lock(mutex_);
        return cv_.wait_for(lock, std::chrono::seconds(2), [this] { return arrived_; });
    }

    void release() {
        std::lock_guard lock(mutex_);
        released_ = true;
        cv_.notify_all();
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    bool arrived_{false};
    bool released_{false};
};

TEST(ReplicaLoopTest, ExecutesOrdinaryTasksInOrderOnItsWorkerThread) {
    LoopHarness harness;
    ReplicaLoop& loop = harness.loop();
    EXPECT_FALSE(loop.on_loop_thread());
    loop.start();
    const std::thread::id worker_id = ReplicaLoopTestPeer::worker_id(loop);

    std::mutex mutex;
    std::condition_variable cv;
    std::vector<int> order;
    bool all_ran_on_loop = true;
    for (int i = 0; i < 40; ++i) {
        ASSERT_TRUE(ReplicaLoopTestPeer::post_task(loop, [&loop, &mutex, &cv, &order, &all_ran_on_loop, i] {
            std::lock_guard lock(mutex);
            all_ran_on_loop = all_ran_on_loop && loop.on_loop_thread();
            order.push_back(i);
            cv.notify_all();
        }));
    }

    std::unique_lock lock(mutex);
    const bool completed = cv.wait_for(lock, std::chrono::seconds(2), [&] { return order.size() == 40; });
    lock.unlock();
    ASSERT_TRUE(completed);
    EXPECT_TRUE(all_ran_on_loop);
    EXPECT_EQ(worker_id, ReplicaLoopTestPeer::worker_id(loop));
    ASSERT_EQ(order.size(), 40U);
    for (int i = 0; i < 40; ++i) {
        EXPECT_EQ(order[i], i);
    }
}

TEST(ReplicaLoopTest, CallPublishesRuntimeInfoBeforeWakingItsWaiter) {
    Gate gate;
    LoopHarness harness([&gate]() { gate.arrive_and_wait(); });
    ReplicaLoop& loop = harness.loop();
    loop.start();
    auto result = std::async(std::launch::async, [&loop]() {
        PreVoteCall call;
        loop.submit_and_wait(&call);
        return call.status;
    });
    const bool arrived = gate.wait_until_arrived();
    const auto before_publish = result.wait_for(std::chrono::milliseconds(100));
    gate.release();
    EXPECT_TRUE(arrived);
    EXPECT_EQ(before_publish, std::future_status::timeout);
    ASSERT_EQ(result.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    EXPECT_TRUE(result.get().ok());
}

TEST(ReplicaLoopTest, CollectsWriteBatchesAtTheFixedLimit) {
    LoopHarness harness;
    ReplicaLoop& loop = harness.loop();

    EXPECT_EQ(ReplicaLoopTestPeer::collect_batch_sizes(loop, 1), (std::vector<size_t>{1}));
    EXPECT_EQ(ReplicaLoopTestPeer::collect_batch_sizes(loop, 64), (std::vector<size_t>{64}));
    EXPECT_EQ(ReplicaLoopTestPeer::collect_batch_sizes(loop, 65), (std::vector<size_t>{64, 1}));
    EXPECT_EQ(ReplicaLoopTestPeer::collect_batch_sizes(loop, 130), (std::vector<size_t>{64, 64, 2}));
}

TEST(ReplicaLoopTest, OrdinaryTaskSeparatesNeighboringWriteBatches) {
    LoopHarness harness;
    ReplicaLoop& loop = harness.loop();
    loop.start();
    std::promise<std::vector<Status>> middle_promise;
    auto middle_result = middle_promise.get_future();
    std::promise<Status> end_promise;
    auto end_result = end_promise.get_future();
    Gate gate;
    ASSERT_TRUE(ReplicaLoopTestPeer::post_task(loop, [&gate] { gate.arrive_and_wait(); }));
    ASSERT_TRUE(gate.wait_until_arrived());

    const auto first_ticket = ReplicaLoopTestPeer::create_ticket(loop);
    const auto second_ticket = ReplicaLoopTestPeer::create_ticket(loop);
    const auto third_ticket = ReplicaLoopTestPeer::create_ticket(loop);
    const auto fourth_ticket = ReplicaLoopTestPeer::create_ticket(loop);
    ASSERT_TRUE(ReplicaLoopTestPeer::post_proposal(loop, first_ticket));
    ASSERT_TRUE(ReplicaLoopTestPeer::post_proposal(loop, second_ticket));

    ASSERT_TRUE(ReplicaLoopTestPeer::post_task(loop, [&loop, &middle_promise, first_ticket, second_ticket, third_ticket] {
        const Status not_finished = Status::ERROR("ticket was not finished before the barrier");
        std::vector<Status> observed;
        observed.push_back(ReplicaLoopTestPeer::wait_for(loop, first_ticket, Milliseconds(0), not_finished));
        observed.push_back(ReplicaLoopTestPeer::wait_for(loop, second_ticket, Milliseconds(0), not_finished));
        observed.push_back(ReplicaLoopTestPeer::wait_for(loop, third_ticket, Milliseconds(0), not_finished));
        middle_promise.set_value(std::move(observed));
    }));
    ASSERT_TRUE(ReplicaLoopTestPeer::post_proposal(loop, third_ticket));
    ASSERT_TRUE(ReplicaLoopTestPeer::post_proposal(loop, fourth_ticket));

    ASSERT_TRUE(ReplicaLoopTestPeer::post_task(loop, [&loop, &end_promise, fourth_ticket] {
        end_promise.set_value(ReplicaLoopTestPeer::wait_for(
                loop, fourth_ticket, Milliseconds(0), Status::ERROR("last write was not handled")));
    }));
    gate.release();

    ASSERT_EQ(middle_result.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    const std::vector<Status> observed = middle_result.get();
    EXPECT_EQ(observed[0].code(), StatusCode::INVALID_ARGUMENT);
    EXPECT_EQ(observed[1].code(), StatusCode::INVALID_ARGUMENT);
    EXPECT_EQ(observed[2].msg(), "ticket was not finished before the barrier");
    ASSERT_EQ(end_result.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    EXPECT_EQ(end_result.get().code(), StatusCode::INVALID_ARGUMENT);
}

TEST(ReplicaLoopTest, RejectsSubmissionsBeforeStartAfterStopAndEmptyTasks) {
    LoopHarness harness;
    ReplicaLoop& loop = harness.loop();
    EXPECT_FALSE(ReplicaLoopTestPeer::post_task(loop, [] {}));
    loop.start();
    const std::thread::id worker_id = ReplicaLoopTestPeer::worker_id(loop);
    loop.start();
    EXPECT_EQ(worker_id, ReplicaLoopTestPeer::worker_id(loop));
    EXPECT_FALSE(ReplicaLoopTestPeer::post_empty_task(loop));

    loop.stop();
    loop.stop();
    EXPECT_FALSE(ReplicaLoopTestPeer::post_task(loop, [] {}));
}

TEST(ReplicaLoopTest, StopFailsAcceptedTasksThatWereNotExecuted) {
    LoopHarness harness;
    ReplicaLoop& loop = harness.loop();
    loop.start();
    Gate gate;
    ASSERT_TRUE(ReplicaLoopTestPeer::post_task(loop, [&gate] { gate.arrive_and_wait(); }));
    ASSERT_TRUE(gate.wait_until_arrived());

    const auto ticket = ReplicaLoopTestPeer::create_ticket(loop);
    ASSERT_TRUE(ReplicaLoopTestPeer::post_task(loop, [&loop, ticket] {
        ReplicaLoopTestPeer::finish(loop, ticket, Status::OK());
    }));
    std::thread stopper([&loop] { loop.stop(); });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (ReplicaLoopTestPeer::active(loop) && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    const bool stopped = !ReplicaLoopTestPeer::active(loop);
    gate.release();
    stopper.join();

    EXPECT_TRUE(stopped);
    EXPECT_EQ(ReplicaLoopTestPeer::wait_for(
                      loop, ticket, Milliseconds(0), Status::ERROR("ticket was not failed by stop"))
                      .msg(),
              "raft loop is stopped");
}

TEST(ReplicaLoopTest, FaultStopsAndFailsAnAcceptedPendingTask) {
    LoopHarness harness;
    ReplicaLoop& loop = harness.loop();
    loop.start();
    Gate gate;
    ASSERT_TRUE(ReplicaLoopTestPeer::post_task(loop, [&gate] { gate.arrive_and_wait(); }));
    ASSERT_TRUE(gate.wait_until_arrived());

    const auto ticket = ReplicaLoopTestPeer::create_ticket(loop);
    loop.submit_async(FaultEvent{Status::ERROR("test fault")});
    ASSERT_TRUE(ReplicaLoopTestPeer::post_task(loop, [&loop, ticket] {
        ReplicaLoopTestPeer::finish(loop, ticket, Status::OK());
    }));
    gate.release();

    const Status status = ReplicaLoopTestPeer::wait_for(
            loop, ticket, Milliseconds(2000), Status::ERROR("fault did not finish the pending ticket"));
    EXPECT_EQ(status.code(), StatusCode::IS_FAULTED);
}

TEST(ReplicaLoopTest, FaultTailPreservesCurrentCallErrorAndFailsOtherTickets) {
    LoopHarness harness;
    ReplicaLoop& loop = harness.loop();
    loop.start();
    const auto current_ticket = ReplicaLoopTestPeer::create_ticket(loop);
    const auto pending_ticket = ReplicaLoopTestPeer::create_ticket(loop);
    ASSERT_TRUE(ReplicaLoopTestPeer::post_task(loop, [&loop, current_ticket]() {
        loop.fault_stop();
        ReplicaLoopTestPeer::finish_work_unit(loop, current_ticket, Status::ERROR("original call error"));
    }));
    const Status timeout = Status::ERROR("fault tail did not finish ticket");
    const Status current = ReplicaLoopTestPeer::wait_for(loop, current_ticket, Milliseconds(500), timeout);
    EXPECT_EQ(current.code(), StatusCode::ERROR);
    EXPECT_EQ(current.msg(), "original call error");
    EXPECT_EQ(ReplicaLoopTestPeer::wait_for(loop, pending_ticket, Milliseconds(500), timeout).code(),
              StatusCode::IS_FAULTED);
}

TEST(ReplicaLoopTest, FaultTailRejectsCurrentCallSuccess) {
    LoopHarness harness;
    ReplicaLoop& loop = harness.loop();
    loop.start();
    const auto ticket = ReplicaLoopTestPeer::create_ticket(loop);
    ASSERT_TRUE(ReplicaLoopTestPeer::post_task(loop, [&loop, ticket]() {
        loop.fault_stop();
        ReplicaLoopTestPeer::finish_work_unit(loop, ticket, Status::OK());
    }));
    EXPECT_EQ(ReplicaLoopTestPeer::wait_for(
                      loop, ticket, Milliseconds(500), Status::ERROR("fault tail did not finish ticket")).code(),
              StatusCode::IS_FAULTED);
}

TEST(ReplicaLoopTest, ResponseTailFailsPendingReadsAfterLeaderChanges) {
    LoopHarness harness;
    ReplicaLoop& loop = harness.loop();
    RaftCore& core = harness.raft_core();
    RaftEffects effects;
    RaftCoreTestPeer::become_candidate(core, effects);
    ASSERT_TRUE(core.is_leader());
    ASSERT_GT(core.commit_index(), core.last_applied());

    const auto ticket = ReplicaLoopTestPeer::create_ticket(loop);
    const auto state = ReplicaLoopTestPeer::new_read(loop, ticket);
    ASSERT_TRUE(ReplicaLoopTestPeer::establish_read(loop, state).ok());
    ASSERT_FALSE(state->done);

    AppendEntriesResult response;
    response.term = core.current_term() + 1;
    RaftEffects step_down_effects;
    EXPECT_EQ(core.handle_append_response(ReplicaID{301, 4, 0}, AppendEntriesParam{}, response,
                                         step_down_effects).code(), StatusCode::NOT_LEADER);
    ASSERT_FALSE(core.is_leader());
    loop.start();
    loop.submit_async(VoteResponseEvent{ReplicaID{301, 4, 0}, RequestVoteResult{core.current_term(), false}});

    const Status status = ReplicaLoopTestPeer::wait_for(
            loop, ticket, Milliseconds(500), Status::ERROR("response tail did not fail pending read"));
    EXPECT_EQ(status.code(), StatusCode::NOT_LEADER);
    EXPECT_TRUE(state->done);
}

TEST(ReplicaLoopTest, FaultTailFailsAReadBeforeItCanCompleteNormally) {
    LoopHarness harness;
    ReplicaLoop& loop = harness.loop();
    RaftCore& core = harness.raft_core();
    RaftEffects effects;
    RaftCoreTestPeer::become_candidate(core, effects);
    ASSERT_TRUE(core.is_leader());

    const auto ticket = ReplicaLoopTestPeer::create_ticket(loop);
    const auto state = ReplicaLoopTestPeer::new_read(loop, ticket);
    ASSERT_TRUE(ReplicaLoopTestPeer::establish_read(loop, state).ok());
    ASSERT_FALSE(state->done);
    core.advance_last_applied(core.commit_index());
    loop.start();
    loop.submit_async(FaultEvent{Status::ERROR("test fault")});

    const Status status = ReplicaLoopTestPeer::wait_for(
            loop, ticket, Milliseconds(500), Status::ERROR("fault tail did not finish pending read"));
    EXPECT_EQ(status.code(), StatusCode::IS_FAULTED);
    EXPECT_EQ(state->status.code(), StatusCode::IS_FAULTED);
    EXPECT_TRUE(state->done);
}

TEST(ReplicaLoopTest, ApplyFailureCancelsThePrefetchedTaskAndFailsItsTicket) {
    LoopHarness harness;
    ReplicaLoop& loop = harness.loop();
    ReplicaID id{301, 4, 0};
    LogEntry poison;
    poison.term = 1;
    poison.index = 1;
    poison.op_type = WriteOpType::PROMOTE_VOTER;
    poison.config_replica_id = ReplicaID{301, 4, 99};
    harness.raft_core().update_log_entries({poison});
    RaftEffects effects;
    AppendEntriesResult append_result;
    harness.raft_core().handle_append_entries(
            AppendEntriesParam{id, id, 1, {}, 1, 1, 1}, append_result, effects);
    ASSERT_TRUE(append_result.success);

    loop.start();
    Gate gate;
    ASSERT_TRUE(ReplicaLoopTestPeer::post_task(loop, [&gate] { gate.arrive_and_wait(); }));
    ASSERT_TRUE(gate.wait_until_arrived());

    const auto write_ticket = ReplicaLoopTestPeer::create_ticket(loop);
    const auto pending_ticket = ReplicaLoopTestPeer::create_ticket(loop);
    ASSERT_TRUE(ReplicaLoopTestPeer::post_proposal(loop, write_ticket));
    ASSERT_TRUE(ReplicaLoopTestPeer::post_task(loop, [&loop, pending_ticket] {
        ReplicaLoopTestPeer::finish(loop, pending_ticket, Status::OK());
    }));
    gate.release();

    const Status status = ReplicaLoopTestPeer::wait_for(
            loop, pending_ticket, Milliseconds(2000), Status::ERROR("apply failure did not fail pending task"));
    EXPECT_EQ(status.code(), StatusCode::IS_FAULTED);
}

}
}
