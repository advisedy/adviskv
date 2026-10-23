#include "storage/replica/replica_loop.h"

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <future>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "common/completion.h"
#include "common/defer.h"
#include "common/model/model.h"
#include "storage/model/model.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/core/raft_core.h"
#include "storage/raft/state_machine/kv_state_machine.h"
#include "storage/replica/replica_message_dispatcher.h"
#include "test/storage/raft/core/raft_core_test_access.h"
#include "test/test_env.h"

namespace adviskv::storage {

class ReplicaLoopTestPeer {
public:
    // 启动前排好具体请求，避免靠线程调度猜测第二个调用有没有入队。
    // 和提交入口一样登记 completion，关闭测试才真正覆盖 registry 的唤醒。
    template <typename Request>
    static auto queue_call(ReplicaLoop& loop, Request request) {
        auto completion = std::make_shared<Completion<typename Request::Result>>();
        EXPECT_TRUE(loop.completion_registry_.add(completion));
        EXPECT_TRUE(loop.queue_.push(PendingTask{QueuedCall<Request>{std::move(request), completion}}));
        return completion;
    }

    static bool post_tick(ReplicaLoop& loop) { return loop.post(PendingTask{TickEvent{}}); }

    static std::thread::id worker_id(ReplicaLoop& loop) { return loop.worker_.get_id(); }

    // 往队列里塞一个写请求。它和别的请求一样走请求通道，没有单独的入队方式。
    static void queue_write(ReplicaLoop& loop, const std::string& request_id) {
        auto completion = std::make_shared<Completion<void>>();
        EXPECT_TRUE(loop.queue_.push(PendingTask{QueuedCall<ProposeCall>{
                ProposeCall{ProposeParam::write(WriteOpType::PUT, "k", "v", request_id)}, completion}}));
    }

    // 按脚本往队列里排任务：'w' 写请求、'c' 普通调用、'e' 事件。
    // 然后照 loop 的写法跑批收集，返回每个写批的尺寸。
    static std::vector<size_t> collect_batch_sizes(ReplicaLoop& loop, const std::string& script) {
        for (size_t i = 0; i < script.size(); ++i) {
            if (script[i] == 'w') {
                queue_write(loop, "rid-" + std::to_string(i));
            } else if (script[i] == 'c') {
                queue_call(loop, PreVoteCall{});
            } else {
                EXPECT_TRUE(loop.queue_.push(PendingTask{TickEvent{}}));
            }
        }

        std::vector<size_t> sizes;
        while (true) {
            PendingTask next;
            // 和 run() 一样：先看暂存项，没有才从队列取。
            if (loop.next_task_.has_value()) {
                next = std::move(*loop.next_task_);
                loop.next_task_.reset();
            } else if (!loop.queue_.try_pop(next)) {
                break;
            }
            auto* first_write = std::get_if<QueuedCall<ProposeCall>>(&next);
            if (first_write == nullptr) {
                // 非写任务在真 loop 里直接分发，不产生写批。
                continue;
            }
            sizes.push_back(loop.collect_write_batch(std::move(*first_write)).size());
        }
        return sizes;
    }
};

namespace {

class LoopHarness {
public:
    // 默认不初始化持久化目录，只有真正写日志的测试才准备磁盘状态。
    explicit LoopHarness(
            const std::function<void()>& runtime_info_publisher = [] {}, const std::string& data_dir = "unused")
            : id_{301, 4, 0},
              persist_(data_dir, id_),
              state_machine_(EngineType::MAP),
              raft_core_(id_, {PeerMember{"self", id_, Endpoint{"127.0.0.1", 19090}}}),
              dispatcher_(1000, persist_, [](Event) {}),
              fault_reporter_(
                      [this](Status status) {
                          if (status.fail() && loop_ptr_ != nullptr) {
                              loop_ptr_->enter_fault();
                          }
                          return status;
                      },
                      [this](Status) {
                          if (loop_ptr_ != nullptr) {
                              loop_ptr_->enter_fault();
                          }
                      }),
              loop_(id_, raft_core_, persist_, state_machine_, dispatcher_, fault_reporter_, 1000,
                    runtime_info_publisher) {
        loop_ptr_ = &loop_;
    }

    ~LoopHarness() { loop_.stop(); }

    DISALLOW_COPY_AND_ASSIGN(LoopHarness)

    ReplicaLoop& loop() { return loop_; }
    RaftCore& raft_core() { return raft_core_; }
    PersistEngine& persist() { return persist_; }

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

TEST(ReplicaLoopTest, CollectsWriteBatchesAtTheFixedLimit) {
    LoopHarness harness;
    ReplicaLoop& loop = harness.loop();
    EXPECT_EQ(ReplicaLoopTestPeer::collect_batch_sizes(loop, "w"), (std::vector<size_t>{1}));
    EXPECT_EQ(ReplicaLoopTestPeer::collect_batch_sizes(loop, std::string(64, 'w')), (std::vector<size_t>{64}));
    EXPECT_EQ(ReplicaLoopTestPeer::collect_batch_sizes(loop, std::string(65, 'w')), (std::vector<size_t>{64, 1}));
    EXPECT_EQ(ReplicaLoopTestPeer::collect_batch_sizes(loop, std::string(130, 'w')), (std::vector<size_t>{64, 64, 2}));
}

// 普通 Call 和 Event 都会把写批断开：断点后面的写请求留到下一轮，顺序和内容都不许变。
TEST(ReplicaLoopTest, NonWriteTaskBreaksTheWriteBatch) {
    LoopHarness harness;
    ReplicaLoop& loop = harness.loop();
    EXPECT_EQ(ReplicaLoopTestPeer::collect_batch_sizes(loop, "wwcw"), (std::vector<size_t>{2, 1}));
    EXPECT_EQ(ReplicaLoopTestPeer::collect_batch_sizes(loop, "wew"), (std::vector<size_t>{1, 1}));
    // 批首不是写请求时，它自己就不该被当成写批处理
    EXPECT_EQ(ReplicaLoopTestPeer::collect_batch_sizes(loop, "cww"), (std::vector<size_t>{2}));
    EXPECT_EQ(ReplicaLoopTestPeer::collect_batch_sizes(loop, "weew"), (std::vector<size_t>{1, 1}));
}

TEST(ReplicaLoopTest, RejectsSubmissionsBeforeStartAndAfterStop) {
    LoopHarness harness;
    ReplicaLoop& loop = harness.loop();
    EXPECT_FALSE(ReplicaLoopTestPeer::post_tick(loop));
    EXPECT_EQ(loop.submit_and_wait(PreVoteCall{}).status.code(), StatusCode::ERROR);
    loop.start();
    const std::thread::id worker_id = ReplicaLoopTestPeer::worker_id(loop);
    loop.start();
    EXPECT_EQ(worker_id, ReplicaLoopTestPeer::worker_id(loop));
    loop.stop();
    loop.stop();
    EXPECT_FALSE(ReplicaLoopTestPeer::post_tick(loop));
    EXPECT_EQ(loop.submit_and_wait(PreVoteCall{}).status.code(), StatusCode::ERROR);
}

// 启动前就 close 掉的 loop 不允许再被 start 拉起来：不然会多出一个对着已关闭队列空转的 worker。
TEST(ReplicaLoopTest, CloseBeforeStartDoesNotCreateWorker) {
    LoopHarness harness;
    ReplicaLoop& loop = harness.loop();
    loop.close();
    loop.start();
    EXPECT_EQ(ReplicaLoopTestPeer::worker_id(loop), std::thread::id{});
    EXPECT_FALSE(ReplicaLoopTestPeer::post_tick(loop));
    EXPECT_EQ(loop.submit_and_wait(PreVoteCall{}).status.code(), StatusCode::ERROR);
}

TEST(ReplicaLoopTest, ProposeCallCompletesViaCompletion) {
    const auto dir = adviskv::test::make_unique_test_dir("propose_completion", 0);
    LoopHarness harness([] {}, dir.string());
    ASSERT_TRUE(harness.persist().init().ok());
    ReplicaLoop& loop = harness.loop();
    RaftEffects effects;
    RaftCoreTestPeer::become_candidate(harness.raft_core(), effects);
    ASSERT_TRUE(harness.raft_core().is_leader());
    loop.start();

    auto result = loop.submit_and_wait(ProposeCall{ProposeParam::write(WriteOpType::PUT, "k1", "v1", "req-1")},
                                       WaitPolicy::proposal(Milliseconds{2000}));
    EXPECT_TRUE(result.status.ok()) << result.status.to_string();
}

TEST(ReplicaLoopTest, ProposeCallDeduplicatesByRequestId) {
    const auto dir = adviskv::test::make_unique_test_dir("propose_completion", 1);
    LoopHarness harness([] {}, dir.string());
    ASSERT_TRUE(harness.persist().init().ok());
    ReplicaLoop& loop = harness.loop();
    RaftEffects effects;
    RaftCoreTestPeer::become_candidate(harness.raft_core(), effects);
    ASSERT_TRUE(harness.raft_core().is_leader());
    loop.start();

    auto run_put = [&loop]() {
        return std::async(std::launch::async, [&loop]() {
            return loop.submit_and_wait(ProposeCall{ProposeParam::write(WriteOpType::PUT, "k", "v", "dup-1")},
                                        WaitPolicy::proposal(Milliseconds{2000}));
        });
    };
    auto f1 = run_put();
    auto f2 = run_put();
    EXPECT_TRUE(f1.get().status.ok());
    EXPECT_TRUE(f2.get().status.ok());
}

TEST(ReplicaLoopTest, ProposeCallFailsWhenLoopNotRunning) {
    LoopHarness harness;
    auto result = harness.loop().submit_and_wait(ProposeCall{ProposeParam::write(WriteOpType::PUT, "k", "v", "req-x")},
                                                 WaitPolicy::proposal(Milliseconds{100}));
    EXPECT_EQ(result.status.code(), StatusCode::ERROR);
}

TEST(ReplicaLoopTest, ProposeCallAfterFaultStopFailsImmediately) {
    ReplicaLoop* loop_ptr = nullptr;
    // 故障必须在 loop 线程触发，避免测试自己跨线程修改故障状态。
    LoopHarness harness([&] { loop_ptr->enter_fault(); });
    loop_ptr = &harness.loop();
    auto first = ReplicaLoopTestPeer::queue_call(*loop_ptr, PreVoteCall{});
    loop_ptr->start();
    EXPECT_EQ(first->wait_for(Milliseconds{2000}, Status::ERROR("test timeout")).status.code(), StatusCode::IS_FAULTED);
    auto result = loop_ptr->submit_and_wait(ProposeCall{ProposeParam::write(WriteOpType::PUT, "k", "v", "req-faulted")},
                                            WaitPolicy::proposal(Milliseconds{30}));
    EXPECT_EQ(result.status.code(), StatusCode::ERROR);
}

TEST(ReplicaLoopTest, ProposeCallOnFollowerFailsFast) {
    LoopHarness harness;
    harness.loop().start();
    auto result =
            harness.loop().submit_and_wait(ProposeCall{ProposeParam::write(WriteOpType::PUT, "k", "v", "req-follower")},
                                           WaitPolicy::proposal(Milliseconds{2000}));
    EXPECT_EQ(result.status.code(), StatusCode::NOT_LEADER);
}

TEST(ReplicaLoopTest, PublishesBeforeWakingCallerAndProcessingNextCall) {
    Gate gate;
    LoopHarness harness([&] { gate.arrive_and_wait(); });
    auto first = ReplicaLoopTestPeer::queue_call(harness.loop(), PreVoteCall{});
    auto second = ReplicaLoopTestPeer::queue_call(harness.loop(), PreVoteCall{});
    // 必须先释放 gate 再析构 harness，ASSERT 提前返回也不能卡在 join。
    auto release = Defer([&] { gate.release(); });
    harness.loop().start();
    ASSERT_TRUE(gate.wait_until_arrived());
    EXPECT_EQ(first->state(), CompletionState::Pending);
    EXPECT_EQ(second->state(), CompletionState::Pending);
    gate.release();
    EXPECT_TRUE(first->wait_for(Milliseconds{2000}, Status::ERROR("test timeout")).status.ok());
    EXPECT_TRUE(second->wait_for(Milliseconds{2000}, Status::ERROR("test timeout")).status.ok());
}

TEST(ReplicaLoopTest, CloseWakesQueuedCallerWhileWorkerIsBlocked) {
    Gate gate;
    LoopHarness harness([&] { gate.arrive_and_wait(); });
    auto first = ReplicaLoopTestPeer::queue_call(harness.loop(), PreVoteCall{});
    auto queued = ReplicaLoopTestPeer::queue_call(harness.loop(), PreVoteCall{});
    std::future<CompletionResult<PreVoteResult>> waiter;
    std::future<void> stopper;
    // guard 比 future 晚构造，失败退出时先放行 worker，再等 future 析构。
    auto release = Defer([&] { gate.release(); });
    harness.loop().start();
    ASSERT_TRUE(gate.wait_until_arrived());
    waiter = std::async(std::launch::async,
                        [queued] { return queued->wait_for(Milliseconds{2000}, Status::ERROR("test timeout")); });
    stopper = std::async(std::launch::async, [&] {
        harness.loop().close();
        harness.loop().close();
    });
    // 如果 close 错误地等 join，断言失败时 guard 也会先放行它。
    ASSERT_EQ(stopper.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    stopper.get();
    EXPECT_EQ(first->state(), CompletionState::Completed);
    EXPECT_EQ(queued->state(), CompletionState::Completed);
    ASSERT_EQ(waiter.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    EXPECT_EQ(waiter.get().status.code(), StatusCode::ERROR);
    EXPECT_EQ(harness.loop().submit_and_wait(PreVoteCall{}).status.code(), StatusCode::ERROR);
    // 上面的所有断言都发生在 worker 仍卡住时，stop 的 join 留到放行之后。
    gate.release();
    harness.loop().stop();
}

TEST(ReplicaLoopTest, WaitPoliciesExpireWhileRequestsAreQueued) {
    Gate gate;
    LoopHarness harness([&] { gate.arrive_and_wait(); });
    auto blocker = ReplicaLoopTestPeer::queue_call(harness.loop(), PreVoteCall{});
    auto release = Defer([&] { gate.release(); });
    harness.loop().start();
    ASSERT_TRUE(gate.wait_until_arrived());

    // worker 确定卡在发布阶段，超时只能由提交入口的等待预算触发。
    auto proposal = harness.loop().submit_and_wait(
            ProposeCall{ProposeParam::write(WriteOpType::PUT, "k", "v", "queued-timeout")},
            WaitPolicy::proposal(Milliseconds{10}));
    EXPECT_EQ(proposal.status.code(), StatusCode::NOT_YET_COMMIT);
    auto read = harness.loop().submit_and_wait(SubmitReadCall{"missing"}, WaitPolicy::read(Milliseconds{10}));
    EXPECT_EQ(read.status.code(), StatusCode::NOT_LEADER);
    EXPECT_FALSE(read.value.has_value());
    EXPECT_EQ(blocker->state(), CompletionState::Pending);
    gate.release();
}

// 排队期间就超时走人的写请求，loop 后面照样会处理它，但结果不能再交付一次。
TEST(ReplicaLoopTest, LateProposalResultIsNotRedeliveredAfterTimeout) {
    Gate gate;
    LoopHarness harness([&] { gate.arrive_and_wait(); });
    auto blocker = ReplicaLoopTestPeer::queue_call(harness.loop(), PreVoteCall{});
    auto proposal = ReplicaLoopTestPeer::queue_call(
            harness.loop(), ProposeCall{ProposeParam::write(WriteOpType::PUT, "k", "v", "late-timeout")});
    // 排在写请求后面的普通调用，它完成就说明写请求一定已经被 loop 处理过了。
    auto after = ReplicaLoopTestPeer::queue_call(harness.loop(), PreVoteCall{});
    auto release = Defer([&] { gate.release(); });
    harness.loop().start();
    ASSERT_TRUE(gate.wait_until_arrived());

    EXPECT_EQ(proposal->wait_for(Milliseconds{10}, Status::NOT_YET_COMMIT("queued")).status.code(),
              StatusCode::NOT_YET_COMMIT);
    ASSERT_EQ(proposal->state(), CompletionState::TimedOut);

    gate.release();
    EXPECT_TRUE(blocker->wait_for(Milliseconds{2000}, Status::ERROR("test timeout")).status.ok());
    EXPECT_TRUE(after->wait_for(Milliseconds{2000}, Status::ERROR("test timeout")).status.ok());

    // 终态是超时，迟到的失败结果和随后的成功都送不进去。
    EXPECT_EQ(proposal->state(), CompletionState::TimedOut);
    EXPECT_EQ(proposal->complete(CompletionResult<void>::ok()), CompleteStatus::TimedOut);
}

TEST(ReplicaLoopTest, FaultPreservesCurrentErrorAndFailsQueuedCaller) {
    ReplicaLoop* loop_ptr = nullptr;
    LoopHarness harness([&] { loop_ptr->enter_fault(); });
    loop_ptr = &harness.loop();
    // follower 的成员变更先产生 NOT_LEADER，再在发布阶段触发故障。
    // 当前请求应保留原始错误，排队请求才收到统一的 IS_FAULTED。
    auto current = ReplicaLoopTestPeer::queue_call(
            *loop_ptr, AddMemberCall{PeerMember{"other", ReplicaID{301, 4, 1}, Endpoint{"127.0.0.1", 19091}}});
    auto queued = ReplicaLoopTestPeer::queue_call(*loop_ptr, PreVoteCall{});
    loop_ptr->start();
    EXPECT_EQ(current->wait_for(Milliseconds{2000}, Status::ERROR("test timeout")).status.code(),
              StatusCode::NOT_LEADER);
    EXPECT_EQ(queued->wait_for(Milliseconds{2000}, Status::ERROR("test timeout")).status.code(),
              StatusCode::IS_FAULTED);
}

TEST(ReplicaLoopTest, FaultRejectsCurrentSuccessWithoutPayload) {
    ReplicaLoop* loop_ptr = nullptr;
    LoopHarness harness([&] { loop_ptr->enter_fault(); });
    loop_ptr = &harness.loop();
    auto current = ReplicaLoopTestPeer::queue_call(*loop_ptr, PreVoteCall{});
    loop_ptr->start();
    auto result = current->wait_for(Milliseconds{2000}, Status::ERROR("test timeout"));
    EXPECT_EQ(result.status.code(), StatusCode::IS_FAULTED);
    EXPECT_FALSE(result.value.has_value());
}

TEST(ReplicaLoopTest, ReadCallOnFollowerFailsFast) {
    LoopHarness harness;
    harness.loop().start();
    auto result = harness.loop().submit_and_wait(SubmitReadCall{"missing"}, WaitPolicy::read(Milliseconds{2000}));
    EXPECT_EQ(result.status.code(), StatusCode::NOT_LEADER);
    EXPECT_FALSE(result.value.has_value());
}

}  // namespace
}  // namespace adviskv::storage
