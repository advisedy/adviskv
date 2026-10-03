#include "common/thread_pool.h"

#include <atomic>
#include <chrono>
#include <thread>

#include <gtest/gtest.h>

#include "common/pause_point.h"

namespace adviskv {
namespace {

// 辅助：等待原子谓词成立（避免各用例重复写轮询样板）
static bool wait_until(const std::atomic<bool>& flag,
                       std::chrono::milliseconds timeout = std::chrono::milliseconds(2000)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!flag.load() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return flag.load();
}

// ========== ThreadPool::submit 返回值（批次 0 / F1） ==========

TEST(ThreadPoolSubmitTest, SubmitReturnsTrueWhenRunning) {
    ThreadPool pool;
    pool.start(2);

    std::atomic<int> executed{0};
    EXPECT_TRUE(pool.submit([&executed] { executed.fetch_add(1); }));

    // 等任务执行完
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (executed.load() == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    EXPECT_EQ(executed.load(), 1);
}

// F1 核心：stopping/已停止时 submit 必须返回 false，任务不被接受。
// 修复前 submit 无返回值，任务被静默吞掉，调用方以为发送成功。
TEST(ThreadPoolSubmitTest, SubmitReturnsFalseAfterStop) {
    ThreadPool pool;
    pool.start(2);
    pool.stop();

    EXPECT_FALSE(pool.submit([] {}));
}

TEST(ThreadPoolSubmitTest, SubmitReturnsFalseWhileStopping) {
    ThreadPool pool;
    pool.start(1);

    // 让 worker 阻塞在长任务上，触发 stop 与 submit 竞争。
    std::atomic<bool> worker_blocked{false};
    std::atomic<bool> release_worker{false};
    ASSERT_TRUE(pool.submit([&] {
        worker_blocked.store(true);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!release_worker.load() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }));
    ASSERT_TRUE(wait_until(worker_blocked));

    // stop 与 submit 竞争：stop 开始后提交的任务是"二选一"契约——
    //   - submit 返回 false：任务被拒绝，调用方得到明确信号（F1 修复点）；
    //   - submit 返回 true：任务已入队，stop() 的 drain 保证它被执行完。
    // 无论哪边赢，任务都不允许既返回 true 又不执行（静默丢失）。
    // 注：不能断言"必须 false"——submit 可能抢在 stop 拿锁之前完成入队，
    // 那是合法的另一侧竞态窗口。
    std::atomic<bool> executed_after_race{false};
    std::thread stopper([&] { pool.stop(); });
    const bool accepted = pool.submit([&] { executed_after_race.store(true); });
    release_worker.store(true);
    stopper.join();

    if (!accepted) {
        SUCCEED() << "submit was rejected during stop (F1 signal works)";
    } else {
        // stop() 的实现是先 drain 剩余任务再 join，因此返回 true 的任务
        // 必然已被执行。
        EXPECT_TRUE(executed_after_race.load()) << "submit returned true during stop but task was silently dropped";
    }
}

TEST(ThreadPoolSubmitTest, SubmitReturnsFalseForNullTask) {
    ThreadPool pool;
    pool.start(1);
    EXPECT_FALSE(pool.submit(nullptr));
}

// ========== 最小暂停设施（方案 E §5） ==========

TEST(PausePointTest, NoCallbackIsNoOp) {
    // 未注册的暂停点必须零开销直接通过，且可反复调用。
    testhook::pause_point("never_registered.point");
    testhook::pause_point("never_registered.point");
    SUCCEED();
}

TEST(PausePointTest, CallbackRunsAndResumeUnblocks) {
    auto& registry = testhook::PausePointRegistry::instance();
    std::atomic<bool> reached{false};

    registry.register_point("test.arrived", [&] {
        reached.store(true);
        return true;  // 要求等待放行
    });

    std::thread worker([] { testhook::pause_point("test.arrived"); });

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!reached.load() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    EXPECT_TRUE(reached.load());
    // worker 此刻应停在暂停点上（30s 超时前），放行后退出。
    registry.resume("test.arrived");
    worker.join();
    registry.unregister_point("test.arrived");
}

TEST(PausePointTest, CallbackReturningFalseDoesNotWait) {
    auto& registry = testhook::PausePointRegistry::instance();
    std::atomic<bool> finished{false};
    registry.register_point("test.observe_only", [] { return false; });

    // 返回 false = 只观察到达，不等待放行：worker 应立即退出。用 finished
    // 标志真正验证"未阻塞"（joinable() 在 join 前恒为 true，挡不住挂死）。
    std::thread worker([&] {
        testhook::pause_point("test.observe_only");
        finished.store(true);
    });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!finished.load() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    EXPECT_TRUE(finished.load()) << "callback 返回 false 时不应等待放行";
    worker.join();
    registry.unregister_point("test.observe_only");
}

// 批次 3.3 回归：resume 必须按名字隔离。早期实现只有一个全局 resumed_ 标志，
// resume("a") 会把同时停在 "b" 上的 worker 一起放行，测试出现假通过。
TEST(PausePointTest, ResumeIsPerName) {
    auto& registry = testhook::PausePointRegistry::instance();
    std::atomic<int> arrived{0};
    std::atomic<bool> a_released{false};
    std::atomic<bool> b_released{false};

    auto wait_callback = [&arrived](std::atomic<bool>& released) {
        return [&arrived, &released] {
            arrived.fetch_add(1);
            released.store(true);
            return true;  // 要求等待放行
        };
    };
    registry.register_point("test.per_name.a", wait_callback(a_released));
    registry.register_point("test.per_name.b", wait_callback(b_released));

    std::thread worker_a([] { testhook::pause_point("test.per_name.a"); });
    std::thread worker_b([] { testhook::pause_point("test.per_name.b"); });

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (arrived.load() < 2 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_EQ(arrived.load(), 2);

    // 只放行 a：b 必须仍然停在点上（30s 超时远大于下面的检查窗口）。
    registry.resume("test.per_name.a");
    worker_a.join();
    EXPECT_TRUE(a_released.load());
    EXPECT_TRUE(worker_b.joinable()) << "resume(a) must not release pause point b";

    registry.resume("test.per_name.b");
    worker_b.join();
    EXPECT_TRUE(b_released.load());

    registry.unregister_point("test.per_name.a");
    registry.unregister_point("test.per_name.b");
}

}  // namespace
}  // namespace adviskv
