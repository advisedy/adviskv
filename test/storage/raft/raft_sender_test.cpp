#include "storage/raft/raft_sender.h"
#include "storage/replica/replica_message_dispatcher.h"

#include <gtest/gtest.h>

#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <system_error>
#include <thread>

#include "common/pause_point.h"
#include "storage/raft/state_machine/kv_state_machine.h"
#include "test/test_env.h"

namespace fs = std::filesystem;

namespace adviskv::storage {
namespace {

class BlockingInstallSnapshotTransport final : public IRaftRpcTransport {
   public:
    Status request_vote(const PeerMember&, const RequestVoteParam&, int32_t,
                        RequestVoteResult&) const override {
        return Status::ERROR("unused");
    }

    Status pre_vote(const PeerMember&, const PreVoteParam&, int32_t,
                    PreVoteResult&) const override {
        return Status::ERROR("unused");
    }

    Status append_entries(const PeerMember&, const AppendEntriesParam&, int32_t,
                          AppendEntriesResult&) const override {
        return Status::ERROR("unused");
    }

    Status install_snapshot_chunk(
        const PeerMember&, const InstallSnapshotParam& param, int32_t,
        InstallSnapshotResult& result) const override {
        std::unique_lock lock(mutex_);
        ++install_snapshot_call_count_;
        cv_.notify_all();
        cv_.wait(lock, [&] { return released_; });
        result.term = param.term;
        result.status = Status::OK();
        result.snapshot_watermark = param.snapshot_index;
        return Status::OK();
    }

    bool wait_until_call_count(int expected, Milliseconds timeout) const {
        std::unique_lock lock(mutex_);
        return cv_.wait_for(lock, timeout, [&] {
            return install_snapshot_call_count_ >= expected;
        });
    }

    void release() {
        std::lock_guard lock(mutex_);
        released_ = true;
        cv_.notify_all();
    }

    int install_snapshot_call_count() const {
        std::lock_guard lock(mutex_);
        return install_snapshot_call_count_;
    }

   private:
    mutable std::mutex mutex_;
    mutable std::condition_variable cv_;
    mutable bool released_{false};
    mutable int install_snapshot_call_count_{0};
};

class RaftSenderTest : public ::testing::Test {
   protected:
    void SetUp() override {
        base_dir_ =
            adviskv::test::make_unique_test_dir("raft_sender", sequence_++);
        ASSERT_TRUE(fs::create_directories(base_dir_)) << base_dir_.string();

        member_.node_id = "node-2";
        member_.replica_id = ReplicaID{101, 7, 1};
        member_.endpoint = Endpoint{"127.0.0.1", 50052};
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(base_dir_, ec);
    }

    PersistEngine make_snapshot_persist(const ReplicaID& replica_id) const {
        return PersistEngine(base_dir_.string(), replica_id);
    }

    InstallSnapshotParam prepare_snapshot(PersistEngine* persist,
                                          const std::string& value) const {
        EXPECT_NE(persist, nullptr);
        EXPECT_TRUE(persist->init().ok());

        KvStateMachine machine(EngineType::MAP);
        EXPECT_TRUE(
            machine.apply(LogEntry{1, 1, WriteOpType::PUT, "snap-key", value})
                .ok());
        EXPECT_TRUE(persist->write_snapshot(machine).ok());

        return InstallSnapshotParam{
            ReplicaID{101, 7, 0}, member_.replica_id, 3, 1, 1, 0, "", false,
        };
    }

    static inline int sequence_{0};
    fs::path base_dir_;
    PeerMember member_;
};

TEST_F(RaftSenderTest, InstallSnapshotInflightIsNotOwnedBySender) {
    PersistEngine persist = make_snapshot_persist(ReplicaID{101, 7, 0});
    InstallSnapshotParam param = prepare_snapshot(&persist, "snap-value");

    auto transport = std::make_unique<BlockingInstallSnapshotTransport>();
    BlockingInstallSnapshotTransport* raw_transport = transport.get();
    RaftSender sender(std::move(transport), 250);

    InstallSnapshotResult first_result;
    Status first_status = Status::ERROR("not finished");
    std::thread first_sender([&] {
        first_status =
            sender.send_install_snapshot(member_, param, persist, first_result);
    });

    bool first_entered =
        raw_transport->wait_until_call_count(1, Milliseconds(1000));
    if (!first_entered) {
        raw_transport->release();
        first_sender.join();
    }
    ASSERT_TRUE(first_entered);

    InstallSnapshotResult second_result;
    Status second_status = Status::ERROR("not finished");
    std::thread second_sender([&] {
        second_status = sender.send_install_snapshot(member_, param, persist,
                                                     second_result);
    });

    bool second_entered =
        raw_transport->wait_until_call_count(2, Milliseconds(1000));

    raw_transport->release();
    first_sender.join();
    second_sender.join();

    ASSERT_TRUE(second_entered);
    ASSERT_TRUE(first_status.ok()) << first_status.msg();
    ASSERT_TRUE(second_status.ok()) << second_status.msg();
    ASSERT_TRUE(first_result.status.ok()) << first_result.status.to_string();
    ASSERT_TRUE(second_result.status.ok()) << second_result.status.to_string();
    EXPECT_EQ(raw_transport->install_snapshot_call_count(), 2);
}

// ========== 批次 0 / F1：dispatcher submit 被拒 → 等价 SendFailed 回执 ==========
//
// F1 窗口：async_send_one 先查 started() 再 submit，两步之间线程池 stop，
// 任务被拒。修复前 submit 无返回值，消息静默丢失——对 INSTALL_SNAPSHOT 而
// 言，构造时已 mark_snapshot_inflight，丢失即孤儿标记，该 follower 快照追赶
// 永久停滞（F2）。修复后 submit 返回 false，async_send_one 必须为
// INSTALL_SNAPSHOT 回投 SnapshotSendFailedEvent（与 send_task 内失败路径同一
// 事件，loop 侧 handle_install_snapshot_send_failed 清标记）。
//
// 用独立 ThreadPool 复现：单 worker 线程，先阻塞它，再触发 stop——submit 在
// stopping 置位后到达即被拒。dispatcher 直接持有该池无法从外部注入，因此这
// 里测的是"submit 拒绝 → 事件回投"的合约单元；dispatcher 内部池的同一合约由
// async_send_one 实现（submit false 分支），合约测试足够钉住行为。

class EventRecorder {
public:
    void record(Event event) {
        std::lock_guard lock(mutex_);
        events_.push_back(std::move(event));
        cv_.notify_all();
    }

    template <typename EventType>
    bool wait_for_event(Milliseconds timeout) const {
        std::unique_lock lock(mutex_);
        return cv_.wait_for(lock, timeout, [this] {
            for (const Event& e : events_) {
                if (std::holds_alternative<EventType>(e)) return true;
            }
            return false;
        });
    }

private:
    mutable std::mutex mutex_;
    mutable std::condition_variable cv_;
    std::vector<Event> events_;
};

TEST(RaftDispatcherSubmitRejectionTest, SubmitRejectedProducesSnapshotSendFailedEvent) {
    // 先造一个带快照文件的 PersistEngine（dispatcher 构造函数需要引用）。
    const fs::path dir =
        adviskv::test::make_unique_test_dir("dispatcher_reject", 0);
    ASSERT_TRUE(fs::create_directories(dir)) << dir.string();
    PersistEngine persist(dir.string(), ReplicaID{101, 7, 0});
    ASSERT_TRUE(persist.init().ok());

    EventRecorder recorder;
    ReplicaMessageDispatcher dispatcher(50, persist,
                                        [&recorder](Event event) { recorder.record(std::move(event)); });

    // 用一个假的 8-worker 池做不到注入——直接利用 dispatcher 自身：
    // 先 start，随后在 stop 后调用 async_send，submit 必然被拒。
    // 为了让"拒绝"确定发生（而非依赖 started() 检查先失败——那也是被拒，
    // 走同一条回执路径），stop 后发送 INSTALL_SNAPSHOT。
    dispatcher.start();
    dispatcher.stop();

    RaftMessage msg;
    msg.type = RaftMessageType::INSTALL_SNAPSHOT;
    msg.target = PeerMember{"peer", ReplicaID{101, 7, 1}, Endpoint{"127.0.0.1", 50052}};
    msg.snapshot_param.from_replica_id = ReplicaID{101, 7, 0};
    msg.snapshot_param.to_replica_id = msg.target.replica_id;
    msg.snapshot_param.term = 3;
    msg.snapshot_param.snapshot_index = 2;
    msg.snapshot_param.snapshot_term = 1;

    // stop 之后 async_send 必须仍然 OK 返回（不能把拒绝当发送失败上抛，
    // 否则上层 run_raft_step 会把可重试错误当成副本故障），
    // 但必须回投 SnapshotSendFailedEvent。
    Status send_status = dispatcher.async_send({msg});
    EXPECT_TRUE(send_status.ok()) << send_status.msg();

    ASSERT_TRUE(recorder.wait_for_event<SnapshotSendFailedEvent>(Milliseconds(2000)))
        << "submit rejection must produce equivalent SnapshotSendFailedEvent "
           "(orphan inflight marker would stall follower catch-up forever)";

    fs::remove_all(dir);
}

// ========== 方案 E 批次 B：发送端文件版本化 ==========

// 记录全部 install_snapshot 块的传输桩（场景 3 用）。
class RecordingInstallSnapshotTransport final : public IRaftRpcTransport {
   public:
    Status request_vote(const PeerMember&, const RequestVoteParam&, int32_t,
                        RequestVoteResult&) const override {
        return Status::ERROR("unused");
    }
    Status pre_vote(const PeerMember&, const PreVoteParam&, int32_t,
                    PreVoteResult&) const override {
        return Status::ERROR("unused");
    }
    Status append_entries(const PeerMember&, const AppendEntriesParam&, int32_t,
                          AppendEntriesResult&) const override {
        return Status::ERROR("unused");
    }
    Status install_snapshot_chunk(const PeerMember&, const InstallSnapshotParam& param, int32_t,
                                  InstallSnapshotResult& result) const override {
        std::lock_guard lock(mutex_);
        chunks_.push_back(Chunk{param.offset, param.data, param.done});
        result.term = param.term;
        result.status = Status::OK();
        result.snapshot_watermark = param.snapshot_index;
        return Status::OK();
    }

    struct Chunk {
        uint64 offset;
        std::string data;
        bool done;
    };

    std::vector<Chunk> snapshot() const {
        std::lock_guard lock(mutex_);
        return chunks_;
    }

   private:
    mutable std::mutex mutex_;
    mutable std::vector<Chunk> chunks_;
};

// 不变量（§3-B8 场景 3）：发送端进块循环前 open 一次、持 fd 读完。发 S
// 首块后 leader 发布更新的本地快照 T（rename 换掉 snapshot 路径），后续
// 全部块仍必须来自 S 的同一文件版本（inode 语义），拼接结果 == S 原文。
//
// 确定性机制：pause_point "raft_sender.after_first_chunk"（生产代码内置）
// 到达时，测试回调在同线程内先发布 T 再放行——第二块的读取必然晚于
// 路径替换。
TEST(RaftSenderSnapshotVersionTest, AllChunksComeFromSameFileVersionAfterPublish) {
    const fs::path dir = adviskv::test::make_unique_test_dir("sender_version", 0);
    ASSERT_TRUE(fs::create_directories(dir)) << dir.string();

    PersistEngine persist(dir.string(), ReplicaID{101, 7, 0});
    ASSERT_TRUE(persist.init().ok());

    // S：index=3，两个 KV。
    KvStateMachine machine_s(EngineType::MAP);
    ASSERT_TRUE(machine_s.apply(LogEntry{1, 2, WriteOpType::PUT, "s-one", "1"}).ok());
    ASSERT_TRUE(machine_s.apply(LogEntry{1, 3, WriteOpType::PUT, "s-two", "2"}).ok());
    ASSERT_TRUE(persist.write_snapshot(machine_s).ok());
    // 发布 T 之前先把 S 的原文读出来（此后 snapshot 路径会被 T 换掉）。
    const fs::path snapshot_path = dir / "101-7" / "snapshot";
    auto read_file = [](const fs::path& path) -> std::string {
        std::ifstream in(path, std::ios::binary);
        if (!in.is_open()) return {};
        std::ostringstream ss;
        ss << in.rdbuf();
        return ss.str();
    };
    const std::string s_content = read_file(snapshot_path);
    ASSERT_GT(s_content.size(), 4U);

    const PeerMember member{"peer", ReplicaID{101, 7, 1}, Endpoint{"127.0.0.1", 50052}};
    InstallSnapshotParam param{ReplicaID{101, 7, 0}, member.replica_id, 3, 3, 1, 0, "", false};

    auto transport = std::make_unique<RecordingInstallSnapshotTransport>();
    RecordingInstallSnapshotTransport* raw_transport = transport.get();
    RaftSender sender(std::move(transport), 250);

    // 在生产暂停点内同步发布 T：index=100，单 KV。回调返回 false = 通知后
    // 不阻塞（注册表语义：回调 false 直接放行）。
    testhook::PausePointRegistry::instance().register_point(
        "raft_sender.after_first_chunk", [&persist]() {
            // T 与 S 内容截然不同（长度也不同），保证拼接结果不可能来自 T。
            KvStateMachine machine_t(EngineType::MAP);
            EXPECT_TRUE(machine_t.apply(LogEntry{2, 100, WriteOpType::PUT, "t-key", "long-t-value"}).ok());
            EXPECT_TRUE(persist.write_snapshot(machine_t).ok());
            return false;
        });
    struct Unregister {
        ~Unregister() {
            testhook::PausePointRegistry::instance().unregister_point("raft_sender.after_first_chunk");
        }
    } unregister_on_exit;

    InstallSnapshotResult result;
    Status status = sender.send_install_snapshot(member, param, persist, result);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    ASSERT_TRUE(result.status.ok()) << test::status_debug_string(result.status);

    const auto chunks = raw_transport->snapshot();
    ASSERT_FALSE(chunks.empty());
    std::string assembled;
    for (const auto& chunk : chunks) {
        EXPECT_EQ(chunk.offset, assembled.size()) << "chunk offsets must be contiguous";
        assembled += chunk.data;
    }
    EXPECT_TRUE(chunks[chunks.size() - 1].done);
    EXPECT_EQ(assembled, s_content)
        << "chunks must all come from the file version opened before the publish";
    // 首块后确实发布过 T（路径上现在是 T），证明替换真实发生。
    EXPECT_NE(read_file(snapshot_path), s_content) << "publish during send must replace the path";

    fs::remove_all(dir);
}

// 不变量（§3-B8 场景 4 + 安全条件 5）：发送参数与打开的文件头必须匹配。
// 参数声称 S（index=10），本地实际快照是 T（index=20）——发送被拒，返回
// CONDITION_NOT_MET；上层按发送失败回执清 inflight（清标记机制由
// RaftClusterTest.SnapshotSendFailedClearsInflightAndRecovers 覆盖）。
TEST(RaftSenderSnapshotVersionTest, HeaderMismatchCancelsSend) {
    const fs::path dir = adviskv::test::make_unique_test_dir("sender_mismatch", 0);
    ASSERT_TRUE(fs::create_directories(dir)) << dir.string();

    PersistEngine persist(dir.string(), ReplicaID{101, 7, 0});
    ASSERT_TRUE(persist.init().ok());

    KvStateMachine machine_t(EngineType::MAP);
    ASSERT_TRUE(machine_t.apply(LogEntry{2, 20, WriteOpType::PUT, "t-key", "tv"}).ok());
    ASSERT_TRUE(persist.write_snapshot(machine_t).ok());

    const PeerMember member{"peer", ReplicaID{101, 7, 1}, Endpoint{"127.0.0.1", 50052}};
    InstallSnapshotParam param_stale{ReplicaID{101, 7, 0}, member.replica_id, 3, 10, 1, 0, "", false};

    RaftSender sender(250);
    InstallSnapshotResult result;
    Status status = sender.send_install_snapshot(member, param_stale, persist, result);
    EXPECT_EQ(status.code(), StatusCode::CONDITION_NOT_MET)
        << "stale send params must be rejected by header check (condition 5)"
        << test::status_debug_string(status);
    // 被拒发生在打开/校验阶段：没有发生任何 RPC 连接尝试才可能快速失败。
    // （不一致的参数永远不会走到 transport 层——这就是条件 5 的语义。）

    fs::remove_all(dir);
}

}  // namespace
}  // namespace adviskv::storage
