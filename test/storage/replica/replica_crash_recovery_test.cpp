#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <ostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <unistd.h>

#include "storage/persist/persist_engine.h"
#include "storage/raft/state_machine/kv_state_machine.h"
#include "storage/replica/replica_manager.h"
#include "test/test_env.h"

namespace adviskv::storage {
namespace {

namespace fs = std::filesystem;

class ReplicaCrashRecoveryTest : public ::testing::Test {
protected:
    void SetUp() override {
        base_dir_ = adviskv::test::make_unique_test_dir("replica_crash_recovery", sequence_++);
        ASSERT_TRUE(fs::create_directories(base_dir_)) << base_dir_.string();
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(base_dir_, ec);
    }

    ReplicaInitParam make_param() const {
        return ReplicaInitParam{
                replica_id_,
                EngineType::MAP,
                Endpoint{"127.0.0.1", 19191},
                {PeerMember{"node-crash", replica_id_, Endpoint{"127.0.0.1", 19191}}},
                ReplicaRuntimeOptions{base_dir_.string(), 1000},
        };
    }

    ReplicaPtr wait_until_leader(ReplicaManager& manager,
                                 std::chrono::milliseconds timeout = std::chrono::milliseconds(2500)) const {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            ReplicaPtr replica = manager.get_replica_by_id(replica_id_);
            if (replica && replica->get_role() == ReplicaRole::LEADER) return replica;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return manager.get_replica_by_id(replica_id_);
    }

    bool wait_until_applied(const ReplicaPtr& replica, LogIndex index,
                            std::chrono::milliseconds timeout = std::chrono::milliseconds(2500)) const {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            Replica::ReplicaStateForTest state{};
            if (replica && replica->get_replica_state_for_test(state).ok() && state.last_applied >= index) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        return false;
    }

    void create_snapshot_image(std::string& data) const {
        const fs::path source_dir = base_dir_ / "snapshot-source";
        ASSERT_TRUE(fs::create_directories(source_dir)) << source_dir.string();
        PersistEngine source(source_dir.string(), ReplicaID{replica_id_.table_id, replica_id_.shard_index, 9});
        ASSERT_TRUE(source.init().ok());

        KvStateMachine state(EngineType::MAP);
        for (LogIndex index = 1; index <= 5; ++index) {
            ASSERT_TRUE(state.apply(LogEntry{2, index, WriteOpType::PUT, "snapshot-key-" + std::to_string(index),
                                             "snapshot-value-" + std::to_string(index)})
                                .ok());
        }
        const std::vector<RaftMember> members{{make_param().members.front(), RaftMemberType::VOTER}};
        ASSERT_TRUE(source.write_snapshot(state, members).ok());
        bool eof = false;
        ASSERT_TRUE(source.read_snapshot_chunk(0, 1024 * 1024, data, eof).ok());
        ASSERT_TRUE(eof);
    }

    void assert_recovered_value(const Key& key, const Value& expected) const {
        ReplicaManager recovered(base_dir_.string());
        recovered.recover();
        ReplicaPtr replica = recovered.get_replica_by_id(replica_id_);
        ASSERT_NE(replica, nullptr);
        recovered.start_tick();
        replica = wait_until_leader(recovered);
        ASSERT_NE(replica, nullptr);
        ASSERT_EQ(replica->get_role(), ReplicaRole::LEADER);

        Value actual;
        Status status = replica->get(GetParam{key}, actual);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        EXPECT_EQ(actual, expected);
    }

    static inline int sequence_{0};
    fs::path base_dir_;
    ReplicaID replica_id_{920, 2, 0};
};

// 场景：单节点 Replica 已当选 leader 并完成 no-op apply，随后接收一条客户端写入。
// 过程：Raft 日志持久化完成、但尚未发送/完成 apply 时杀死进程，再从同一目录重建 Replica。
// 预期：重启会重放已持久化的 committed WAL，客户端写入最终可读且值不丢失。
TEST_F(ReplicaCrashRecoveryTest, CommittedWalReplaysAfterCrashBeforeApply) {
    ASSERT_EXIT(
            {
                ReplicaManager manager(base_dir_.string());
                if (manager.add_replica(make_param()).fail()) ::_exit(2);
                manager.start_tick();
                ReplicaPtr replica = wait_until_leader(manager);
                if (!replica || replica->get_role() != ReplicaRole::LEADER) ::_exit(3);
                if (!wait_until_applied(replica, 1)) ::_exit(4);
                if (::setenv("ADVISKV_ENABLE_CRASH_POINT", "replica.raft_step.after_persist_before_send", 1) != 0) {
                    ::_exit(5);
                }
                Status status = replica->put(PutParam{"commit-crash", "durable"});
                ::_exit(status.ok() ? 6 : 7);
            },
            ::testing::ExitedWithCode(137), "");

    assert_recovered_value("commit-crash", "durable");
}

// 场景：单节点 Replica 提交写入，状态机已经执行该 entry，但 applied progress 尚未持久化推进。
// 过程：在两者之间的 crash point 杀死进程，再用原目录恢复并重新成为 leader。
// 预期：恢复过程可安全重放该 entry，最终值正确，不因 progress 落后而丢写。
TEST_F(ReplicaCrashRecoveryTest, ApplyReplaysAfterCrashBeforeProgressAdvance) {
    ASSERT_EXIT(
            {
                ReplicaManager manager(base_dir_.string());
                if (manager.add_replica(make_param()).fail()) ::_exit(2);
                manager.start_tick();
                ReplicaPtr replica = wait_until_leader(manager);
                if (!replica || replica->get_role() != ReplicaRole::LEADER) ::_exit(3);
                if (!wait_until_applied(replica, 1)) ::_exit(4);
                if (::setenv("ADVISKV_ENABLE_CRASH_POINT", "replica.apply.after_state_machine_before_progress", 1) !=
                    0) {
                    ::_exit(5);
                }
                if (replica->put(PutParam{"apply-crash", "replayed"}).fail()) ::_exit(6);
                std::this_thread::sleep_for(std::chrono::seconds(3));
                ::_exit(7);
            },
            ::testing::ExitedWithCode(137), "");

    assert_recovered_value("apply-crash", "replayed");
}

struct InstallSnapshotCrashCase {
    const char* name;
    const char* crash_point;
};

void PrintTo(const InstallSnapshotCrashCase& value, std::ostream* stream) { *stream << value.name; }

class ReplicaInstallSnapshotCrashTest : public ReplicaCrashRecoveryTest,
                                        public ::testing::WithParamInterface<InstallSnapshotCrashCase> {};

// 场景：Replica 接收一份 index=5 的完整 snapshot，镜像包含成员信息和 5 条 KV。
// 过程：分别在 snapshot 持久化后、状态机 restore 后但 Raft publish 前杀死进程并重建。
// 预期：两个边界都能从已发布的持久化镜像恢复，重启后可读到 snapshot 的最后一条 KV。
TEST_P(ReplicaInstallSnapshotCrashTest, RebuildRestoresPublishedSnapshot) {
    std::string snapshot_data;
    create_snapshot_image(snapshot_data);
    const std::string crash_point = GetParam().crash_point;

    ASSERT_EXIT(
            {
                ReplicaManager manager(base_dir_.string());
                if (manager.add_replica(make_param()).fail()) ::_exit(2);
                ReplicaPtr replica = manager.get_replica_by_id(replica_id_);
                if (!replica) ::_exit(3);
                if (::setenv("ADVISKV_ENABLE_CRASH_POINT", crash_point.c_str(), 1) != 0) ::_exit(4);
                Status status = replica->handle_install_snapshot(
                        InstallSnapshotParam{replica_id_, replica_id_, 8, 5, 2, 0, snapshot_data, true});
                ::_exit(status.ok() ? 5 : 6);
            },
            ::testing::ExitedWithCode(137), "");

    assert_recovered_value("snapshot-key-5", "snapshot-value-5");
}

INSTANTIATE_TEST_SUITE_P(
        InstallBoundaries, ReplicaInstallSnapshotCrashTest,
        ::testing::Values(
                InstallSnapshotCrashCase{"BeforeStateRestore", "replica.install_snapshot.after_persist_before_restore"},
                InstallSnapshotCrashCase{"BeforeRaftPublish", "replica.install_snapshot.after_restore_before_raft"}),
        [](const ::testing::TestParamInfo<InstallSnapshotCrashCase>& info) { return info.param.name; });

// 场景：单节点 Replica apply 到本地 snapshot 阈值，最后一次写入触发 snapshot 生成。
// 过程：snapshot 已落盘、但 compact 结果尚未发布给 RaftCore 时杀死进程，再恢复 Replica。
// 预期：重建识别 index=1000 的本地 snapshot，重新选主后最后一次写入仍然可读。
TEST_F(ReplicaCrashRecoveryTest, LocalSnapshotRecoversAfterCrashBeforeRaftPublish) {
    ASSERT_EXIT(
            {
                ReplicaManager manager(base_dir_.string());
                if (manager.add_replica(make_param()).fail()) ::_exit(2);
                manager.start_tick();
                ReplicaPtr replica = wait_until_leader(manager);
                if (!replica || replica->get_role() != ReplicaRole::LEADER) ::_exit(3);
                if (!wait_until_applied(replica, 1)) ::_exit(4);

                for (int i = 0; i < 998; ++i) {
                    if (replica->put(PutParam{"local-snapshot-" + std::to_string(i), "value-" + std::to_string(i)})
                                .fail()) {
                        ::_exit(5);
                    }
                }
                if (!wait_until_applied(replica, 999)) ::_exit(6);
                Replica::ReplicaStateForTest before{};
                if (replica->get_replica_state_for_test(before).fail() || before.snapshot_index != 0) ::_exit(7);
                if (::setenv("ADVISKV_ENABLE_CRASH_POINT", "replica.local_snapshot.after_persist_before_raft", 1) !=
                    0) {
                    ::_exit(8);
                }
                if (replica->put(PutParam{"local-snapshot-final", "final-value"}).fail()) ::_exit(9);
                std::this_thread::sleep_for(std::chrono::seconds(5));
                ::_exit(10);
            },
            ::testing::ExitedWithCode(137), "");

    ReplicaManager recovered(base_dir_.string());
    recovered.recover();
    ReplicaPtr replica = recovered.get_replica_by_id(replica_id_);
    ASSERT_NE(replica, nullptr);
    Replica::ReplicaStateForTest state{};
    ASSERT_TRUE(replica->get_replica_state_for_test(state).ok());
    EXPECT_EQ(state.snapshot_index, 1000);
    recovered.start_tick();
    replica = wait_until_leader(recovered);
    ASSERT_NE(replica, nullptr);

    Value value;
    Status status = replica->get(GetParam{"local-snapshot-final"}, value);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(value, "final-value");
}

}  // namespace
}  // namespace adviskv::storage
