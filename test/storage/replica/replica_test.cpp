#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>

#include "storage/model/param.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/state_machine/kv_state_machine.h"
#include "storage/replica/replica_manager.h"
#include "test/test_env.h"

namespace fs = std::filesystem;

namespace adviskv::storage {
namespace {

class ReplicaTest : public ::testing::Test {
   protected:
    void SetUp() override {
        base_dir_ = adviskv::test::make_unique_test_dir("replica", sequence_++);
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
            Endpoint{"127.0.0.1", 19090},
            {PeerMember{"node-1", replica_id_, Endpoint{"127.0.0.1", 19090}}},
            base_dir_.string(),
        };
    }

    Replica* add_single_replica(ReplicaManager& manager) const {
        Status status = manager.add_replica(make_param());
        EXPECT_TRUE(status.ok()) << test::status_debug_string(status);
        if (!status.ok()) {
            return nullptr;
        }
        return manager.get_replica_by_id(replica_id_);
    }

    Replica* wait_until_leader(ReplicaManager& manager,
                               std::chrono::milliseconds timeout =
                                   std::chrono::milliseconds(1500)) const {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            Replica* replica = manager.get_replica_by_id(replica_id_);
            if (replica && replica->get_role() == ReplicaRole::LEADER) {
                return replica;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return manager.get_replica_by_id(replica_id_);
    }

    static LogEntry make_entry(Term term, LogIndex index, WriteOpType op_type,
                               std::string key, std::string value) {
        return LogEntry{term, index, op_type, std::move(key), std::move(value)};
    }

    static inline int sequence_{0};

    fs::path base_dir_;
    ReplicaID replica_id_{201, 3, 0};
};

// 通过流式InstallSnapshot安装快照后，replica应能选举为leader并读取快照中的数据
TEST_F(ReplicaTest, HandleInstallSnapshotUpdatesReadableState) {
    ReplicaManager manager(base_dir_.string());
    Replica* replica = add_single_replica(manager);
    ASSERT_NE(replica, nullptr);

    auto source_dir = base_dir_ / "source";
    ASSERT_TRUE(fs::create_directories(source_dir)) << source_dir.string();
    ReplicaID source_id{201, 3, 1};
    PersistEngine source_persist(source_dir.string(), source_id);
    ASSERT_TRUE(source_persist.init().ok());

    KvStateMachine source_state(EngineType::MAP);
    ASSERT_TRUE(
        source_state.apply(LogEntry{7, 5, WriteOpType::PUT, "hello", "world"})
            .ok());
    ASSERT_TRUE(
        source_state.apply(LogEntry{7, 6, WriteOpType::PUT, "foo", "bar"})
            .ok());
    ASSERT_TRUE(source_persist.do_snapshot(source_state).ok());

    uint64 offset = 0;
    Status status = Status::OK();
    while (true) {
        std::string data;
        bool eof = false;
        status = source_persist.read_snapshot_chunk(offset, 8, data, eof);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

        InstallSnapshotParam param{source_id,
                                   replica_id_,
                                   8,
                                   source_state.apply_index(),
                                   source_state.apply_term(),
                                   offset,
                                   data,
                                   eof};

        status = replica->handle_install_snapshot(param);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        if (eof) break;
        offset += data.size();
    }

    manager.start_tick();
    replica = wait_until_leader(manager);
    ASSERT_NE(replica, nullptr);
    ASSERT_EQ(replica->get_role(), ReplicaRole::LEADER);

    Value value;
    status = replica->get(GetParam{"hello"}, value);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(value, "world");
}

// 单节点replica选举为leader后，put写入的数据应能通过get读回
TEST_F(ReplicaTest, SingleReplicaPutAndGetAfterElection) {
    ReplicaManager manager(base_dir_.string());
    Replica* replica = add_single_replica(manager);
    ASSERT_NE(replica, nullptr);

    manager.start_tick();
    replica = wait_until_leader(manager);
    ASSERT_NE(replica, nullptr);
    ASSERT_EQ(replica->get_role(), ReplicaRole::LEADER);

    Status status = replica->put(PutParam{"k1", "v1"});
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    Value value;
    status = replica->get(GetParam{"k1"}, value);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(value, "v1");
}

// 单节点replica正常写入超过snapshot阈值后，通过后台tick自然触发snapshot。
// 这个测试不手动调用do_snapshot/truncate_log，覆盖Replica::try_take_snapshot的真实链路。
TEST_F(ReplicaTest, TakesSnapshotNaturallyAfterEnoughAppliedLogs) {
    ReplicaManager manager(base_dir_.string());
    Replica* replica = add_single_replica(manager);
    ASSERT_NE(replica, nullptr);

    manager.start_tick();
    replica = wait_until_leader(manager);
    ASSERT_NE(replica, nullptr);
    ASSERT_EQ(replica->get_role(), ReplicaRole::LEADER);

    for (int i = 0; i < 1000; ++i) {
        Status status = replica->put(PutParam{
            .key = "snapshot-auto-" + std::to_string(i),
            .value = "value-" + std::to_string(i),
        });
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status)
                                 << ", index=" << i;
    }

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(3);
    Replica::ReplicaStateForTest state{};
    while (std::chrono::steady_clock::now() < deadline) {
        Status status = replica->get_replica_state_for_test(state);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        if (state.snapshot_index > 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    EXPECT_GT(state.snapshot_index, 0);
    EXPECT_GE(state.last_applied, state.snapshot_index);

    Value value;
    Status status =
        replica->get(GetParam{.key = "snapshot-auto-999"}, value);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(value, "value-999");
}

// replica写入数据后关闭，重新创建replica并recover，应能恢复之前写入的数据
TEST_F(ReplicaTest, RecoverRestoresDataFromPersistedState) {
    {
        ReplicaManager manager(base_dir_.string());
        Replica* replica = add_single_replica(manager);
        ASSERT_NE(replica, nullptr);

        manager.start_tick();
        replica = wait_until_leader(manager);
        ASSERT_NE(replica, nullptr);
        ASSERT_EQ(replica->get_role(), ReplicaRole::LEADER);

        Status status = replica->put(PutParam{"persisted", "v"});
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    }

    ReplicaManager recovered_manager(base_dir_.string());
    Replica* recovered_replica = add_single_replica(recovered_manager);
    ASSERT_NE(recovered_replica, nullptr);

    recovered_manager.recover();
    recovered_manager.start_tick();
    recovered_replica = wait_until_leader(recovered_manager);
    ASSERT_NE(recovered_replica, nullptr);
    ASSERT_EQ(recovered_replica->get_role(), ReplicaRole::LEADER);

    Value value;
    Status status = recovered_replica->get(GetParam{"persisted"}, value);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(value, "v");
}

// WAL损坏导致已提交日志缺失，recover后进入recovering，期间get/put应被拒绝；
// leader通过AppendEntries补齐entries后应退出recovering并恢复正常读写
TEST_F(ReplicaTest, WalCatchupRecoveryRejectsRequestsUntilEntriesApplied) {
    {
        PersistEngine persist(base_dir_.string(), replica_id_);
        Status status = persist.init();
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

        status = persist.append_wal_batch({
            make_entry(1, 1, WriteOpType::PUT, "k1", "v1"),
            make_entry(1, 2, WriteOpType::PUT, "k2", "v2"),
        });
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = persist.save_raft_meta(RaftMeta{1, 3, std::nullopt});
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        ASSERT_TRUE(persist.close().ok());
    }

    const fs::path wal_path = base_dir_ /
                              (std::to_string(replica_id_.table_id) + "-" +
                               std::to_string(replica_id_.shard_index)) /
                              "wal.log";
    std::ofstream wal(wal_path, std::ios::binary | std::ios::app);
    ASSERT_TRUE(wal.is_open()) << wal_path.string();
    wal.put('\x01');
    wal.close();

    ReplicaManager manager(base_dir_.string());
    Replica* replica = add_single_replica(manager);
    ASSERT_NE(replica, nullptr);

    manager.recover();
    EXPECT_TRUE(replica->is_recovering());

    Value value;
    Status status = replica->get(GetParam{"k1"}, value);
    EXPECT_EQ(status.code(), StatusCode::ERROR);
    status = replica->put(PutParam{"blocked", "value"});
    EXPECT_EQ(status.code(), StatusCode::ERROR);

    AppendEntriesResult result;
    status = replica->handle_append_entries(
        AppendEntriesParam{ReplicaID{201, 3, 1},
                           replica_id_,
                           2,
                           {make_entry(2, 3, WriteOpType::PUT, "k3", "v3")},
                           2,
                           1,
                           3},
        result);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_TRUE(result.success);
    EXPECT_FALSE(replica->is_recovering());

    manager.start_tick();
    replica = wait_until_leader(manager);
    ASSERT_NE(replica, nullptr);
    ASSERT_EQ(replica->get_role(), ReplicaRole::LEADER);

    status = replica->get(GetParam{"k3"}, value);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(value, "v3");
}

// WAL损坏导致已提交日志缺失，recover后进入recovering；
// leader通过InstallSnapshot发送快照覆盖recovery_target后应退出recovering并恢复正常读写
TEST_F(ReplicaTest, SnapshotCatchupRecoveryFinishesWhenSnapshotCoversTarget) {
    {
        PersistEngine persist(base_dir_.string(), replica_id_);
        Status status = persist.init();
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

        status = persist.append_wal_batch({
            make_entry(1, 1, WriteOpType::PUT, "k1", "v1"),
            make_entry(1, 2, WriteOpType::PUT, "k2", "v2"),
        });
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = persist.save_raft_meta(RaftMeta{1, 6, std::nullopt});
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        ASSERT_TRUE(persist.close().ok());
    }

    const fs::path wal_path = base_dir_ /
                              (std::to_string(replica_id_.table_id) + "-" +
                               std::to_string(replica_id_.shard_index)) /
                              "wal.log";
    std::ofstream wal(wal_path, std::ios::binary | std::ios::app);
    ASSERT_TRUE(wal.is_open()) << wal_path.string();
    wal.put('\x01');
    wal.close();

    ReplicaManager manager(base_dir_.string());
    Replica* replica = add_single_replica(manager);
    ASSERT_NE(replica, nullptr);

    manager.recover();
    ASSERT_TRUE(replica->is_recovering());
    EXPECT_EQ(replica->get_status(), ReplicaStatus::ADDING);

    auto source_dir = base_dir_ / "snapshot_source";
    ASSERT_TRUE(fs::create_directories(source_dir)) << source_dir.string();
    ReplicaID source_id{201, 3, 1};
    PersistEngine source_persist(source_dir.string(), source_id);
    ASSERT_TRUE(source_persist.init().ok());

    KvStateMachine source_state(EngineType::MAP);
    Status status = Status::OK();
    for (LogIndex index = 1; index <= 6; ++index) {
        status = source_state.apply(make_entry(
            2, index, WriteOpType::PUT, "snap-" + std::to_string(index),
            "value-" + std::to_string(index)));
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    }
    ASSERT_TRUE(source_persist.do_snapshot(source_state).ok());

    uint64 offset = 0;
    while (true) {
        std::string data;
        bool eof = false;
        status = source_persist.read_snapshot_chunk(offset, 7, data, eof);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

        status = replica->handle_install_snapshot(InstallSnapshotParam{
            source_id, replica_id_, 8, source_state.apply_index(),
            source_state.apply_term(), offset, data, eof});
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        if (eof) break;
        offset += data.size();
    }

    EXPECT_FALSE(replica->is_recovering());
    EXPECT_EQ(replica->get_status(), ReplicaStatus::READY);

    manager.start_tick();
    replica = wait_until_leader(manager);
    ASSERT_NE(replica, nullptr);
    ASSERT_EQ(replica->get_role(), ReplicaRole::LEADER);

    Value value;
    status = replica->get(GetParam{"snap-6"}, value);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(value, "value-6");
}

}  // namespace
}  // namespace adviskv::storage