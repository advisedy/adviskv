#include "storage/persist/persist_engine.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <string>
#include <utility>
#include <vector>

#include "storage/model/param.h"
#include "storage/raft/state_machine/kv_state_machine.h"
#include "storage/raft/state_machine/state_machine.h"
#include "test/test_env.h"

namespace fs = std::filesystem;

namespace adviskv::storage {
namespace {

std::string status_debug_string(const Status& status) {
    return "code=" + std::to_string(static_cast<int>(status.code())) +
           ", msg=" + status.msg();
}
/*
测试内容列表:

//1.
//测试一下append_wal_batch和read_wal_batch是否可以正常运行

//2.
// 测试一下save_raft_meta和load_raft_meta是否可以正常运行

//3.
// 走正常流程，综合检测一下: 
// 放entry。然后状态机应用跑快照，
// 读取快照是否没有问题;
// 跑快照后会截取wal，这个是否没有问题
// persist在截取wal之后剩下的wal，是否没有问题

4.


*/
class PersistEngineTest : public ::testing::Test {
   protected:
    void SetUp() override {
        base_dir_ = adviskv::test::make_unique_test_dir("persist", sequence_++);
        ASSERT_TRUE(fs::create_directories(base_dir_)) << base_dir_.string();
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(base_dir_, ec);
    }

    PersistEngine make_engine() const {
        return PersistEngine(base_dir_.string(), replica_id_);
    }

    static LogEntry make_entry(Term term, LogIndex index, WriteOpType op_type,
                               std::string key, std::string value) {
        return LogEntry{
            .term = term,
            .index = index,
            .op_type = op_type,
            .key = std::move(key),
            .value = std::move(value),
        };
    }

    static inline int sequence_{0};

    fs::path base_dir_;
    ReplicaID replica_id_{
        .table_id = 101, .shard_index = 7, .replica_index = 2};
};

// 测试一下append_wal_batch和read_wal_batch是否可以正常运行
TEST_F(PersistEngineTest, AppendWalBatchAndReadBackEntries) {
    PersistEngine engine = make_engine();
    Status status = engine.init();
    ASSERT_TRUE(status.ok()) << status_debug_string(status);

    const std::vector<LogEntry> expected = {
        make_entry(1, 1, WriteOpType::PUT, "k1", "v1"),
        make_entry(1, 2, WriteOpType::DEL, "k2", ""),
        make_entry(2, 3, WriteOpType::PUT, "k3", "v3"),
    };

    status = engine.append_wal_batch(expected);
    ASSERT_TRUE(status.ok()) << status_debug_string(status);

    std::vector<LogEntry> actual;
    status = engine.read_wal_batch(actual);
    ASSERT_TRUE(status.ok()) << status_debug_string(status);
    ASSERT_EQ(actual.size(), expected.size());

    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(actual[i].term, expected[i].term);
        EXPECT_EQ(actual[i].index, expected[i].index);
        EXPECT_EQ(actual[i].op_type, expected[i].op_type);
        EXPECT_EQ(actual[i].key, expected[i].key);
        EXPECT_EQ(actual[i].value, expected[i].value);
    }
}

// 测试一下save_raft_meta和load_raft_meta是否可以正常运行
TEST_F(PersistEngineTest, SaveAndLoadRaftMeta) {
    PersistEngine engine = make_engine();
    Status status = engine.init();
    ASSERT_TRUE(status.ok()) << status_debug_string(status);

    RaftMeta expected{
        .current_term = 9,
        .commit_index = 17,
        .voted_for =
            ReplicaID{.table_id = 9, .shard_index = 2, .replica_index = 1},
    };

    status = engine.save_raft_meta(expected);
    ASSERT_TRUE(status.ok()) << status_debug_string(status);

    RaftMeta actual{};
    status = engine.load_raft_meta(actual);
    ASSERT_TRUE(status.ok()) << status_debug_string(status);
    EXPECT_EQ(actual.current_term, expected.current_term);
    EXPECT_EQ(actual.commit_index, expected.commit_index);
    ASSERT_TRUE(actual.voted_for.has_value());
    EXPECT_EQ(actual.voted_for.value(), expected.voted_for.value());
}

// MapEngine的状态机应用no-op entry，然后走快照，检测快照内容是否正确
TEST_F(PersistEngineTest, LoadSnapshotMetaWithoutLoadingKvs) {
    PersistEngine engine = make_engine();
    Status status = engine.init();
    ASSERT_TRUE(status.ok()) << status_debug_string(status);

    KvStateMachine state_machine(EngineType::MAP);
    ASSERT_TRUE(
        state_machine.apply(make_entry(4, 12, WriteOpType::NONE, "", "")).ok());
    status = engine.do_snapshot(state_machine);
    ASSERT_TRUE(status.ok()) << status_debug_string(status);

    SnapshotPtr actual = std::make_shared<Snapshot>();
    status = engine.load_snapshot_meta(actual);
    ASSERT_TRUE(status.ok()) << status_debug_string(status);
    EXPECT_EQ(actual->apply_index, 12);
    EXPECT_EQ(actual->apply_term, 4);
    EXPECT_FALSE(actual->path.empty());

    size_t kv_count = 0;
    status = engine.for_each_snapshot_kv(
        [&kv_count](const Key&, const Value&) -> Status {
            ++kv_count;
            return Status::OK();
        });
    ASSERT_TRUE(status.ok()) << status_debug_string(status);
    EXPECT_EQ(kv_count, 0U);
}

// 搞4个entry，然后截取前两个，检测截取wal之后，剩余的进行read_wal_batch是否正确
TEST_F(PersistEngineTest, TruncateWalKeepsEntriesAfterSnapshotIndex) {
    PersistEngine engine = make_engine();
    Status status = engine.init();
    ASSERT_TRUE(status.ok()) << status_debug_string(status);

    const std::vector<LogEntry> entries = {
        make_entry(1, 1, WriteOpType::PUT, "k1", "v1"),
        make_entry(1, 2, WriteOpType::PUT, "k2", "v2"),
        make_entry(2, 3, WriteOpType::DEL, "k1", ""),
        make_entry(2, 4, WriteOpType::PUT, "k3", "v3"),
    };

    status = engine.append_wal_batch(entries);
    ASSERT_TRUE(status.ok()) << status_debug_string(status);

    status = engine.truncate_wal(2);
    ASSERT_TRUE(status.ok()) << status_debug_string(status);

    std::vector<LogEntry> actual;
    status = engine.read_wal_batch(actual);
    ASSERT_TRUE(status.ok()) << status_debug_string(status);
    ASSERT_EQ(actual.size(), 2U);
    EXPECT_EQ(actual[0].index, 3);
    EXPECT_EQ(actual[0].key, "k1");
    EXPECT_EQ(actual[1].index, 4);
    EXPECT_EQ(actual[1].key, "k3");
}

//走正常流程，综合检测一下: 
// 放entry。然后状态机应用跑快照，
// 读取快照是否没有问题;
// 跑快照后会截取wal，这个是否没有问题
// persist在截取wal之后剩下的wal，是否没有问题
TEST_F(PersistEngineTest, DoSnapshotPersistsSnapshotAndTruncatesWal) {
    PersistEngine engine = make_engine();
    Status status = engine.init();
    ASSERT_TRUE(status.ok()) << status_debug_string(status);

    const std::vector<LogEntry> entries = {
        make_entry(3, 11, WriteOpType::PUT, "a", "1"),
        make_entry(3, 12, WriteOpType::PUT, "b", "2"),
        make_entry(4, 13, WriteOpType::PUT, "c", "3"),
    };
    status = engine.append_wal_batch(entries);
    ASSERT_TRUE(status.ok()) << status_debug_string(status);

    // Build a state machine that represents the snapshot state at index=12.
    KvStateMachine state_machine(EngineType::MAP);
    ASSERT_TRUE(
        state_machine.apply(make_entry(3, 11, WriteOpType::PUT, "a", "1"))
            .ok());
    ASSERT_TRUE(
        state_machine.apply(make_entry(3, 12, WriteOpType::PUT, "b", "2"))
            .ok());

    status = engine.do_snapshot(state_machine);
    ASSERT_TRUE(status.ok()) << status_debug_string(status);

    SnapshotPtr loaded_snapshot = std::make_shared<Snapshot>();
    status = engine.load_snapshot_meta(loaded_snapshot);
    ASSERT_TRUE(status.ok()) << status_debug_string(status);
    EXPECT_EQ(loaded_snapshot->apply_index, 12);
    EXPECT_EQ(loaded_snapshot->apply_term, 3);
    std::vector<KV> loaded_kvs;
    status = engine.for_each_snapshot_kv(
        [&loaded_kvs](const Key& key, const Value& value) -> Status {
            loaded_kvs.emplace_back(key, value);
            return Status::OK();
        });
    ASSERT_TRUE(status.ok()) << status_debug_string(status);
    EXPECT_EQ(loaded_kvs, (std::vector<KV>{{"a", "1"}, {"b", "2"}}));

    std::vector<LogEntry> actual;
    status = engine.read_wal_batch(actual);
    ASSERT_TRUE(status.ok()) << status_debug_string(status);
    ASSERT_EQ(actual.size(), 1U);
    EXPECT_EQ(actual[0].index, 13);
    EXPECT_EQ(actual[0].key, "c");
}

// 走快照之后，搞一个新的enginie去进行recover，检测recover的内容是否可以
TEST_F(PersistEngineTest, RecoverLoadsSnapshotMetaAndWalTogether) {
    PersistEngine engine = make_engine();
    Status status = engine.init();
    ASSERT_TRUE(status.ok()) << status_debug_string(status);

    const std::vector<LogEntry> wal_entries = {
        make_entry(3, 21, WriteOpType::PUT, "hot", "cold"),
        make_entry(3, 22, WriteOpType::DEL, "trash", ""),
    };
    KvStateMachine state_machine(EngineType::MAP);
    ASSERT_TRUE(
        state_machine.apply(make_entry(4, 12, WriteOpType::PUT, "alpha", "1"))
            .ok());
    ASSERT_TRUE(
        state_machine.apply(make_entry(4, 13, WriteOpType::PUT, "beta", "2"))
            .ok());
    const RaftMeta meta{
        .current_term = 11,
        .commit_index = 22,
        .voted_for =
            ReplicaID{.table_id = 101, .shard_index = 7, .replica_index = 0},
    };

    status = engine.append_wal_batch(wal_entries);
    ASSERT_TRUE(status.ok()) << status_debug_string(status);
    status = engine.do_snapshot(state_machine);
    ASSERT_TRUE(status.ok()) << status_debug_string(status);
    status = engine.save_raft_meta(meta);
    ASSERT_TRUE(status.ok()) << status_debug_string(status);

    PersistEngine recovered_engine = make_engine();
    status = recovered_engine.init();
    ASSERT_TRUE(status.ok()) << status_debug_string(status);

    PersistEngine::RecoverResult result;
    status = recovered_engine.recover(result);
    ASSERT_TRUE(status.ok()) << status_debug_string(status);
    ASSERT_NE(result.snapshot, nullptr);
    EXPECT_EQ(result.snapshot->apply_index, 13);
    EXPECT_EQ(result.snapshot->apply_term, 4);
    std::vector<KV> loaded_kvs;
    status = recovered_engine.for_each_snapshot_kv(
        [&loaded_kvs](const Key& key, const Value& value) -> Status {
            loaded_kvs.emplace_back(key, value);
            return Status::OK();
        });
    ASSERT_TRUE(status.ok()) << status_debug_string(status);
    EXPECT_EQ(loaded_kvs, (std::vector<KV>{{"alpha", "1"}, {"beta", "2"}}));
    EXPECT_EQ(result.raft_meta.current_term, meta.current_term);
    EXPECT_EQ(result.raft_meta.commit_index, meta.commit_index);
    ASSERT_TRUE(result.raft_meta.voted_for.has_value());
    EXPECT_EQ(result.raft_meta.voted_for.value(), meta.voted_for.value());
    ASSERT_EQ(result.wal_entries.size(), wal_entries.size());
    EXPECT_EQ(result.wal_entries[0].key, wal_entries[0].key);
    EXPECT_EQ(result.wal_entries[1].index, wal_entries[1].index);
}

}  // namespace
}  // namespace adviskv::storage
