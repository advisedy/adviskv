#include <gtest/gtest.h>

#include <filesystem>
#include <string>
#include <utility>
#include <vector>

#include "test/test_env.h"
#include "storage/model/param.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/state_machine/state_machine.h"

namespace fs = std::filesystem;

namespace adviskv::storage {
namespace {

std::string status_debug_string(const Status& status) {
    return "code=" + std::to_string(static_cast<int>(status.code())) +
           ", msg=" + status.msg();
}

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

    static SnapshotPtr make_snapshot() {
        auto snapshot = std::make_shared<Snapshot>();
        snapshot->apply_index = 12;
        snapshot->apply_term = 4;
        snapshot->kvs = {
            {"alpha", "1"},
            {"beta", "2"},
        };
        return snapshot;
    }

    static inline int sequence_{0};

    fs::path base_dir_;
    ReplicaID replica_id_{.table_id = 101, .shard_index = 7, .replica_index = 2};
};

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

TEST_F(PersistEngineTest, SaveAndLoadRaftMeta) {
    PersistEngine engine = make_engine();
    Status status = engine.init();
    ASSERT_TRUE(status.ok()) << status_debug_string(status);

    RaftMeta expected{
        .current_term = 9,
        .commit_index = 17,
        .voted_for = ReplicaID{.table_id = 9, .shard_index = 2, .replica_index = 1},
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

TEST_F(PersistEngineTest, SaveAndLoadSnapshot) {
    PersistEngine engine = make_engine();
    Status status = engine.init();
    ASSERT_TRUE(status.ok()) << status_debug_string(status);

    SnapshotPtr expected = make_snapshot();
    status = engine.save_snapshot(expected);
    ASSERT_TRUE(status.ok()) << status_debug_string(status);

    SnapshotPtr actual = std::make_shared<Snapshot>();
    status = engine.load_snapshot(actual);
    ASSERT_TRUE(status.ok()) << status_debug_string(status);
    EXPECT_EQ(actual->apply_index, expected->apply_index);
    EXPECT_EQ(actual->apply_term, expected->apply_term);
    EXPECT_EQ(actual->kvs, expected->kvs);
}

TEST_F(PersistEngineTest, RecoverLoadsSnapshotMetaAndWalTogether) {
    PersistEngine engine = make_engine();
    Status status = engine.init();
    ASSERT_TRUE(status.ok()) << status_debug_string(status);

    const std::vector<LogEntry> wal_entries = {
        make_entry(3, 21, WriteOpType::PUT, "hot", "cold"),
        make_entry(3, 22, WriteOpType::DEL, "trash", ""),
    };
    const SnapshotPtr snapshot = make_snapshot();
    const RaftMeta meta{
        .current_term = 11,
        .commit_index = 22,
        .voted_for = ReplicaID{.table_id = 101, .shard_index = 7, .replica_index = 0},
    };

    status = engine.append_wal_batch(wal_entries);
    ASSERT_TRUE(status.ok()) << status_debug_string(status);
    status = engine.save_snapshot(snapshot);
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
    EXPECT_EQ(result.snapshot->apply_index, snapshot->apply_index);
    EXPECT_EQ(result.snapshot->apply_term, snapshot->apply_term);
    EXPECT_EQ(result.snapshot->kvs, snapshot->kvs);
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
