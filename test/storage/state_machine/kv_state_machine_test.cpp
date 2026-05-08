#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "storage/persist/persist_engine.h"
#include "storage/model/param.h"
#include "storage/raft/state_machine/kv_state_machine.h"
#include "test/test_env.h"

namespace adviskv::storage {
namespace {

namespace fs = std::filesystem;

std::string status_debug_string(const Status& status) {
    return "code=" + std::to_string(static_cast<int>(status.code())) +
           ", msg=" + status.msg();
}

LogEntry make_entry(Term term, LogIndex index, WriteOpType op_type,
                    std::string key, std::string value) {
    return LogEntry{
        .term = term,
        .index = index,
        .op_type = op_type,
        .key = std::move(key),
        .value = std::move(value),
    };
}

TEST(KvStateMachineTest, ApplyPutDeleteAndNoneUpdateState) {
    KvStateMachine state_machine(EngineType::MAP);

    Status status =
        state_machine.apply(make_entry(1, 1, WriteOpType::PUT, "k1", "v1"));
    ASSERT_TRUE(status.ok()) << status_debug_string(status);
    EXPECT_EQ(state_machine.apply_index(), 1);
    EXPECT_EQ(state_machine.apply_term(), 1);

    Value value;
    status = state_machine.get("k1", value);
    ASSERT_TRUE(status.ok()) << status_debug_string(status);
    EXPECT_EQ(value, "v1");

    status =
        state_machine.apply(make_entry(2, 2, WriteOpType::DEL, "k1", ""));
    ASSERT_TRUE(status.ok()) << status_debug_string(status);
    EXPECT_EQ(state_machine.apply_index(), 2);
    EXPECT_EQ(state_machine.apply_term(), 2);
    status = state_machine.get("k1", value);
    EXPECT_FALSE(status.ok());

    status =
        state_machine.apply(make_entry(3, 3, WriteOpType::NONE, "", ""));
    ASSERT_TRUE(status.ok()) << status_debug_string(status);
    EXPECT_EQ(state_machine.apply_index(), 3);
    EXPECT_EQ(state_machine.apply_term(), 3);
}

TEST(KvStateMachineTest, SnapshotAndRestoreRoundTrip) {
    KvStateMachine source(EngineType::MAP);
    ASSERT_TRUE(source.apply(make_entry(4, 10, WriteOpType::PUT, "a", "1"))
                    .ok());
    ASSERT_TRUE(source.apply(make_entry(4, 11, WriteOpType::PUT, "b", "2"))
                    .ok());

    static int sequence = 0;
    auto base_dir = adviskv::test::make_unique_test_dir("kv_state_machine", sequence++);
    ASSERT_TRUE(fs::create_directories(base_dir)) << base_dir.string();
    PersistEngine persist(base_dir.string(),
                          ReplicaID{.table_id = 1, .shard_index = 1, .replica_index = 0});
    ASSERT_TRUE(persist.init().ok());
    ASSERT_TRUE(persist.do_snapshot(source).ok());

    SnapshotPtr snapshot = std::make_shared<Snapshot>();
    ASSERT_TRUE(persist.load_snapshot_meta(snapshot).ok());
    ASSERT_NE(snapshot, nullptr);
    EXPECT_EQ(snapshot->apply_index, 11);
    EXPECT_EQ(snapshot->apply_term, 4);

    KvStateMachine restored(EngineType::MAP);
    Status status = restored.restore(
        snapshot, [&persist](const KvVisitor& consume) -> Status {
            return persist.for_each_snapshot_kv(consume);
        });
    ASSERT_TRUE(status.ok()) << status_debug_string(status);
    EXPECT_EQ(restored.apply_index(), 11);
    EXPECT_EQ(restored.apply_term(), 4);

    Value value;
    status = restored.get("a", value);
    ASSERT_TRUE(status.ok()) << status_debug_string(status);
    EXPECT_EQ(value, "1");

    status = restored.get("b", value);
    ASSERT_TRUE(status.ok()) << status_debug_string(status);
    EXPECT_EQ(value, "2");
}

TEST(KvStateMachineTest, RestoreReplacesExistingData) {
    KvStateMachine state_machine(EngineType::MAP);
    ASSERT_TRUE(
        state_machine.apply(make_entry(1, 1, WriteOpType::PUT, "old", "x"))
            .ok());

    SnapshotPtr snapshot = std::make_shared<Snapshot>();
    snapshot->apply_index = 20;
    snapshot->apply_term = 5;
    std::vector<KV> kvs = {{"new", "y"}};

    Status status = state_machine.restore(
        snapshot, [&kvs](const KvVisitor& consume) -> Status {
            for (const auto& [key, value] : kvs) {
                Status status = consume(key, value);
                if (status.fail()) {
                    return status;
                }
            }
            return Status::OK();
        });
    ASSERT_TRUE(status.ok()) << status_debug_string(status);
    EXPECT_EQ(state_machine.apply_index(), 20);
    EXPECT_EQ(state_machine.apply_term(), 5);

    Value value;
    status = state_machine.get("old", value);
    EXPECT_FALSE(status.ok());

    status = state_machine.get("new", value);
    ASSERT_TRUE(status.ok()) << status_debug_string(status);
    EXPECT_EQ(value, "y");
}

}  // namespace
}  // namespace adviskv::storage
