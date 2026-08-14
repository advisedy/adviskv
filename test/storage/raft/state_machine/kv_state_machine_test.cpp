#include "storage/raft/state_machine/kv_state_machine.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "storage/model/model.h"
#include "storage/persist/persist_engine.h"
#include "test/test_env.h"

namespace adviskv::storage {
namespace {

namespace fs = std::filesystem;

LogEntry make_entry(Term term, LogIndex index, WriteOpType op_type,
                    std::string key, std::string value) {
    return LogEntry{term, index, op_type, std::move(key), std::move(value)};
}

class KvStateMachineTest : public ::testing::Test {
   protected:
    void SetUp() override {
        base_dir_ = adviskv::test::make_unique_test_dir("kv_state_machine",
                                                        sequence_++);
        ASSERT_TRUE(fs::create_directories(base_dir_)) << base_dir_.string();
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(base_dir_, ec);
    }

    PersistEngine make_engine() const {
        return PersistEngine(base_dir_.string(), ReplicaID{1, 1, 0});
    }

   private:
    static inline int sequence_{0};
    fs::path base_dir_;
};

// 测试PUT/DEL/NONE三种操作对状态机水位的更新是否正确
TEST_F(KvStateMachineTest, ApplyPutDeleteAndNoneUpdateState) {
    KvStateMachine state_machine(EngineType::MAP);

    Status status =
        state_machine.apply(make_entry(1, 1, WriteOpType::PUT, "k1", "v1"));
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(state_machine.apply_index(), 1);
    EXPECT_EQ(state_machine.apply_term(), 1);

    Value value;
    status = state_machine.get("k1", value);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(value, "v1");

    status = state_machine.apply(make_entry(2, 2, WriteOpType::DEL, "k1", ""));
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(state_machine.apply_index(), 2);
    EXPECT_EQ(state_machine.apply_term(), 2);
    status = state_machine.get("k1", value);
    EXPECT_FALSE(status.ok());

    status = state_machine.apply(make_entry(3, 3, WriteOpType::NONE, "", ""));
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(state_machine.apply_index(), 3);
    EXPECT_EQ(state_machine.apply_term(), 3);

}

// 状态机做快照后，新创建的状态机通过restore应能恢复快照中的数据
TEST_F(KvStateMachineTest, SnapshotAndRestoreRoundTrip) {
    KvStateMachine source(EngineType::MAP);
    ASSERT_TRUE(
        source.apply(make_entry(4, 10, WriteOpType::PUT, "a", "1")).ok());
    ASSERT_TRUE(
        source.apply(make_entry(4, 11, WriteOpType::PUT, "b", "2")).ok());

    PersistEngine persist = make_engine();
    ASSERT_TRUE(persist.init().ok());
    ASSERT_TRUE(persist.write_snapshot(source).ok());

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
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(restored.apply_index(), 11);
    EXPECT_EQ(restored.apply_term(), 4);

    Value value;
    status = restored.get("a", value);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(value, "1");

    status = restored.get("b", value);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(value, "2");
}

// restore应替换状态机中的全部数据，旧key应被清除，新key应可读
TEST_F(KvStateMachineTest, RestoreReplacesExistingData) {
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
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(state_machine.apply_index(), 20);
    EXPECT_EQ(state_machine.apply_term(), 5);

    Value value;
    status = state_machine.get("old", value);
    EXPECT_FALSE(status.ok());

    status = state_machine.get("new", value);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(value, "y");
}

// 状态机不对 request_id 做 apply 侧幂等/冲突拦截；只在成功 apply 后登记，
// 供 ReplicaLoop 查询与 snapshot 使用。
TEST_F(KvStateMachineTest, RequestIdIsRecordedOnApply) {
    KvStateMachine state_machine(EngineType::MAP);

    LogEntry first = make_entry(1, 1, WriteOpType::PUT, "idempotent-key", "v1");
    first.request_id = "request-1";
    ASSERT_TRUE(state_machine.apply(first).ok());

    RequestRecord record;
    ASSERT_TRUE(state_machine.find_request("request-1", record).ok());
    EXPECT_EQ(record.index, 1);
    EXPECT_EQ(record.key, "idempotent-key");
    EXPECT_EQ(record.value, "v1");

    // 若同 request_id 的日志再次出现，状态机仍按 entry 执行（不在此层跳过）。
    LogEntry again = first;
    again.term = 2;
    again.index = 2;
    again.value = "v2";
    ASSERT_TRUE(state_machine.apply(again).ok());

    Value value;
    ASSERT_TRUE(state_machine.get("idempotent-key", value).ok());
    EXPECT_EQ(value, "v2");
    // 首次登记保留，便于 loop 用“第一次已 apply 的内容”做冲突判断。
    ASSERT_TRUE(state_machine.find_request("request-1", record).ok());
    EXPECT_EQ(record.index, 1);
    EXPECT_EQ(record.value, "v1");
}

TEST_F(KvStateMachineTest, SnapshotRestoresRequestIdRecords) {
    KvStateMachine source(EngineType::MAP);
    LogEntry entry = make_entry(4, 10, WriteOpType::PUT, "snap-key", "snap-value");
    entry.request_id = "snap-request";
    ASSERT_TRUE(source.apply(entry).ok());

    PersistEngine persist = make_engine();
    ASSERT_TRUE(persist.init().ok());
    ASSERT_TRUE(persist.write_snapshot(source).ok());

    SnapshotPtr snapshot = std::make_shared<Snapshot>();
    ASSERT_TRUE(persist.load_snapshot_meta(snapshot).ok());
    KvStateMachine restored(EngineType::MAP);
    Status restore_status = restored.restore(
            snapshot,
            [&persist](const KvVisitor& visitor) { return persist.for_each_snapshot_kv(visitor); },
            [&persist](const RequestVisitor& visitor) { return persist.for_each_snapshot_request(visitor); });
    ASSERT_TRUE(restore_status.ok()) << test::status_debug_string(restore_status);

    RequestRecord record;
    ASSERT_TRUE(restored.find_request("snap-request", record).ok());
    EXPECT_EQ(record.key, "snap-key");
    EXPECT_EQ(record.value, "snap-value");
    EXPECT_TRUE(restored.apply(entry).ok());
    Value value;
    ASSERT_TRUE(restored.get("snap-key", value).ok());
    EXPECT_EQ(value, "snap-value");
}

}  // namespace
}  // namespace adviskv::storage
