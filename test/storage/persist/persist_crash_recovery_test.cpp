#include <cstdlib>
#include <filesystem>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <unistd.h>

#include "storage/persist/persist_engine.h"
#include "storage/raft/state_machine/kv_state_machine.h"
#include "test/test_env.h"

namespace adviskv::storage {
namespace {

namespace fs = std::filesystem;

class PersistCrashRecoveryTest : public ::testing::Test {
protected:
    void SetUp() override {
        base_dir_ = adviskv::test::make_unique_test_dir("persist_crash_recovery", sequence_++);
        ASSERT_TRUE(fs::create_directories(base_dir_)) << base_dir_.string();
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(base_dir_, ec);
    }

    PersistEngine make_engine() const { return PersistEngine(base_dir_.string(), replica_id_); }

    static LogEntry make_entry(Term term, LogIndex index, std::string key, std::string value) {
        return LogEntry{term, index, WriteOpType::PUT, std::move(key), std::move(value)};
    }

    void save_basic_wal() const {
        PersistEngine persist = make_engine();
        ASSERT_TRUE(persist.init().ok());
        ASSERT_TRUE(persist.append_wal_batch(
                                   {make_entry(1, 1, "stable-1", "value-1"), make_entry(1, 2, "stable-2", "value-2")})
                            .ok());
        ASSERT_TRUE(persist.save_raft_meta(RaftMeta{1, std::nullopt}).ok());
        ASSERT_TRUE(persist.close().ok());
    }

    void expect_append_crash(const char* crash_point) const {
        ASSERT_EXIT(
                {
                    if (::setenv("ADVISKV_ENABLE_CRASH_POINT", crash_point, 1) != 0) ::_exit(2);
                    PersistEngine persist(base_dir_.string(), replica_id_);
                    if (persist.init().fail()) ::_exit(3);
                    if (persist.append_wal(make_entry(2, 3, "crash-key", "crash-value")).fail()) ::_exit(4);
                    ::_exit(0);
                },
                ::testing::ExitedWithCode(137), "");
    }

    PersistEngine::RecoverResult recover() const {
        PersistEngine recovered = make_engine();
        EXPECT_TRUE(recovered.init().ok());
        PersistEngine::RecoverResult result;
        EXPECT_TRUE(recovered.recover(result).ok());
        return result;
    }

    void seed_snapshot_and_wal(const std::vector<LogEntry>& wal_entries) const {
        PersistEngine persist = make_engine();
        ASSERT_TRUE(persist.init().ok());
        KvStateMachine snapshot_state(EngineType::MAP);
        ASSERT_TRUE(snapshot_state.apply(make_entry(1, 1, "snapshot-1", "value-1")).ok());
        ASSERT_TRUE(snapshot_state.apply(make_entry(1, 2, "snapshot-2", "value-2")).ok());
        ASSERT_TRUE(persist.write_snapshot(snapshot_state).ok());
        ASSERT_TRUE(persist.append_wal_batch(wal_entries).ok());
        ASSERT_TRUE(persist.close().ok());
    }

    static inline int sequence_{0};
    fs::path base_dir_;
    ReplicaID replica_id_{910, 1, 0};
};

struct WalCrashCase {
    const char* name;
    const char* crash_point;
    size_t expected_entries;
    bool expected_recovering;
};

void PrintTo(const WalCrashCase& value, std::ostream* stream) { *stream << value.name; }

class WalRecordCrashTest : public PersistCrashRecoveryTest, public ::testing::WithParamInterface<WalCrashCase> {};

// 场景：磁盘已有一条完整 WAL record，子进程开始追加第二条 framed record。
// 过程：分别在 frame 各写入边界触发进程退出，再用同一目录重建 PersistEngine。
// 预期：恢复结果只包含完整 record；半写 header/body/checksum 不会被当作有效日志。
TEST_P(WalRecordCrashTest, RecoveryKeepsOnlyCompleteFramedRecords) {
    save_basic_wal();
    expect_append_crash(GetParam().crash_point);

    PersistEngine::RecoverResult result = recover();

    ASSERT_EQ(result.wal_entries.size(), GetParam().expected_entries);
    EXPECT_EQ(result.need_recover, GetParam().expected_recovering);
    EXPECT_EQ(result.wal_entries[0].index, 1);
    EXPECT_EQ(result.wal_entries[1].index, 2);
    if (GetParam().expected_entries == 3) {
        EXPECT_EQ(result.wal_entries[2], make_entry(2, 3, "crash-key", "crash-value"));
    }
}

INSTANTIATE_TEST_SUITE_P(WriteBoundaries, WalRecordCrashTest,
                         ::testing::Values(WalCrashCase{"AfterLength", "framed_record.after_len_write", 2, true},
                                           WalCrashCase{"AfterCrc", "framed_record.after_crc_write", 2, true},
                                           WalCrashCase{"DuringPayload",
                                                        "framed_record.payload_write.after_partial_write", 2, true},
                                           WalCrashCase{"AfterPayload", "framed_record.after_payload_write", 3, false},
                                           WalCrashCase{"AfterFsync", "persist.append_wal.after_fsync", 3, false}),
                         [](const ::testing::TestParamInfo<WalCrashCase>& info) { return info.param.name; });

struct AtomicPublishCase {
    const char* name;
    const char* crash_point;
    bool published;
};

void PrintTo(const AtomicPublishCase& value, std::ostream* stream) { *stream << value.name; }

class RaftMetaAtomicCrashTest : public PersistCrashRecoveryTest,
                                public ::testing::WithParamInterface<AtomicPublishCase> {};

// 场景：目录中已持久化旧 Raft hard state，子进程准备原子发布新 term/vote。
// 过程：分别在临时文件 fsync 后、rename 后杀死进程，再重新打开目录读取元数据。
// 预期：rename 前看到完整旧状态，rename 后看到完整新状态，绝不出现字段拼接或损坏。
TEST_P(RaftMetaAtomicCrashTest, ReopenObservesOldOrNewHardState) {
    {
        PersistEngine persist = make_engine();
        ASSERT_TRUE(persist.init().ok());
        ASSERT_TRUE(persist.save_raft_meta(RaftMeta{1, replica_id_}).ok());
    }

    ASSERT_EXIT(
            {
                if (::setenv("ADVISKV_ENABLE_CRASH_POINT", GetParam().crash_point, 1) != 0) ::_exit(2);
                PersistEngine persist(base_dir_.string(), replica_id_);
                if (persist.init().fail()) ::_exit(3);
                if (persist.save_raft_meta(RaftMeta{2, std::nullopt}).fail()) ::_exit(4);
                ::_exit(0);
            },
            ::testing::ExitedWithCode(137), "");

    PersistEngine reopened = make_engine();
    ASSERT_TRUE(reopened.init().ok());
    RaftMeta actual;
    ASSERT_TRUE(reopened.load_raft_meta(actual).ok());
    EXPECT_EQ(actual.current_term, GetParam().published ? 2 : 1);
    EXPECT_EQ(actual.voted_for.has_value(), !GetParam().published);
}

INSTANTIATE_TEST_SUITE_P(
        RenameBoundary, RaftMetaAtomicCrashTest,
        ::testing::Values(AtomicPublishCase{"BeforeRename", "persist.save_raft_meta.after_tmp_fsync", false},
                          AtomicPublishCase{"AfterRename", "persist.save_raft_meta.after_rename", true}),
        [](const ::testing::TestParamInfo<AtomicPublishCase>& info) { return info.param.name; });

class WalRewriteAtomicCrashTest : public PersistCrashRecoveryTest,
                                  public ::testing::WithParamInterface<AtomicPublishCase> {};

// 场景：旧 WAL 已完整存在，子进程因截断/压缩准备以一份新 WAL 替换它。
// 过程：在新文件 fsync 与 rename 两侧分别崩溃，然后从原目录执行 WAL recovery。
// 预期：恢复只会观察到整份旧 WAL 或整份新 WAL，不会混合两个版本的记录。
TEST_P(WalRewriteAtomicCrashTest, RecoveryObservesEntireOldOrNewWal) {
    const std::vector<LogEntry> old_entries{make_entry(1, 3, "old-3", "value-3"), make_entry(1, 4, "old-4", "value-4")};
    const std::vector<LogEntry> new_entries{make_entry(2, 3, "new-3", "value-3"), make_entry(2, 4, "new-4", "value-4")};
    seed_snapshot_and_wal(old_entries);

    ASSERT_EXIT(
            {
                if (::setenv("ADVISKV_ENABLE_CRASH_POINT", GetParam().crash_point, 1) != 0) ::_exit(2);
                PersistEngine persist(base_dir_.string(), replica_id_);
                if (persist.init().fail()) ::_exit(3);
                if (persist.rewrite_wal(new_entries).fail()) ::_exit(4);
                ::_exit(0);
            },
            ::testing::ExitedWithCode(137), "");

    PersistEngine::RecoverResult result = recover();
    EXPECT_FALSE(result.need_recover);
    EXPECT_EQ(result.wal_entries, GetParam().published ? new_entries : old_entries);
}

INSTANTIATE_TEST_SUITE_P(RenameBoundary, WalRewriteAtomicCrashTest,
                         ::testing::Values(AtomicPublishCase{"BeforeRename", "persist.rewrite_wal.after_tmp_fsync",
                                                             false},
                                           AtomicPublishCase{"AfterRename", "persist.rewrite_wal.after_rename", true}),
                         [](const ::testing::TestParamInfo<AtomicPublishCase>& info) { return info.param.name; });

class SnapshotAtomicCrashTest : public PersistCrashRecoveryTest,
                                public ::testing::WithParamInterface<AtomicPublishCase> {};

// 场景：目录中已有旧 snapshot，子进程写入包含更高 apply index 和新 KV 的 snapshot。
// 过程：在 snapshot 临时文件 fsync 后或 rename 后强制退出，再重开并遍历 snapshot。
// 预期：rename 前恢复完整旧镜像，rename 后恢复完整新镜像，元数据与 KV 必须属于同一版本。
TEST_P(SnapshotAtomicCrashTest, ReopenObservesEntireOldOrNewSnapshot) {
    {
        PersistEngine persist = make_engine();
        ASSERT_TRUE(persist.init().ok());
        KvStateMachine old_state(EngineType::MAP);
        ASSERT_TRUE(old_state.apply(make_entry(1, 1, "old", "value-1")).ok());
        ASSERT_TRUE(persist.write_snapshot(old_state).ok());
    }

    ASSERT_EXIT(
            {
                if (::setenv("ADVISKV_ENABLE_CRASH_POINT", GetParam().crash_point, 1) != 0) ::_exit(2);
                PersistEngine persist(base_dir_.string(), replica_id_);
                if (persist.init().fail()) ::_exit(3);
                KvStateMachine new_state(EngineType::MAP);
                if (new_state.apply(make_entry(1, 1, "old", "value-1")).fail()) ::_exit(4);
                if (new_state.apply(make_entry(2, 2, "new", "value-2")).fail()) ::_exit(5);
                if (persist.write_snapshot(new_state).fail()) ::_exit(6);
                ::_exit(0);
            },
            ::testing::ExitedWithCode(137), "");

    PersistEngine reopened = make_engine();
    ASSERT_TRUE(reopened.init().ok());
    SnapshotPtr snapshot;
    ASSERT_TRUE(reopened.load_snapshot_meta(snapshot).ok());
    ASSERT_NE(snapshot, nullptr);
    EXPECT_EQ(snapshot->apply_index, GetParam().published ? 2 : 1);
    std::vector<KV> kvs;
    ASSERT_TRUE(reopened.for_each_snapshot_kv([&kvs](const Key& key, const Value& value) {
                            kvs.emplace_back(key, value);
                            return Status::OK();
                        })
                        .ok());
    const std::vector<KV> expected = GetParam().published ? std::vector<KV>{{"new", "value-2"}, {"old", "value-1"}}
                                                          : std::vector<KV>{{"old", "value-1"}};
    EXPECT_EQ(kvs, expected);
}

INSTANTIATE_TEST_SUITE_P(
        RenameBoundary, SnapshotAtomicCrashTest,
        ::testing::Values(AtomicPublishCase{"BeforeRename", "persist.write_snapshot.after_tmp_fsync", false},
                          AtomicPublishCase{"AfterRename", "persist.write_snapshot.after_rename", true}),
        [](const ::testing::TestParamInfo<AtomicPublishCase>& info) { return info.param.name; });

}  // namespace
}  // namespace adviskv::storage
