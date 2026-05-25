#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <string>

#include "storage/model/param.h"
#include "storage/persist/persist_engine.h"
#include "test/test_env.h"

namespace fs = std::filesystem;

namespace adviskv::storage {
namespace {

class WalCrashInjectionTest : public ::testing::Test {
   protected:
    void SetUp() override {
        base_dir_ =
            adviskv::test::make_unique_test_dir("wal_crash_injection", seq_++);
        ASSERT_TRUE(fs::create_directories(base_dir_)) << base_dir_.string();
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(base_dir_, ec);
    }

    static LogEntry make_entry(Term term, LogIndex index, std::string key,
                               std::string value) {
        return LogEntry{term, index, WriteOpType::PUT, std::move(key),
                        std::move(value)};
    }

    void save_basic_wal() const {
        PersistEngine persist(base_dir_.string(), replica_id_);
        Status status = persist.init();
        ASSERT_TRUE(status.ok()) << adviskv::test::status_debug_string(status);

        status = persist.append_wal_batch({
            make_entry(1, 1, "stable-1", "value-1"),
            make_entry(1, 2, "stable-2", "value-2"),
        });
        ASSERT_TRUE(status.ok()) << adviskv::test::status_debug_string(status);

        status = persist.save_raft_meta(RaftMeta{1, 2, std::nullopt});
        ASSERT_TRUE(status.ok()) << adviskv::test::status_debug_string(status);
        ASSERT_TRUE(persist.close().ok());
    }

    int run_crash_runner(const char* crash_point) const {
        pid_t pid = ::fork();
        // 父进程的返回值大于0，代表子进程的pid，子进程的返回值是0
        if (pid == 0) {
            ::setenv("ADVISKV_ENABLE_CRASH_POINT", crash_point, 1);
            ::execl(ADVISKV_CRASH_RUNNER_PATH, ADVISKV_CRASH_RUNNER_PATH,
                    base_dir_.c_str(), static_cast<char*>(nullptr));
            ::_exit(127);
        }

        if (pid < 0) {
            return -1;
        }

        int status = 0;
        if (::waitpid(pid, &status, 0) < 0) {
            return -1;
        }
        return status;
    }

    void assert_recovery_truncates_crashed_tail() const {
        PersistEngine recovered(base_dir_.string(), replica_id_);
        Status status = recovered.init();
        ASSERT_TRUE(status.ok()) << adviskv::test::status_debug_string(status);

        PersistEngine::RecoverResult result;
        status = recovered.recover(result);
        ASSERT_TRUE(status.ok()) << adviskv::test::status_debug_string(status);

        ASSERT_EQ(result.wal_entries.size(), 2U);
        EXPECT_EQ(result.wal_entries[0].index, 1);
        EXPECT_EQ(result.wal_entries[1].index, 2);
        EXPECT_EQ(result.wal_recovery.action,
                  WalRecoveryAction::TRUNCATED_UNCOMMITTED);
        EXPECT_EQ(result.wal_recovery.last_good_index, 2);

        std::vector<LogEntry> entries_after_repair;
        status = recovered.read_wal_batch(entries_after_repair);
        ASSERT_TRUE(status.ok()) << adviskv::test::status_debug_string(status);
        ASSERT_EQ(entries_after_repair.size(), 2U);
        EXPECT_EQ(entries_after_repair.back().key, "stable-2");
    }
    void assert_recovery_not_truncates_crashed_tail() const {
        PersistEngine recovered(base_dir_.string(), replica_id_);
        Status status = recovered.init();
        ASSERT_TRUE(status.ok()) << adviskv::test::status_debug_string(status);

        PersistEngine::RecoverResult result;
        status = recovered.recover(result);
        ASSERT_TRUE(status.ok()) << adviskv::test::status_debug_string(status);

        ASSERT_EQ(result.wal_entries.size(), 3U);
        EXPECT_EQ(result.wal_entries[0].index, 1);
        EXPECT_EQ(result.wal_entries[1].index, 2);
        EXPECT_EQ(result.wal_entries[2].index, 3);
        EXPECT_EQ(result.wal_recovery.action, WalRecoveryAction::NONE);
        EXPECT_EQ(result.wal_recovery.last_good_index, 3);

        std::vector<LogEntry> entries_after_repair;
        status = recovered.read_wal_batch(entries_after_repair);
        ASSERT_TRUE(status.ok()) << adviskv::test::status_debug_string(status);
        ASSERT_EQ(entries_after_repair.size(), 3U);
        EXPECT_EQ(entries_after_repair.back().key, "crash-key");
    }
    static inline int seq_{0};
    fs::path base_dir_;
    ReplicaID replica_id_{910, 1, 0};
};

TEST_F(WalCrashInjectionTest,
       RecoverTruncatesPartialWalRecordAfterProcessCrash) {
    save_basic_wal();
    /*
            testhook::crash_point("framed_record.after_len_write");
            RETURN_IF_INVALID_STATUS(func::write_value(fd, crc))
            testhook::crash_point("framed_record.after_crc_write");
            RETURN_IF_INVALID_STATUS(func::write_full(
                fd, payload.data(), payload.size(),
                "framed_record.payload_write.after_partial_write"))
            testhook::crash_point("framed_record.after_payload_write");
    */
    {
        int child_status = run_crash_runner("framed_record.after_len_write");
        ASSERT_NE(child_status, -1);
        ASSERT_TRUE(WIFEXITED(child_status));
        ASSERT_EQ(WEXITSTATUS(child_status), 137);

        assert_recovery_truncates_crashed_tail();
    }
    {
        int child_status = run_crash_runner("framed_record.after_crc_write");
        ASSERT_NE(child_status, -1);
        ASSERT_TRUE(WIFEXITED(child_status));
        ASSERT_EQ(WEXITSTATUS(child_status), 137);

        assert_recovery_truncates_crashed_tail();
    }
    {
        int child_status =
            run_crash_runner("framed_record.after_payload_write");
        ASSERT_NE(child_status, -1);
        ASSERT_TRUE(WIFEXITED(child_status));
        ASSERT_EQ(WEXITSTATUS(child_status), 137);

        assert_recovery_not_truncates_crashed_tail();
    }
}

TEST_F(WalCrashInjectionTest, RecoverTruncatesPayloadPartialWriteCrash) {
    save_basic_wal();

    int child_status =
        run_crash_runner("framed_record.payload_write.after_partial_write");
    ASSERT_NE(child_status, -1);
    ASSERT_TRUE(WIFEXITED(child_status));
    ASSERT_EQ(WEXITSTATUS(child_status), 137);

    assert_recovery_truncates_crashed_tail();
}

}  // namespace
}  // namespace adviskv::storage