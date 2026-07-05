#include <gtest/gtest.h>
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

        status = persist.save_raft_meta(RaftMeta{1, std::nullopt});
        ASSERT_TRUE(status.ok()) << adviskv::test::status_debug_string(status);
        ASSERT_TRUE(persist.close().ok());
    }

    void expect_append_wal_crashes_at(const char* crash_point) const {
        ASSERT_EXIT(
            {
                ::setenv("ADVISKV_ENABLE_CRASH_POINT", crash_point, 1);

                PersistEngine persist(base_dir_.string(), replica_id_);
                Status status = persist.init();
                if (status.fail()) {
                    ::_exit(3);
                }

                status = persist.append_wal(
                    make_entry(2, 3, "crash-key", "crash-value"));
                if (status.fail()) {
                    ::_exit(4);
                }
                ::_exit(0);
            },
            ::testing::ExitedWithCode(137), "");
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
        EXPECT_TRUE(result.need_recover);

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
        EXPECT_FALSE(result.need_recover);

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
        expect_append_wal_crashes_at("framed_record.after_len_write");
        assert_recovery_truncates_crashed_tail();
    }
    {
        expect_append_wal_crashes_at("framed_record.after_crc_write");
        assert_recovery_truncates_crashed_tail();
    }
    {
        expect_append_wal_crashes_at("framed_record.after_payload_write");
        assert_recovery_not_truncates_crashed_tail();
    }
}

TEST_F(WalCrashInjectionTest, RecoverTruncatesPayloadPartialWriteCrash) {
    save_basic_wal();

    expect_append_wal_crashes_at(
        "framed_record.payload_write.after_partial_write");

    assert_recovery_truncates_crashed_tail();
}

}  // namespace
}  // namespace adviskv::storage