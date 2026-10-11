#include "storage/persist/persist_engine.h"

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <fmt/base.h>
#include <gtest/gtest.h>

#include <fcntl.h>
#include <unistd.h>

#include "common/binary/codec.h"
#include "common/crc.h"
#include "common/status.h"
#include "common/model/model.h"
#include "storage/model/model.h"
#include "storage/raft/state_machine/kv_state_machine.h"
#include "storage/raft/state_machine/state_machine.h"
#include "storage/persist/snapshot_store.h"
#include "test/storage/persist/snapshot_test_util.h"
#include "test/storage/persist/snapshot_test_util.h"
#include "test/test_env.h"

namespace fs = std::filesystem;

namespace adviskv::storage {
namespace {

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

    fs::path wal_path() const {
        return base_dir_ / (std::to_string(replica_id_.table_id) + "-" + std::to_string(replica_id_.shard_index)) /
               "wal.log";
    }

    static LogEntry make_entry(Term term, LogIndex index, WriteOpType op_type, std::string key, std::string value) {
        return LogEntry{term, index, op_type, std::move(key), std::move(value)};
    }

    static std::vector<uint8_t> encode_wal_payload(const LogEntry& entry) {
        BufferWriter buf;
        binary::encode(entry, buf);
        return buf.take();
    }

    static std::vector<uint8_t> encode_wal_record(const LogEntry& entry) {
        std::vector<uint8_t> payload = encode_wal_payload(entry);
        const int32_t data_len = static_cast<int32_t>(payload.size());
        const uint32_t crc = compute_crc32(payload.data(), payload.size());

        std::vector<uint8_t> record;
        append_raw_value(record, data_len);
        append_raw_value(record, crc);
        record.insert(record.end(), payload.begin(), payload.end());
        return record;
    }

    static std::vector<uint8_t> encode_wal_record_with_bad_crc(const LogEntry& entry) {
        std::vector<uint8_t> record = encode_wal_record(entry);
        record.back() ^= 0x01;
        return record;
    }

    template <typename T>
    static void append_raw_value(std::vector<uint8_t>& out, const T& value) {
        const auto* p = reinterpret_cast<const uint8_t*>(&value);
        out.insert(out.end(), p, p + sizeof(T));
    }

    static void append_raw_bytes(const fs::path& path, const std::vector<uint8_t>& bytes) {
        std::ofstream out(path, std::ios::binary | std::ios::app);
        ASSERT_TRUE(out.is_open()) << path.string();
        out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
        ASSERT_TRUE(out.good()) << path.string();
    }

    static inline int sequence_{0};

    fs::path base_dir_;
    ReplicaID replica_id_{101, 7, 2};
};

// 测试一下append_wal_batch和read_wal_batch是否可以正常运行
TEST_F(PersistEngineTest, AppendWalBatchAndReadBackEntries) {
    PersistEngine engine = make_engine();
    Status status = engine.init();
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    const std::vector<LogEntry> expected = {
            make_entry(1, 1, WriteOpType::PUT, "k1", "v1"),
            make_entry(1, 2, WriteOpType::DEL, "k2", ""),
            make_entry(2, 3, WriteOpType::PUT, "k3", "v3"),
    };

    status = engine.wal().append_batch(expected);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    std::vector<LogEntry> actual;
    status = engine.wal().read_batch(actual);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    ASSERT_EQ(actual.size(), expected.size());

    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(actual[i].term, expected[i].term);
        EXPECT_EQ(actual[i].index, expected[i].index);
        EXPECT_EQ(actual[i].op_type, expected[i].op_type);
        EXPECT_EQ(actual[i].key, expected[i].key);
        EXPECT_EQ(actual[i].value, expected[i].value);
        EXPECT_EQ(actual[i].request_id, expected[i].request_id);
    }
}

// 检测配置变更 WAL entry 里的成员字段可以完整持久化和读取。
TEST_F(PersistEngineTest, ConfigChangeWalPersistsMemberFields) {
    PersistEngine engine = make_engine();
    Status status = engine.init();
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    LogEntry add_learner = make_entry(3, 10, WriteOpType::ADD_LEARNER, "", "");
    add_learner.config_member =
            PeerMember{"node-10", ReplicaID{101, 7, 10}, Endpoint{"127.0.0.10", 5010}};
    add_learner.config_replica_id = add_learner.config_member.replica_id;

    LogEntry promote_voter = make_entry(3, 11, WriteOpType::PROMOTE_VOTER, "", "");
    promote_voter.config_replica_id = ReplicaID{101, 7, 10};

    LogEntry remove_member = make_entry(3, 12, WriteOpType::REMOVE_MEMBER, "", "");
    remove_member.config_replica_id = ReplicaID{101, 7, 10};

    const std::vector<LogEntry> expected = {add_learner, promote_voter, remove_member};
    status = engine.wal().append_batch(expected);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    std::vector<LogEntry> actual;
    status = engine.wal().read_batch(actual);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(actual, expected);
}

// 测试一下save_raft_meta和load_raft_meta是否可以正常运行
TEST_F(PersistEngineTest, SaveAndLoadRaftMeta) {
    PersistEngine engine = make_engine();
    Status status = engine.init();
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    RaftMeta expected{9, ReplicaID{9, 2, 1}};

    status = engine.raft_meta().save(expected);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    RaftMeta actual{};
    status = engine.raft_meta().load(actual);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(actual.current_term, expected.current_term);
    ASSERT_TRUE(actual.voted_for.has_value());
    EXPECT_EQ(actual.voted_for.value(), expected.voted_for.value());
}

// MapEngine的状态机应用no-op entry，然后走快照，检测快照内容是否正确
TEST_F(PersistEngineTest, LoadSnapshotMetaWithoutLoadingKvs) {
    PersistEngine engine = make_engine();
    Status status = engine.init();
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    KvStateMachine state_machine(EngineType::MAP);
    ASSERT_TRUE(
        state_machine.apply(make_entry(4, 12, WriteOpType::NONE, "", "")).ok());
    status = test::write_state_machine_snapshot(engine.snapshot(), state_machine);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    SnapshotHeaderPtr actual = std::make_shared<SnapshotHeader>();
    status = engine.snapshot().load_header(actual);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(actual->apply_index, 12);
    EXPECT_EQ(actual->apply_term, 4);
    EXPECT_TRUE(actual->members.empty());
    // EXPECT_FALSE(actual->path.empty());

    size_t kv_count = 0;
    status = test::for_each_snapshot_kv(engine.snapshot(), [&kv_count](const Key&, const Value&) -> Status {
        ++kv_count;
        return Status::OK();
    });
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(kv_count, 0U);
}

// 检测 snapshot 会持久化当前 Raft membership，且 member 区域不会影响后续 KV 读取。
TEST_F(PersistEngineTest, SnapshotPersistsRaftMembersBeforeKvs) {
    PersistEngine engine = make_engine();
    Status status = engine.init();
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    KvStateMachine state_machine(EngineType::MAP);
    ASSERT_TRUE(state_machine.apply(make_entry(5, 20, WriteOpType::PUT, "member-k1", "v1")).ok());
    ASSERT_TRUE(state_machine.apply(make_entry(5, 21, WriteOpType::PUT, "member-k2", "v2")).ok());

    const std::vector<RaftMember> members = {
            RaftMember{PeerMember{"node-20", ReplicaID{101, 7, 20}, Endpoint{"127.0.0.20", 5020}},
                       RaftMemberType::VOTER},
            RaftMember{PeerMember{"node-21", ReplicaID{101, 7, 21}, Endpoint{"127.0.0.21", 5021}},
                       RaftMemberType::LEARNER},
    };

    status = test::write_state_machine_snapshot(engine.snapshot(), state_machine, members);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    SnapshotHeaderPtr actual = std::make_shared<SnapshotHeader>();
    status = engine.snapshot().load_header(actual);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(actual->apply_index, 21);
    EXPECT_EQ(actual->apply_term, 5);
    EXPECT_EQ(actual->members, members);

    std::vector<KV> loaded_kvs;
    status = test::for_each_snapshot_kv(engine.snapshot(), [&loaded_kvs](const Key& key, const Value& value) -> Status {
        loaded_kvs.emplace_back(key, value);
        return Status::OK();
    });
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(loaded_kvs, (std::vector<KV>{{"member-k1", "v1"}, {"member-k2", "v2"}}));
}

// 搞4个entry，然后用 rewrite 只保留快照之后的两个，检测 read_wal_batch 是否正确
TEST_F(PersistEngineTest, RewriteWalKeepsEntriesAfterSnapshotIndex) {
    PersistEngine engine = make_engine();
    Status status = engine.init();
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    const std::vector<LogEntry> entries = {
            make_entry(1, 1, WriteOpType::PUT, "k1", "v1"),
            make_entry(1, 2, WriteOpType::PUT, "k2", "v2"),
            make_entry(2, 3, WriteOpType::DEL, "k1", ""),
            make_entry(2, 4, WriteOpType::PUT, "k3", "v3"),
    };

    status = engine.wal().append_batch(entries);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    status = engine.wal().rewrite({entries[2], entries[3]});
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    std::vector<LogEntry> actual;
    status = engine.wal().read_batch(actual);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    ASSERT_EQ(actual.size(), 2U);
    EXPECT_EQ(actual[0].index, 3);
    EXPECT_EQ(actual[0].key, "k1");
    EXPECT_EQ(actual[1].index, 4);
    EXPECT_EQ(actual[1].key, "k3");
}

// 走正常流程，综合检测一下:
//  放entry。然后状态机应用跑快照，
//  读取快照是否没有问题;
//  跑快照后用 rewrite 对齐 wal（只保留快照之后的日志），这个是否没有问题
//  persist在 rewrite 之后剩下的wal，是否没有问题
TEST_F(PersistEngineTest, WriteSnapshotAndRewriteWalPersistsSnapshotAndShrinksWal) {
    PersistEngine engine = make_engine();
    Status status = engine.init();
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    const std::vector<LogEntry> entries = {
            make_entry(3, 11, WriteOpType::PUT, "a", "1"),
            make_entry(3, 12, WriteOpType::PUT, "b", "2"),
            make_entry(4, 13, WriteOpType::PUT, "c", "3"),
    };
    status = engine.wal().append_batch(entries);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    // Build a state machine that represents the snapshot state at index=12.
    KvStateMachine state_machine(EngineType::MAP);
    ASSERT_TRUE(state_machine.apply(make_entry(3, 11, WriteOpType::PUT, "a", "1")).ok());
    ASSERT_TRUE(state_machine.apply(make_entry(3, 12, WriteOpType::PUT, "b", "2")).ok());

    status = test::write_state_machine_snapshot(engine.snapshot(), state_machine);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    status = engine.wal().rewrite({entries[2]});
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    SnapshotHeaderPtr loaded_snapshot = std::make_shared<SnapshotHeader>();
    status = engine.snapshot().load_header(loaded_snapshot);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(loaded_snapshot->apply_index, 12);
    EXPECT_EQ(loaded_snapshot->apply_term, 3);
    std::vector<KV> loaded_kvs;
    status = test::for_each_snapshot_kv(engine.snapshot(), [&loaded_kvs](const Key& key, const Value& value) -> Status {
        loaded_kvs.emplace_back(key, value);
        return Status::OK();
    });
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(loaded_kvs, (std::vector<KV>{{"a", "1"}, {"b", "2"}}));

    std::vector<LogEntry> actual;
    status = engine.wal().read_batch(actual);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    ASSERT_EQ(actual.size(), 1U);
    EXPECT_EQ(actual[0].index, 13);
    EXPECT_EQ(actual[0].key, "c");
}

// 走快照之后，搞一个新的enginie去进行recover，检测recover的内容是否可以
TEST_F(PersistEngineTest, RecoverLoadsSnapshotMetaAndWalTogether) {
    PersistEngine engine = make_engine();
    Status status = engine.init();
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    const std::vector<LogEntry> wal_entries = {
            make_entry(4, 14, WriteOpType::PUT, "hot", "cold"),
            make_entry(4, 15, WriteOpType::DEL, "trash", ""),
    };
    KvStateMachine state_machine(EngineType::MAP);
    ASSERT_TRUE(state_machine.apply(make_entry(4, 12, WriteOpType::PUT, "alpha", "1")).ok());
    ASSERT_TRUE(state_machine.apply(make_entry(4, 13, WriteOpType::PUT, "beta", "2")).ok());
    const RaftMeta meta{11, ReplicaID{101, 7, 0}};

    status = engine.wal().append_batch(wal_entries);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    status = test::write_state_machine_snapshot(engine.snapshot(), state_machine);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    status = engine.raft_meta().save(meta);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    PersistEngine recovered_engine = make_engine();
    status = recovered_engine.init();
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    PersistEngine::RecoverResult result;
    status = recovered_engine.recover(result);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    ASSERT_NE(result.snapshot, nullptr);
    EXPECT_EQ(result.snapshot->apply_index, 13);
    EXPECT_EQ(result.snapshot->apply_term, 4);
    std::vector<KV> loaded_kvs;
    status = test::for_each_snapshot_kv(recovered_engine.snapshot(),
                                        [&loaded_kvs](const Key& key, const Value& value) -> Status {
        loaded_kvs.emplace_back(key, value);
        return Status::OK();
    });
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(loaded_kvs, (std::vector<KV>{{"alpha", "1"}, {"beta", "2"}}));
    EXPECT_EQ(result.raft_meta.current_term, meta.current_term);
    ASSERT_TRUE(result.raft_meta.voted_for.has_value());
    EXPECT_EQ(result.raft_meta.voted_for.value(), meta.voted_for.value());
    ASSERT_EQ(result.wal_entries.size(), wal_entries.size());
    EXPECT_EQ(result.wal_entries[0].key, wal_entries[0].key);
    EXPECT_EQ(result.wal_entries[1].index, wal_entries[1].index);
    EXPECT_FALSE(result.need_recover);
}

// 检测关于recover的场景下: 对于一个正常的信息是否可以判断recover正确
TEST_F(PersistEngineTest, RecoverLoadsCompleteWalWithoutRepair) {
    const std::vector<LogEntry> entries = {
            make_entry(1, 1, WriteOpType::PUT, "normal-1", "v1"),
            make_entry(1, 2, WriteOpType::PUT, "normal-2", "v2"),
    };
    {
        PersistEngine engine = make_engine();
        Status status = engine.init();
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = engine.wal().append_batch(entries);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = engine.raft_meta().save(RaftMeta{1, std::nullopt});
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = engine.close();
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    }
    const uintmax_t wal_size_before = fs::file_size(wal_path());

    PersistEngine recovered_engine = make_engine();
    Status status = recovered_engine.init();
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    PersistEngine::RecoverResult result;
    status = recovered_engine.recover(result);

    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_FALSE(result.need_recover);
    EXPECT_EQ(result.wal_entries, entries);
    EXPECT_EQ(fs::file_size(wal_path()), wal_size_before);
}

// 写2条entry后追加半条partial record，recover应保留可信前缀并进入recovering
TEST_F(PersistEngineTest, RecoverTruncatesUncommittedPartialWalTail) {
    const std::vector<LogEntry> entries = {
            make_entry(1, 1, WriteOpType::PUT, "tail-1", "v1"),
            make_entry(1, 2, WriteOpType::PUT, "tail-2", "v2"),
    };
    {
        PersistEngine engine = make_engine();
        Status status = engine.init();
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = engine.wal().append_batch(entries);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = engine.raft_meta().save(RaftMeta{1, std::nullopt});
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = engine.close();
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    }
    const uintmax_t valid_wal_size = fs::file_size(wal_path());
    std::vector<uint8_t> partial = encode_wal_record(make_entry(1, 3, WriteOpType::PUT, "tail-3", "v3"));
    partial.resize(sizeof(int32_t) + sizeof(uint32_t) + 3);
    append_raw_bytes(wal_path(), partial);
    ASSERT_GT(fs::file_size(wal_path()), valid_wal_size);

    PersistEngine recovered_engine = make_engine();
    Status status = recovered_engine.init();
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    PersistEngine::RecoverResult result;
    status = recovered_engine.recover(result);

    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_TRUE(result.need_recover);
    EXPECT_EQ(result.wal_entries, entries);
    EXPECT_EQ(fs::file_size(wal_path()), valid_wal_size);

    std::vector<LogEntry> reread;
    status = recovered_engine.wal().read_batch(reread);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(reread, entries);
}

// 写1条entry后追加1条CRC错误entry，recover应保留可信前缀并进入recovering
TEST_F(PersistEngineTest, RecoverTruncatesUncommittedCrcMismatch) {
    const LogEntry committed = make_entry(1, 1, WriteOpType::PUT, "crc-1", "v1");
    {
        PersistEngine engine = make_engine();
        Status status = engine.init();
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = engine.wal().append(committed);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = engine.raft_meta().save(RaftMeta{1, std::nullopt});
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = engine.close();
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    }
    const uintmax_t valid_wal_size = fs::file_size(wal_path());
    append_raw_bytes(wal_path(), encode_wal_record_with_bad_crc(make_entry(1, 2, WriteOpType::PUT, "crc-2", "v2")));

    PersistEngine recovered_engine = make_engine();
    Status status = recovered_engine.init();
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    PersistEngine::RecoverResult result;
    status = recovered_engine.recover(result);

    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_TRUE(result.need_recover);
    EXPECT_EQ(result.wal_entries, std::vector<LogEntry>{committed});
    EXPECT_EQ(fs::file_size(wal_path()), valid_wal_size);
}

// WAL在index=2处CRC损坏，recover应保留损坏前的可信前缀并进入recovering
TEST_F(PersistEngineTest, RecoverTruncatesCommittedCorruptionForRaftCatchUp) {
    const LogEntry entry1 = make_entry(1, 1, WriteOpType::PUT, "committed-1", "v1");
    const std::vector<uint8_t> bad_entry2 =
            encode_wal_record_with_bad_crc(make_entry(1, 2, WriteOpType::PUT, "committed-2", "v2"));
    const std::vector<uint8_t> entry3 = encode_wal_record(make_entry(1, 3, WriteOpType::PUT, "committed-3", "v3"));
    {
        PersistEngine engine = make_engine();
        Status status = engine.init();
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = engine.wal().append(entry1);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = engine.raft_meta().save(RaftMeta{1, std::nullopt});
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = engine.close();
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    }
    append_raw_bytes(wal_path(), bad_entry2);
    append_raw_bytes(wal_path(), entry3);
    const uintmax_t first_record_size = fs::file_size(wal_path()) - bad_entry2.size() - entry3.size();

    PersistEngine recovered_engine = make_engine();
    Status status = recovered_engine.init();
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    PersistEngine::RecoverResult result;
    status = recovered_engine.recover(result);

    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_TRUE(result.need_recover);
    ASSERT_EQ(result.wal_entries.size(), 1U);
    EXPECT_EQ(result.wal_entries[0], entry1);
    EXPECT_EQ(fs::file_size(wal_path()), first_record_size);

    std::vector<LogEntry> reread;
    status = recovered_engine.wal().read_batch(reread);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(reread, std::vector<LogEntry>{entry1});
}

// 第一次recover修复损坏WAL后，第二次recover看到的是已修复的可信WAL。
// 当前设计不持久化recovering marker，因此第二次recover不会继续返回need_recover。
TEST_F(PersistEngineTest, RecoverContinuesCatchUpAfterCrashDuringRecovering) {
    const LogEntry entry1 = make_entry(1, 1, WriteOpType::PUT, "recovering-crash-1", "v1");
    const std::vector<uint8_t> bad_entry2 =
            encode_wal_record_with_bad_crc(make_entry(1, 2, WriteOpType::PUT, "recovering-crash-2", "v2"));
    {
        PersistEngine engine = make_engine();
        Status status = engine.init();
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = engine.wal().append(entry1);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = engine.raft_meta().save(RaftMeta{1, std::nullopt});
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = engine.close();
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    }
    append_raw_bytes(wal_path(), bad_entry2);
    const uintmax_t first_record_size = fs::file_size(wal_path()) - bad_entry2.size();

    {
        PersistEngine first_recover = make_engine();
        Status status = first_recover.init();
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        PersistEngine::RecoverResult result;
        status = first_recover.recover(result);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        EXPECT_TRUE(result.need_recover);
        EXPECT_EQ(fs::file_size(wal_path()), first_record_size);
        status = first_recover.close();
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    }

    PersistEngine second_recover = make_engine();
    Status status = second_recover.init();
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    PersistEngine::RecoverResult result;
    status = second_recover.recover(result);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_FALSE(result.need_recover);
    EXPECT_EQ(result.wal_entries, std::vector<LogEntry>{entry1});
    EXPECT_EQ(fs::file_size(wal_path()), first_record_size);
}

// 写1条entry后追加非法data_len(-1)，recover应保留可信前缀并进入recovering
TEST_F(PersistEngineTest, RecoverHandlesInvalidWalDataLenByCommitIndex) {
    const LogEntry committed = make_entry(1, 1, WriteOpType::PUT, "len-1", "v1");
    {
        PersistEngine engine = make_engine();
        Status status = engine.init();
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = engine.wal().append(committed);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = engine.raft_meta().save(RaftMeta{1, std::nullopt});
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = engine.close();
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    }
    const uintmax_t valid_wal_size = fs::file_size(wal_path());
    std::vector<uint8_t> invalid_len;
    append_raw_value<int32_t>(invalid_len, -1);
    append_raw_bytes(wal_path(), invalid_len);

    PersistEngine recovered_engine = make_engine();
    Status status = recovered_engine.init();
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    PersistEngine::RecoverResult result;
    status = recovered_engine.recover(result);

    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_TRUE(result.need_recover);
    EXPECT_EQ(result.wal_entries, std::vector<LogEntry>{committed});
    EXPECT_EQ(fs::file_size(wal_path()), valid_wal_size);
}

TEST_F(PersistEngineTest, RecoverSnapshotAndWalAfterWriteSnapshot) {
    // write_snapshot 不再截断 WAL，写完 snapshot 后 recover 会过滤掉快照前缀。
    PersistEngine engine = make_engine();
    Status status = engine.init();
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    const std::vector<LogEntry> entries = {
            make_entry(1, 1, WriteOpType::PUT, "k1", "v1"),
            make_entry(1, 2, WriteOpType::PUT, "k2", "v2"),
            make_entry(2, 3, WriteOpType::DEL, "k1", ""),
            make_entry(2, 4, WriteOpType::PUT, "k3", "v3"),
    };

    status = engine.wal().append_batch(entries);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    ASSERT_TRUE(engine.raft_meta().save(RaftMeta{2, std::nullopt}).ok());
    ASSERT_TRUE(engine.close().ok());

    KvStateMachine state_machine(EngineType::MAP);
    for (size_t i = 0; i < 2; ++i) {
        status = state_machine.apply(entries[i]);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    }
    PersistEngine snapshot_writer = make_engine();
    status = snapshot_writer.init();
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    status = test::write_state_machine_snapshot(snapshot_writer.snapshot(), state_machine);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    ASSERT_TRUE(snapshot_writer.close().ok());

    // 重启 recover：snapshot 写完，recover 会过滤掉 snapshot 之前的 WAL 条目
    PersistEngine recovered = make_engine();
    ASSERT_TRUE(recovered.init().ok());
    PersistEngine::RecoverResult res;
    ASSERT_TRUE(recovered.recover(res).ok());
    ASSERT_NE(res.snapshot, nullptr);
    ASSERT_EQ(res.snapshot->apply_index, 2);
    ASSERT_EQ(res.snapshot->apply_term, 1);
    ASSERT_EQ(res.wal_entries.size(), 2U);
    EXPECT_EQ(res.wal_entries[0].index, 3);
    EXPECT_EQ(res.wal_entries[1].index, 4);
    EXPECT_FALSE(res.need_recover);

    std::vector<LogEntry> reread;
    status = recovered.wal().read_batch(reread);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    ASSERT_EQ(reread.size(), 2U);
    EXPECT_EQ(reread[0].index, 3);
    EXPECT_EQ(reread[1].index, 4);
}
// TODO check
TEST_F(PersistEngineTest, RecoverRewritesWalWhenSnapshotPrefixAndCorruptionCoexist) {
    const std::vector<LogEntry> entries = {
            make_entry(1, 1, WriteOpType::PUT, "snap-1", "v1"),
            make_entry(1, 2, WriteOpType::PUT, "snap-2", "v2"),
    };

    {
        PersistEngine engine = make_engine();
        Status status = engine.init();
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = engine.wal().append_batch(entries);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

        KvStateMachine state_machine(EngineType::MAP);
        for (const LogEntry& entry : entries) {
            status = state_machine.apply(entry);
            ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        }
        status = test::write_state_machine_snapshot(engine.snapshot(), state_machine);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        ASSERT_TRUE(engine.close().ok());
    }

    append_raw_bytes(wal_path(), encode_wal_record(entries[0]));
    append_raw_bytes(wal_path(), encode_wal_record(entries[1]));
    append_raw_bytes(wal_path(), encode_wal_record_with_bad_crc(make_entry(1, 3, WriteOpType::PUT, "bad", "bad")));

    PersistEngine recovered = make_engine();
    Status status = recovered.init();
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    PersistEngine::RecoverResult result;
    status = recovered.recover(result);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_TRUE(result.need_recover);
    EXPECT_TRUE(result.wal_entries.empty());

    std::vector<LogEntry> reread;
    status = recovered.wal().read_batch(reread);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_TRUE(reread.empty());
}

// ===== 快照 trailer 损坏负例 =====
// trailer 是"打开即判定完整"的那道闸：尾部 magic、total_len、body_crc 三条
// 各钉一个负例，任何一条对不上都不许被当成有效快照读出来。

namespace {

fs::path snapshot_path_of(const fs::path& base_dir, const ReplicaID& replica_id) {
    return PersistEngine::snapshot_file(PersistEngine::replica_dir(base_dir.string(), replica_id));
}

// 写一份带若干 KV 的正快照，之后各负例在这份"好的"文件上精准改一个字段。
void write_reference_snapshot(const fs::path& base_dir, const ReplicaID& replica_id) {
    KvStateMachine state_machine(EngineType::MAP);
    ASSERT_TRUE(state_machine.apply(LogEntry{1, 1, WriteOpType::PUT, "tk", "tv"}).ok());
    ASSERT_TRUE(state_machine.apply(LogEntry{1, 2, WriteOpType::PUT, "tk2", "tv2"}).ok());
    PersistEngine engine(base_dir.string(), replica_id);
    ASSERT_TRUE(engine.init().ok());
    ASSERT_TRUE(test::write_state_machine_snapshot(engine.snapshot(), state_machine).ok());
    ASSERT_TRUE(engine.close().ok());
}

// 从文件尾部按偏移改一个字节：trailer 定长在最末尾，偏移全从尾部数，
// 前面格式怎么演进都不用跟着改这些用例。
void patch_from_tail(const fs::path& file, size_t offset_from_end, uint8_t xor_mask) {
    int fd = ::open(file.c_str(), O_RDWR);
    ASSERT_NE(fd, -1);
    const off_t size = ::lseek(fd, 0, SEEK_END);
    ASSERT_GT(size, static_cast<off_t>(offset_from_end));
    const off_t at = size - static_cast<off_t>(offset_from_end);
    uint8_t byte = 0;
    ASSERT_EQ(::pread(fd, &byte, 1, at), 1);
    byte ^= xor_mask;
    ASSERT_EQ(::pwrite(fd, &byte, 1, at), 1);
    ::close(fd);
}

void patch_at(const fs::path& file, off_t offset, uint8_t xor_mask) {
    int fd = ::open(file.c_str(), O_RDWR);
    ASSERT_NE(fd, -1);
    uint8_t byte = 0;
    ASSERT_EQ(::pread(fd, &byte, 1, offset), 1);
    byte ^= xor_mask;
    ASSERT_EQ(::pwrite(fd, &byte, 1, offset), 1);
    ::close(fd);
}

// trailer 布局（从尾部数）：magic 在最后 4 字节；total_len 是再往前 8 字节。
// （body_crc32 在 load 时不重放，只有发布路径的 verify 会全文件重算，
// 所以 crc 负例放在接收发布用例里。）
constexpr size_t K_TAIL_MAGIC_FROM_END = 4;
constexpr size_t K_TOTAL_LEN_FROM_END = 4 + 8;

}  // namespace

TEST_F(PersistEngineTest, SnapshotLoadRejectsCorruptedTailMagic) {
    write_reference_snapshot(base_dir_, replica_id_);
    patch_from_tail(snapshot_path_of(base_dir_, replica_id_), K_TAIL_MAGIC_FROM_END, 0x01);

    PersistEngine engine = make_engine();
    ASSERT_TRUE(engine.init().ok());
    SnapshotHeaderPtr snapshot;
    Status status = engine.snapshot().load_header(snapshot);
    EXPECT_TRUE(status.fail()) << test::status_debug_string(status);
}

TEST_F(PersistEngineTest, SnapshotLoadRejectsBogusTotalLen) {
    write_reference_snapshot(base_dir_, replica_id_);
    // 只动 total_len 的最低字节：文件没变，声明的总长对不上实际大小。
    patch_from_tail(snapshot_path_of(base_dir_, replica_id_), K_TOTAL_LEN_FROM_END, 0x10);

    PersistEngine engine = make_engine();
    ASSERT_TRUE(engine.init().ok());
    SnapshotHeaderPtr snapshot;
    Status status = engine.snapshot().load_header(snapshot);
    EXPECT_TRUE(status.fail()) << test::status_debug_string(status);
}

TEST_F(PersistEngineTest, ReceivedSnapshotPublishRejectsBodyCrcMismatch) {
    // 这条走接收发布路径：rename 前的 verify 是全文件 crc 重放，body 里
    // 改一个字节（不动 trailer）也必须被拦住，坏文件不许发布。
    // 参考快照写在另一个目录下（目录名只看 table_id+shard_index，所以换个
    // shard_index）——接收方（make_engine）那边本来就没有已发布快照，
    // "拒绝后不该出现发布文件"这个断言才有意义。
    const ReplicaID source_replica_id{101, 8, 9};
    write_reference_snapshot(base_dir_, source_replica_id);
    const fs::path received = base_dir_ / "received.tmp";
    std::filesystem::copy_file(snapshot_path_of(base_dir_, source_replica_id), received);
    // header = magic(4)+apply_index(8)+apply_term(8)+block_len(4)+count(4)，
    // 偏移 28 起是第一个 KV 的 key 长度字段：改这里 body_crc 必挂而 trailer 完好。
    patch_at(received, 28, 0x01);

    PersistEngine engine = make_engine();
    ASSERT_TRUE(engine.init().ok());
    InstallSnapshotParam param;
    param.from_replica_id = replica_id_;
    param.to_replica_id = replica_id_;
    param.term = 7;
    param.snapshot_index = 2;
    param.snapshot_term = 1;
    param.offset = 0;
    param.done = true;
    std::ifstream in(received, std::ios::binary);
    ASSERT_TRUE(in.is_open());
    param.data = std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());

    ASSERT_TRUE(engine.snapshot().append_snapshot_chunk(param).ok());
    Status status = engine.snapshot().finish_snapshot_receive(param);
    EXPECT_TRUE(status.fail()) << test::status_debug_string(status);
    // 被拒绝的快照绝不能出现在已发布路径上。
    EXPECT_FALSE(fs::exists(snapshot_path_of(base_dir_, replica_id_)));

    // 反过来验证这份字节本身是合法格式（只有 body 被我们改坏）：原样发一份
    // 应当能发布成功，免得负例其实是被格式问题挡下的假绿。
    std::ifstream pristine(snapshot_path_of(base_dir_, source_replica_id), std::ios::binary);
    ASSERT_TRUE(pristine.is_open());
    param.data = std::string((std::istreambuf_iterator<char>(pristine)), std::istreambuf_iterator<char>());
    ASSERT_TRUE(engine.snapshot().append_snapshot_chunk(param).ok());
    ASSERT_TRUE(engine.snapshot().finish_snapshot_receive(param).ok())
            << "intact snapshot must publish; negative case may be failing for the wrong reason";
}

}  // namespace
}  // namespace adviskv::storage
