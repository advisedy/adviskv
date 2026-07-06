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

#include "common/buffer.h"
#include "common/crc.h"
#include "common/status.h"
#include "common/model/type.h"
#include "storage/model/param.h"
#include "storage/raft/state_machine/kv_state_machine.h"
#include "storage/raft/state_machine/state_machine.h"
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
        EncodeBuffer buf;
        buf.write(entry.term);
        buf.write(entry.index);
        buf.write(static_cast<int32_t>(entry.op_type));
        buf.write(entry.key);
        buf.write(entry.value);
        buf.write(entry.config_member.node_id);
        buf.write(entry.config_member.replica_id.table_id);
        buf.write(entry.config_member.replica_id.shard_index);
        buf.write(entry.config_member.replica_id.replica_seq);
        buf.write(entry.config_member.endpoint.ip);
        buf.write(entry.config_member.endpoint.port);
        buf.write(entry.config_replica_id.table_id);
        buf.write(entry.config_replica_id.shard_index);
        buf.write(entry.config_replica_id.replica_seq);
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

    status = engine.append_wal_batch(expected);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    std::vector<LogEntry> actual;
    status = engine.read_wal_batch(actual);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    ASSERT_EQ(actual.size(), expected.size());

    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(actual[i].term, expected[i].term);
        EXPECT_EQ(actual[i].index, expected[i].index);
        EXPECT_EQ(actual[i].op_type, expected[i].op_type);
        EXPECT_EQ(actual[i].key, expected[i].key);
        EXPECT_EQ(actual[i].value, expected[i].value);
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
    status = engine.append_wal_batch(expected);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    std::vector<LogEntry> actual;
    status = engine.read_wal_batch(actual);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(actual, expected);
}

// 测试一下save_raft_meta和load_raft_meta是否可以正常运行
TEST_F(PersistEngineTest, SaveAndLoadRaftMeta) {
    PersistEngine engine = make_engine();
    Status status = engine.init();
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    RaftMeta expected{9, ReplicaID{9, 2, 1}};

    status = engine.save_raft_meta(expected);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    RaftMeta actual{};
    status = engine.load_raft_meta(actual);
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
    status = engine.write_snapshot(state_machine);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    SnapshotPtr actual = std::make_shared<Snapshot>();
    status = engine.load_snapshot_meta(actual);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(actual->apply_index, 12);
    EXPECT_EQ(actual->apply_term, 4);
    EXPECT_TRUE(actual->members.empty());
    // EXPECT_FALSE(actual->path.empty());

    size_t kv_count = 0;
    status = engine.for_each_snapshot_kv([&kv_count](const Key&, const Value&) -> Status {
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

    status = engine.write_snapshot(state_machine, members);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    SnapshotPtr actual = std::make_shared<Snapshot>();
    status = engine.load_snapshot_meta(actual);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(actual->apply_index, 21);
    EXPECT_EQ(actual->apply_term, 5);
    EXPECT_EQ(actual->members, members);

    std::vector<KV> loaded_kvs;
    status = engine.for_each_snapshot_kv([&loaded_kvs](const Key& key, const Value& value) -> Status {
        loaded_kvs.emplace_back(key, value);
        return Status::OK();
    });
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(loaded_kvs, (std::vector<KV>{{"member-k1", "v1"}, {"member-k2", "v2"}}));
}

// 搞4个entry，然后截取前两个，检测截取wal之后，剩余的进行read_wal_batch是否正确
TEST_F(PersistEngineTest, TruncateWalKeepsEntriesAfterSnapshotIndex) {
    PersistEngine engine = make_engine();
    Status status = engine.init();
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    const std::vector<LogEntry> entries = {
            make_entry(1, 1, WriteOpType::PUT, "k1", "v1"),
            make_entry(1, 2, WriteOpType::PUT, "k2", "v2"),
            make_entry(2, 3, WriteOpType::DEL, "k1", ""),
            make_entry(2, 4, WriteOpType::PUT, "k3", "v3"),
    };

    status = engine.append_wal_batch(entries);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    status = engine.truncate_wal(2);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    std::vector<LogEntry> actual;
    status = engine.read_wal_batch(actual);
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
//  跑快照后会截取wal，这个是否没有问题
//  persist在截取wal之后剩下的wal，是否没有问题
TEST_F(PersistEngineTest, WriteSnapshotAndTruncateWalPersistsSnapshotAndShrinksWal) {
    PersistEngine engine = make_engine();
    Status status = engine.init();
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    const std::vector<LogEntry> entries = {
            make_entry(3, 11, WriteOpType::PUT, "a", "1"),
            make_entry(3, 12, WriteOpType::PUT, "b", "2"),
            make_entry(4, 13, WriteOpType::PUT, "c", "3"),
    };
    status = engine.append_wal_batch(entries);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    // Build a state machine that represents the snapshot state at index=12.
    KvStateMachine state_machine(EngineType::MAP);
    ASSERT_TRUE(state_machine.apply(make_entry(3, 11, WriteOpType::PUT, "a", "1")).ok());
    ASSERT_TRUE(state_machine.apply(make_entry(3, 12, WriteOpType::PUT, "b", "2")).ok());

    status = engine.write_snapshot(state_machine);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    status = engine.truncate_wal(state_machine.apply_index());
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    SnapshotPtr loaded_snapshot = std::make_shared<Snapshot>();
    status = engine.load_snapshot_meta(loaded_snapshot);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(loaded_snapshot->apply_index, 12);
    EXPECT_EQ(loaded_snapshot->apply_term, 3);
    std::vector<KV> loaded_kvs;
    status = engine.for_each_snapshot_kv([&loaded_kvs](const Key& key, const Value& value) -> Status {
        loaded_kvs.emplace_back(key, value);
        return Status::OK();
    });
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(loaded_kvs, (std::vector<KV>{{"a", "1"}, {"b", "2"}}));

    std::vector<LogEntry> actual;
    status = engine.read_wal_batch(actual);
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

    status = engine.append_wal_batch(wal_entries);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    status = engine.write_snapshot(state_machine);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    status = engine.save_raft_meta(meta);
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
    status = recovered_engine.for_each_snapshot_kv([&loaded_kvs](const Key& key, const Value& value) -> Status {
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
        status = engine.append_wal_batch(entries);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = engine.save_raft_meta(RaftMeta{1, std::nullopt});
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
        status = engine.append_wal_batch(entries);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = engine.save_raft_meta(RaftMeta{1, std::nullopt});
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
    status = recovered_engine.read_wal_batch(reread);
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
        status = engine.append_wal(committed);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = engine.save_raft_meta(RaftMeta{1, std::nullopt});
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
        status = engine.append_wal(entry1);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = engine.save_raft_meta(RaftMeta{1, std::nullopt});
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
    status = recovered_engine.read_wal_batch(reread);
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
        status = engine.append_wal(entry1);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = engine.save_raft_meta(RaftMeta{1, std::nullopt});
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
        status = engine.append_wal(committed);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = engine.save_raft_meta(RaftMeta{1, std::nullopt});
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

    status = engine.append_wal_batch(entries);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    ASSERT_TRUE(engine.save_raft_meta(RaftMeta{2, std::nullopt}).ok());
    ASSERT_TRUE(engine.close().ok());

    KvStateMachine state_machine(EngineType::MAP);
    for (size_t i = 0; i < 2; ++i) {
        status = state_machine.apply(entries[i]);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    }
    PersistEngine snapshot_writer = make_engine();
    status = snapshot_writer.init();
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    status = snapshot_writer.write_snapshot(state_machine);
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
    status = recovered.read_wal_batch(reread);
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
        status = engine.append_wal_batch(entries);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

        KvStateMachine state_machine(EngineType::MAP);
        for (const LogEntry& entry : entries) {
            status = state_machine.apply(entry);
            ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        }
        status = engine.write_snapshot(state_machine);
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
    status = recovered.read_wal_batch(reread);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_TRUE(reread.empty());
}

}  // namespace
}  // namespace adviskv::storage
