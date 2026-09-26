#include "meta/persist/meta_persist_engine.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>

#include <gtest/gtest.h>

#include "common/status.h"
#include "test_env.h"

namespace fs = std::filesystem;

namespace adviskv::meta {

class MetaPersistEngineTest : public ::testing::Test {
protected:
    void SetUp() override {
        base_dir_ = adviskv::test::make_unique_test_dir("meta_persist", sequence_++);
        ASSERT_TRUE(fs::create_directories(base_dir_)) << base_dir_.string();
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(base_dir_, ec);
    }

    fs::path meta_path() const { return base_dir_ / "meta.json"; }

    static MetaPersistedRecord make_minimal_record() {
        MetaPersistedRecord record;
        record.next_db_id = 2;
        record.next_table_id = 102;
        record.databases.push_back(Database{1, "app", "zone-a"});
        TableState table_state{};
        table_state.desired = TableDesired::PRESENT;
        table_state.phase = TablePhase::READY;
        table_state.update_ts = 100;

        record.tables.push_back(Table{
                101,
                TableSpec{"users", 1, "app", 8, 3, "pool-a", "op-table", EngineType::MAP, make_shard_ranges(8)},
                table_state,
        });

        ReplicaID replica_id{101, 2, 0};
        ReplicaState replica_state{};
        replica_state.desired = ReplicaDesired::PRESENT;
        replica_state.phase = ReplicaPhase::READY;
        replica_state.observed_raft_role = ReplicaRole::LEADER;
        replica_state.observed_member_type = RaftMemberType::VOTER;
        replica_state.observed_endpoint = Endpoint{"127.0.0.1", 9000};
        replica_state.observed_storage_status = StorageReplicaStatus::READY;
        replica_state.update_ts = 300;
        replica_state.term = 4;
        record.replicas.push_back(Replica{
                replica_id,
                ReplicaSpec{
                        "dc-a",
                        "node-1",
                        EngineType::MAP,
                },
                replica_state,
        });

        record.resource_pools.push_back(ResourcePool{"pool-a"});

        return record;
    }

    static MetaPersistedRecord make_record() {
        MetaPersistedRecord record;
        record.next_db_id = 3;
        record.next_table_id = 203;
        record.databases.push_back(Database{2, "billing", "zone-b"});
        TableState table_state{};
        table_state.desired = TableDesired::ABSENT;
        table_state.phase = TablePhase::DELETING;
        table_state.last_error_msg = "delete pending";
        table_state.update_ts = 1000;

        record.tables.push_back(Table{
                202,
                TableSpec{"orders", 2, "billing", 16, 5, "pool-b", "op-table-2", EngineType::MAP,
                          make_shard_ranges(16)},
                table_state,
        });

        ReplicaID leader_id{202, 3, 0};
        ReplicaID follower_id{202, 3, 1};
        ReplicaState replica_state{};
        replica_state.desired = ReplicaDesired::ABSENT;
        replica_state.phase = ReplicaPhase::DELETING;
        replica_state.observed_raft_role = ReplicaRole::FOLLOWER;
        replica_state.observed_member_type = RaftMemberType::NON_MEMBER;
        replica_state.observed_endpoint = Endpoint{"127.0.0.2", 9100};
        replica_state.observed_storage_status = StorageReplicaStatus::READY;
        replica_state.observed_no_exist = true;
        replica_state.last_error_msg = "replica draining";
        replica_state.update_ts = 3000;
        replica_state.term = 11;
        record.replicas.push_back(Replica{
                leader_id,
                ReplicaSpec{
                        "dc-b",
                        "node-2",
                        EngineType::MAP,
                },
                replica_state,
        });
        ReplicaState follower_state = replica_state;
        follower_state.observed_raft_role = ReplicaRole::FOLLOWER;
        record.replicas.push_back(Replica{
                follower_id,
                ReplicaSpec{
                        "dc-b",
                        "node-3",
                        EngineType::MAP,
                },
                follower_state,
        });

        record.resource_pools.push_back(ResourcePool{"pool-b"});

        ShardID shard_id{202, 3};
        ReplicaGroup group;
        group.shard_id = shard_id;
        group.mode = ReplicaGroupMode::RAFT_RECONFIG;
        group.target_replica_count = 2;
        group.desired_members = {leader_id, follower_id};
        group.observed_membership_term = 11;
        group.observed_membership_leader = leader_id;
        group.seq_allocator = IDAllocator<ReplicaSeq>(2);
        record.replica_groups.push_back(group);

        return record;
    }

    static void expect_record_equal(const MetaPersistedRecord& actual, const MetaPersistedRecord& expected) {
        EXPECT_EQ(actual.next_db_id, expected.next_db_id);
        EXPECT_EQ(actual.next_table_id, expected.next_table_id);
        ASSERT_EQ(actual.databases.size(), expected.databases.size());
        for (const auto& expected_db : expected.databases) {
            auto it = std::find_if(actual.databases.begin(), actual.databases.end(), [&](const Database& db) {
                return db.db_id == expected_db.db_id;
            });
            ASSERT_NE(it, actual.databases.end());
            EXPECT_EQ(it->db_id, expected_db.db_id);
            EXPECT_EQ(it->name, expected_db.name);
            EXPECT_EQ(it->zone, expected_db.zone);
        }
        ASSERT_EQ(actual.tables.size(), expected.tables.size());
        for (const auto& expected_table : expected.tables) {
            auto it = std::find_if(actual.tables.begin(), actual.tables.end(), [&](const Table& table) {
                return table.table_id == expected_table.table_id;
            });
            ASSERT_NE(it, actual.tables.end());
            const Table& actual_table = *it;
            EXPECT_EQ(actual_table.table_id, expected_table.table_id);
            EXPECT_EQ(actual_table.spec.name, expected_table.spec.name);
            EXPECT_EQ(actual_table.spec.db_id, expected_table.spec.db_id);
            EXPECT_EQ(actual_table.spec.db_name, expected_table.spec.db_name);
            EXPECT_EQ(actual_table.shard_count(), expected_table.shard_count());
            ASSERT_EQ(actual_table.spec.shard_ranges.size(), expected_table.spec.shard_ranges.size());
            for (size_t i = 0; i < actual_table.spec.shard_ranges.size(); ++i) {
                EXPECT_EQ(actual_table.spec.shard_ranges[i].shard_id, expected_table.spec.shard_ranges[i].shard_id);
                EXPECT_EQ(actual_table.spec.shard_ranges[i].range, expected_table.spec.shard_ranges[i].range);
            }
            EXPECT_EQ(actual_table.spec.target_replica_count, expected_table.spec.target_replica_count);
            EXPECT_EQ(actual_table.spec.resource_pool, expected_table.spec.resource_pool);
            EXPECT_EQ(actual_table.state.operation_id, expected_table.state.operation_id);
            EXPECT_EQ(actual_table.spec.engine_type, expected_table.spec.engine_type);
            EXPECT_EQ(actual_table.state.desired, expected_table.state.desired);
            EXPECT_EQ(actual_table.state.phase, expected_table.state.phase);
            EXPECT_EQ(actual_table.state.last_error_msg, expected_table.state.last_error_msg);
            EXPECT_EQ(actual_table.state.update_ts, expected_table.state.update_ts);
        }

        ASSERT_EQ(actual.replicas.size(), expected.replicas.size());
        for (const auto& expected_replica : expected.replicas) {
            auto it = std::find_if(actual.replicas.begin(), actual.replicas.end(), [&](const Replica& replica) {
                return replica.replica_id == expected_replica.replica_id;
            });
            ASSERT_NE(it, actual.replicas.end());
            const Replica& actual_replica = *it;
            EXPECT_EQ(actual_replica.replica_id, expected_replica.replica_id);
            EXPECT_EQ(actual_replica.spec.dc, expected_replica.spec.dc);
            EXPECT_EQ(actual_replica.spec.assign_node_id, expected_replica.spec.assign_node_id);
            EXPECT_EQ(actual_replica.spec.engine_type, expected_replica.spec.engine_type);
            EXPECT_EQ(actual_replica.state.desired, expected_replica.state.desired);
            EXPECT_EQ(actual_replica.state.phase, expected_replica.state.phase);
            EXPECT_EQ(actual_replica.state.observed_raft_role, expected_replica.state.observed_raft_role);
            EXPECT_EQ(actual_replica.state.observed_member_type, expected_replica.state.observed_member_type);
            EXPECT_EQ(actual_replica.state.observed_endpoint, expected_replica.state.observed_endpoint);
            EXPECT_EQ(actual_replica.state.observed_storage_status, expected_replica.state.observed_storage_status);
            EXPECT_EQ(actual_replica.state.observed_no_exist, expected_replica.state.observed_no_exist);
            EXPECT_EQ(actual_replica.state.last_error_msg, expected_replica.state.last_error_msg);
            EXPECT_EQ(actual_replica.state.update_ts, expected_replica.state.update_ts);
            EXPECT_EQ(actual_replica.state.term, expected_replica.state.term);
        }

        ASSERT_EQ(actual.resource_pools.size(), expected.resource_pools.size());
        for (const auto& expected_pool : expected.resource_pools) {
            auto it = std::find_if(actual.resource_pools.begin(), actual.resource_pools.end(), [&](const ResourcePool& pool) {
                return pool.name == expected_pool.name;
            });
            ASSERT_NE(it, actual.resource_pools.end());
            EXPECT_EQ(it->name, expected_pool.name);
        }

        ASSERT_EQ(actual.replica_groups.size(), expected.replica_groups.size());
        for (const auto& expected_group : expected.replica_groups) {
            auto it = std::find_if(actual.replica_groups.begin(), actual.replica_groups.end(), [&](const ReplicaGroup& group) {
                return group.shard_id == expected_group.shard_id;
            });
            ASSERT_NE(it, actual.replica_groups.end());
            const ReplicaGroup& actual_group = *it;
            EXPECT_EQ(actual_group.shard_id, expected_group.shard_id);
            EXPECT_EQ(actual_group.mode, expected_group.mode);
            EXPECT_EQ(actual_group.target_replica_count, expected_group.target_replica_count);
            EXPECT_EQ(actual_group.desired_members, expected_group.desired_members);
            EXPECT_EQ(actual_group.observed_membership_term, expected_group.observed_membership_term);
            EXPECT_EQ(actual_group.observed_membership_leader, expected_group.observed_membership_leader);
            EXPECT_EQ(actual_group.seq_allocator.current_id(), expected_group.seq_allocator.current_id());
        }
    }

    static inline int sequence_{0};
    fs::path base_dir_;
};

// 检测一下正常的 init、save、load 流程。
TEST_F(MetaPersistEngineTest, InitSaveAndLoadRoundTrip) {
    MetaPersistEngine engine(base_dir_.string());
    Status status = engine.init();
    ASSERT_TRUE(status.ok());

    MetaPersistedRecord record = make_record();
    status = engine.save_meta(record);
    ASSERT_TRUE(status.ok());

    MetaPersistedRecord loaded;
    status = engine.load_meta(loaded);
    ASSERT_TRUE(status.ok());

    expect_record_equal(loaded, record);
}

TEST_F(MetaPersistEngineTest, InitRejectsEmptyDataDir) {
    MetaPersistEngine engine("");

    Status status = engine.init();

    EXPECT_EQ(status.code(), StatusCode::INVALID_ARGUMENT);
    EXPECT_EQ(status.msg(), "meta persist engine data_dir is empty");
}

// 检测一下还没有save的时候，load 返回空数据且不报错。
TEST_F(MetaPersistEngineTest, LoadMissingMetaReturnsEmptyRecord) {
    MetaPersistEngine engine(base_dir_.string());
    Status status = engine.init();
    ASSERT_TRUE(status.ok());

    MetaPersistedRecord loaded = make_record();
    status = engine.load_meta(loaded);
    ASSERT_TRUE(status.ok());

    EXPECT_TRUE(loaded.tables.empty());
    EXPECT_TRUE(loaded.replicas.empty());
    EXPECT_TRUE(loaded.resource_pools.empty());
    EXPECT_TRUE(loaded.replica_groups.empty());
}

// 检测一下空的save和load是否正确
TEST_F(MetaPersistEngineTest, SaveAndLoadEmptyRecord) {
    MetaPersistEngine engine(base_dir_.string());
    Status status = engine.init();
    ASSERT_TRUE(status.ok());

    MetaPersistedRecord record;
    status = engine.save_meta(record);
    ASSERT_TRUE(status.ok());

    MetaPersistedRecord loaded = make_record();
    status = engine.load_meta(loaded);
    ASSERT_TRUE(status.ok());

    expect_record_equal(loaded, record);
}

// 检测一下当save了多次之后，load出来的是最新save的数据
TEST_F(MetaPersistEngineTest, OverwriteSaveKeepsLatestRecord) {
    MetaPersistEngine engine(base_dir_.string());
    Status status = engine.init();
    ASSERT_TRUE(status.ok());

    MetaPersistedRecord old = make_record();
    old.tables.front().spec.name = "old_orders";
    status = engine.save_meta(old);
    ASSERT_TRUE(status.ok());

    MetaPersistedRecord latest = make_record();
    status = engine.save_meta(latest);
    ASSERT_TRUE(status.ok());

    MetaPersistedRecord loaded;
    status = engine.load_meta(loaded);
    ASSERT_TRUE(status.ok());

    expect_record_equal(loaded, latest);
}

// 检测没有 init 时进行 save 或 load 会返回明确的未初始化错误。
TEST_F(MetaPersistEngineTest, SaveOrLoadWithoutInitReturnsNotInit) {
    MetaPersistEngine engine(base_dir_.string());

    Status status = engine.save_meta(make_record());
    EXPECT_EQ(status.code(), StatusCode::NOT_INIT);

    MetaPersistedRecord loaded;
    status = engine.load_meta(loaded);
    EXPECT_EQ(status.code(), StatusCode::NOT_INIT);
}

// 检测 load 一段错误数据时返回错误。
TEST_F(MetaPersistEngineTest, LoadCorruptedMetaFails) {
    {
        std::ofstream out(meta_path(), std::ios::binary);
        ASSERT_TRUE(out.is_open());
        out << "corrupted meta";
    }

    MetaPersistEngine engine(base_dir_.string());
    Status status = engine.init();
    ASSERT_TRUE(status.ok());

    MetaPersistedRecord loaded;
    status = engine.load_meta(loaded);
    EXPECT_TRUE(status.fail());
}

TEST_F(MetaPersistEngineTest, LoadEmptyFileFails) {
    {
        std::ofstream out(meta_path(), std::ios::binary);
        ASSERT_TRUE(out.is_open());
    }

    MetaPersistEngine engine(base_dir_.string());
    ASSERT_TRUE(engine.init().ok());
    MetaPersistedRecord loaded;
    EXPECT_TRUE(engine.load_meta(loaded).fail());
}

TEST_F(MetaPersistEngineTest, LoadRejectsStaleAllocator) {
    MetaPersistEngine engine(base_dir_.string());
    ASSERT_TRUE(engine.init().ok());
    ASSERT_TRUE(engine.save_meta(make_minimal_record()).ok());
    std::ifstream in(meta_path());
    std::string payload((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    const std::string from = "\"next_table_id\": 102";
    const auto pos = payload.find(from);
    ASSERT_NE(pos, std::string::npos);
    payload.replace(pos, from.size(), "\"next_table_id\": 0");
    std::ofstream out(meta_path(), std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(out.is_open());
    out << payload;
    out.close();
    MetaPersistedRecord loaded;
    EXPECT_TRUE(engine.load_meta(loaded).fail());
}

TEST_F(MetaPersistEngineTest, ReopenKeepsUnevenRangesAndSparseShardIds) {
    auto record = make_minimal_record();
    record.tables.front().spec.shard_ranges = {{7, {0, 12345}}, {91, {12345, K_HASH_BUCKET_COUNT}}};
    {
        MetaPersistEngine engine(base_dir_.string());
        ASSERT_TRUE(engine.init().ok());
        ASSERT_TRUE(engine.save_meta(record).ok());
    }
    MetaPersistEngine reopened(base_dir_.string());
    ASSERT_TRUE(reopened.init().ok());
    MetaPersistedRecord loaded;
    ASSERT_TRUE(reopened.load_meta(loaded).ok());
    expect_record_equal(loaded, record);
}

TEST_F(MetaPersistEngineTest, InvalidRangesCannotOverwriteValidMetadata) {
    auto record = make_minimal_record();
    MetaPersistEngine engine(base_dir_.string());
    ASSERT_TRUE(engine.init().ok());
    ASSERT_TRUE(engine.save_meta(record).ok());
    auto invalid = record;
    invalid.tables.front().spec.shard_ranges[1].range.start++;
    EXPECT_TRUE(engine.save_meta(invalid).fail());
    MetaPersistedRecord loaded;
    ASSERT_TRUE(engine.load_meta(loaded).ok());
    expect_record_equal(loaded, record);
}

}  // namespace adviskv::meta
