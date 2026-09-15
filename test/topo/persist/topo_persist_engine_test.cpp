#include "topo/persist/topo_persist_engine.h"

#include <filesystem>
#include <fstream>
#include <string>

#include <gtest/gtest.h>

#include "common/status.h"
#include "test_env.h"

namespace fs = std::filesystem;

namespace adviskv::topo {

class TopoPersistEngineTest : public ::testing::Test {
protected:
    void SetUp() override {
        base_dir_ = adviskv::test::make_unique_test_dir("topo_persist", sequence_++);
        ASSERT_TRUE(fs::create_directories(base_dir_)) << base_dir_.string();
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(base_dir_, ec);
    }

    fs::path meta_path() const {
        return base_dir_ / "topo_meta";
    }

    static TopoPersistedRecord make_minimal_record() {
        TopoPersistedRecord record;
        TableState table_state{};
        table_state.desired = TableDesired::PRESENT;
        table_state.phase = TablePhase::READY;
        table_state.update_ts = 100;

        record.tables[101] = Table{
                101,
                TableSpec{"users", 1, "app", 8, 3, "pool-a", "op-table", EngineType::MAP,
                          make_shard_ranges(8)},
                table_state,
        };

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
        record.replicas[replica_id] = Replica{
                replica_id,
                ReplicaSpec{
                        "dc-a",
                        "node-1",
                        EngineType::MAP,
                },
                replica_state,
        };

        record.resource_pools["pool-a"] = ResourcePool{"pool-a"};

        return record;
    }

    static TopoPersistedRecord make_record() {
        TopoPersistedRecord record;
        TableState table_state{};
        table_state.desired = TableDesired::ABSENT;
        table_state.phase = TablePhase::DELETING;
        table_state.last_error_msg = "delete pending";
        table_state.update_ts = 1000;

        record.tables[202] = Table{
                202,
                TableSpec{"orders", 2, "billing", 16, 5, "pool-b", "op-table-2", EngineType::MAP,
                          make_shard_ranges(16)},
                table_state,
        };

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
        record.replicas[leader_id] = Replica{
                leader_id,
                ReplicaSpec{
                        "dc-b",
                        "node-2",
                        EngineType::MAP,
                },
                replica_state,
        };

        record.resource_pools["pool-b"] = ResourcePool{"pool-b"};

        ShardID shard_id{202, 3};
        ReplicaGroup group;
        group.shard_id = shard_id;
        group.mode = ReplicaGroupMode::RAFT_RECONFIG;
        group.target_replica_count = 2;
        group.desired_members = {leader_id, follower_id};
        group.observed_membership_term = 11;
        group.observed_membership_leader = leader_id;
        group.seq_allocator = IDAllocator<ReplicaSeq>(2);
        record.replica_groups[shard_id] = group;

        return record;
    }

    static void expect_record_equal(const TopoPersistedRecord& actual, const TopoPersistedRecord& expected) {
        ASSERT_EQ(actual.tables.size(), expected.tables.size());
        for (const auto& [id, expected_table] : expected.tables) {
            auto it = actual.tables.find(id);
            ASSERT_NE(it, actual.tables.end());
            const Table& actual_table = it->second;
            EXPECT_EQ(actual_table.table_id, expected_table.table_id);
            EXPECT_EQ(actual_table.spec.table_name, expected_table.spec.table_name);
            EXPECT_EQ(actual_table.spec.db_id, expected_table.spec.db_id);
            EXPECT_EQ(actual_table.spec.db_name, expected_table.spec.db_name);
            EXPECT_EQ(actual_table.spec.shard_count, expected_table.spec.shard_count);
            ASSERT_EQ(actual_table.spec.shard_ranges.size(), expected_table.spec.shard_ranges.size());
            for (size_t i = 0; i < actual_table.spec.shard_ranges.size(); ++i) {
                EXPECT_EQ(actual_table.spec.shard_ranges[i].shard_id, expected_table.spec.shard_ranges[i].shard_id);
                EXPECT_EQ(actual_table.spec.shard_ranges[i].range, expected_table.spec.shard_ranges[i].range);
            }
            EXPECT_EQ(actual_table.spec.replica_count, expected_table.spec.replica_count);
            EXPECT_EQ(actual_table.spec.resource_pool, expected_table.spec.resource_pool);
            EXPECT_EQ(actual_table.spec.operation_id, expected_table.spec.operation_id);
            EXPECT_EQ(actual_table.spec.engine_type, expected_table.spec.engine_type);
            EXPECT_EQ(actual_table.state.desired, expected_table.state.desired);
            EXPECT_EQ(actual_table.state.phase, expected_table.state.phase);
            EXPECT_EQ(actual_table.state.last_error_msg, expected_table.state.last_error_msg);
            EXPECT_EQ(actual_table.state.update_ts, expected_table.state.update_ts);
        }

        ASSERT_EQ(actual.replicas.size(), expected.replicas.size());
        for (const auto& [id, expected_replica] : expected.replicas) {
            auto it = actual.replicas.find(id);
            ASSERT_NE(it, actual.replicas.end());
            const Replica& actual_replica = it->second;
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
        for (const auto& [name, expected_pool] : expected.resource_pools) {
            auto it = actual.resource_pools.find(name);
            ASSERT_NE(it, actual.resource_pools.end());
            EXPECT_EQ(it->second.name, expected_pool.name);
        }

        ASSERT_EQ(actual.replica_groups.size(), expected.replica_groups.size());
        for (const auto& [id, expected_group] : expected.replica_groups) {
            auto it = actual.replica_groups.find(id);
            ASSERT_NE(it, actual.replica_groups.end());
            const ReplicaGroup& actual_group = it->second;
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
TEST_F(TopoPersistEngineTest, InitSaveAndLoadRoundTrip) {
    TopoPersistEngine engine(base_dir_.string());
    Status status = engine.init();
    ASSERT_TRUE(status.ok());

    TopoPersistedRecord record = make_record();
    status = engine.save_topo_meta(record);
    ASSERT_TRUE(status.ok());

    TopoPersistedRecord loaded;
    status = engine.load_topo_meta(loaded);
    ASSERT_TRUE(status.ok());

    expect_record_equal(loaded, record);
}

TEST_F(TopoPersistEngineTest, InitRejectsEmptyDataDir) {
    TopoPersistEngine engine("");

    Status status = engine.init();

    EXPECT_EQ(status.code(), StatusCode::INVALID_ARGUMENT);
    EXPECT_EQ(status.msg(), "topo persist engine data_dir is empty");
}

// 检测一下还没有save的时候，load 返回空数据且不报错。
TEST_F(TopoPersistEngineTest, LoadMissingMetaReturnsEmptyRecord) {
    TopoPersistEngine engine(base_dir_.string());
    Status status = engine.init();
    ASSERT_TRUE(status.ok());

    TopoPersistedRecord loaded = make_record();
    status = engine.load_topo_meta(loaded);
    ASSERT_TRUE(status.ok());

    EXPECT_TRUE(loaded.tables.empty());
    EXPECT_TRUE(loaded.replicas.empty());
    EXPECT_TRUE(loaded.resource_pools.empty());
    EXPECT_TRUE(loaded.replica_groups.empty());
}

// 检测一下空的save和load是否正确
TEST_F(TopoPersistEngineTest, SaveAndLoadEmptyRecord) {
    TopoPersistEngine engine(base_dir_.string());
    Status status = engine.init();
    ASSERT_TRUE(status.ok());

    TopoPersistedRecord record;
    status = engine.save_topo_meta(record);
    ASSERT_TRUE(status.ok());

    TopoPersistedRecord loaded = make_record();
    status = engine.load_topo_meta(loaded);
    ASSERT_TRUE(status.ok());

    expect_record_equal(loaded, record);
}

// 检测一下当save了多次之后，load出来的是最新save的数据
TEST_F(TopoPersistEngineTest, OverwriteSaveKeepsLatestRecord) {
    TopoPersistEngine engine(base_dir_.string());
    Status status = engine.init();
    ASSERT_TRUE(status.ok());

    TopoPersistedRecord old = make_record();
    old.tables.begin()->second.spec.table_name = "old_orders";
    status = engine.save_topo_meta(old);
    ASSERT_TRUE(status.ok());

    TopoPersistedRecord latest = make_record();
    status = engine.save_topo_meta(latest);
    ASSERT_TRUE(status.ok());

    TopoPersistedRecord loaded;
    status = engine.load_topo_meta(loaded);
    ASSERT_TRUE(status.ok());

    expect_record_equal(loaded, latest);
}

// 检测没有 init 时进行 save 或 load 会返回明确的未初始化错误。
TEST_F(TopoPersistEngineTest, SaveOrLoadWithoutInitReturnsNotInit) {
    TopoPersistEngine engine(base_dir_.string());

    Status status = engine.save_topo_meta(make_record());
    EXPECT_EQ(status.code(), StatusCode::NOT_INIT);

    TopoPersistedRecord loaded;
    status = engine.load_topo_meta(loaded);
    EXPECT_EQ(status.code(), StatusCode::NOT_INIT);
}

// 检测 load 一段错误数据时返回错误。
TEST_F(TopoPersistEngineTest, LoadCorruptedTopoMetaFails) {
    {
        std::ofstream out(meta_path(), std::ios::binary);
        ASSERT_TRUE(out.is_open());
        out << "corrupted topo meta";
    }

    TopoPersistEngine engine(base_dir_.string());
    Status status = engine.init();
    ASSERT_TRUE(status.ok());

    TopoPersistedRecord loaded;
    status = engine.load_topo_meta(loaded);
    EXPECT_TRUE(status.fail());
}

TEST_F(TopoPersistEngineTest, ReopenKeepsUnevenRangesAndSparseShardIds) {
    auto record = make_minimal_record();
    record.tables.at(101).spec.shard_count = 2;
    record.tables.at(101).spec.shard_ranges = {{7, {0, 12345}}, {91, {12345, K_HASH_BUCKET_COUNT}}};
    {
        TopoPersistEngine engine(base_dir_.string());
        ASSERT_TRUE(engine.init().ok());
        ASSERT_TRUE(engine.save_topo_meta(record).ok());
    }
    TopoPersistEngine reopened(base_dir_.string());
    ASSERT_TRUE(reopened.init().ok());
    TopoPersistedRecord loaded;
    ASSERT_TRUE(reopened.load_topo_meta(loaded).ok());
    expect_record_equal(loaded, record);
}

TEST_F(TopoPersistEngineTest, InvalidRangesCannotOverwriteValidMetadata) {
    auto record = make_minimal_record();
    TopoPersistEngine engine(base_dir_.string());
    ASSERT_TRUE(engine.init().ok());
    ASSERT_TRUE(engine.save_topo_meta(record).ok());
    auto invalid = record;
    invalid.tables.at(101).spec.shard_ranges[1].range.start++;
    EXPECT_TRUE(engine.save_topo_meta(invalid).fail());
    TopoPersistedRecord loaded;
    ASSERT_TRUE(engine.load_topo_meta(loaded).ok());
    expect_record_equal(loaded, record);
}

}  // namespace adviskv::topo
