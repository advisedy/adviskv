#include "sdm/persist/sdm_persist_engine.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>

#include "common/status.h"
#include "test_env.h"

namespace fs = std::filesystem;

namespace adviskv::sdm {

class SdmPersistEngineTest : public ::testing::Test {
   protected:
    void SetUp() override {
        base_dir_ =
            adviskv::test::make_unique_test_dir("sdm_persist", sequence_++);
        ASSERT_TRUE(fs::create_directories(base_dir_)) << base_dir_.string();
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(base_dir_, ec);
    }

    fs::path meta_path() const { return base_dir_ / "sdm_meta"; }

    static SdmPersistedRecord make_minimal_record() {
        SdmPersistedRecord record;

        record.tables[101] = Table{
            .table_id = 101,
            .spec =
                TableSpec{
                    .table_name = "users",
                    .db_id = 1,
                    .db_name = "app",
                    .shard_count = 8,
                    .replica_count = 3,
                    .resource_pool = "pool-a",
                    .operation_id = "op-table",
                },
            .state =
                TableState{
                    .desired = TableDesired::PRESENT,
                    .phase = TablePhase::READY,
                    .last_error_msg = "",
                    .update_ts = 100,
                },
        };

        record.nodes["node-1"] = Node{
            .id = "node-1",
            .spec =
                NodeSpec{
                    .resource_pool = "pool-a",
                    .dc = "dc-a",
                    .status = NodeStatus::ONLINE,
                },
            .state =
                NodeState{
                    .endpoint = Endpoint{.ip = "127.0.0.1", .port = 9000},
                    .last_heartbeat_ts = 200,
                },
            .derived =
                NodeDerived{
                    .owned_replica_count = 3,
                    .owned_leader_count = 1,
                },
        };

        ReplicaID replica_id{
            .table_id = 101, .shard_index = 2, .replica_index = 0};
        record.replicas[replica_id] = Replica{
            .replica_id = replica_id,
            .spec =
                ReplicaSpec{
                    .dc = "dc-a",
                    .assign_node_id = "node-1",
                    .engine_type = EngineType::MAP,
                    .members =
                        {
                            PeerMember{
                                .node_id = "node-1",
                                .replica_id = replica_id,
                                .endpoint =
                                    Endpoint{.ip = "127.0.0.1", .port = 9000},
                            },
                        },
                },
            .state =
                ReplicaState{
                    .desired = ReplicaDesired::PRESENT,
                    .phase = ReplicaPhase::READY,
                    .observed_role = ReplicaRole::LEADER,
                    .observed_endpoint =
                        Endpoint{.ip = "127.0.0.1", .port = 9000},
                    .last_error_msg = "",
                    .update_ts = 300,
                    .term = 4,
                },
        };

        record.resource_pools["pool-a"] = ResourcePool{.name = "pool-a"};

        ShardID shard_id{.table_id = 101, .shard_index = 2};
        record.shard_routes[shard_id] = ShardRoute{
            .shard_id = shard_id,
            .replicas =
                {
                    RouteEntry{
                        .replica_id = replica_id,
                        .node_id = "node-1",
                        .ip = "127.0.0.1",
                        .port = 9000,
                        .role = ReplicaRole::LEADER,
                        .term = 4,
                    },
                },
        };

        return record;
    }

    static SdmPersistedRecord make_record() {
        SdmPersistedRecord record;

        record.tables[202] = Table{
            .table_id = 202,
            .spec =
                TableSpec{
                    .table_name = "orders",
                    .db_id = 2,
                    .db_name = "billing",
                    .shard_count = 16,
                    .replica_count = 5,
                    .resource_pool = "pool-b",
                    .operation_id = "op-table-2",
                },
            .state =
                TableState{
                    .desired = TableDesired::ABSENT,
                    .phase = TablePhase::DELETING,
                    .last_error_msg = "delete pending",
                    .update_ts = 1000,
                },
        };

        record.nodes["node-2"] = Node{
            .id = "node-2",
            .spec =
                NodeSpec{
                    .resource_pool = "pool-b",
                    .dc = "dc-b",
                    .status = NodeStatus::SUSPECT,
                },
            .state =
                NodeState{
                    .endpoint = Endpoint{.ip = "127.0.0.2", .port = 9100},
                    .last_heartbeat_ts = 2000,
                },
            .derived =
                NodeDerived{
                    .owned_replica_count = 7,
                    .owned_leader_count = 2,
                },
        };

        ReplicaID leader_id{
            .table_id = 202, .shard_index = 3, .replica_index = 0};
        ReplicaID follower_id{
            .table_id = 202, .shard_index = 3, .replica_index = 1};
        record.replicas[leader_id] = Replica{
            .replica_id = leader_id,
            .spec =
                ReplicaSpec{
                    .dc = "dc-b",
                    .assign_node_id = "node-2",
                    .engine_type = EngineType::ROCKSDB,
                    .members =
                        {
                            PeerMember{
                                .node_id = "node-2",
                                .replica_id = leader_id,
                                .endpoint =
                                    Endpoint{.ip = "127.0.0.2", .port = 9100},
                            },
                            PeerMember{
                                .node_id = "node-3",
                                .replica_id = follower_id,
                                .endpoint =
                                    Endpoint{.ip = "127.0.0.3", .port = 9101},
                            },
                        },
                },
            .state =
                ReplicaState{
                    .desired = ReplicaDesired::ABSENT,
                    .phase = ReplicaPhase::DELETING,
                    .observed_role = ReplicaRole::FOLLOWER,
                    .observed_endpoint =
                        Endpoint{.ip = "127.0.0.2", .port = 9100},
                    .last_error_msg = "replica draining",
                    .update_ts = 3000,
                    .term = 11,
                },
        };

        record.resource_pools["pool-b"] = ResourcePool{.name = "pool-b"};

        ShardID shard_id{.table_id = 202, .shard_index = 3};
        record.shard_routes[shard_id] = ShardRoute{
            .shard_id = shard_id,
            .replicas =
                {
                    RouteEntry{
                        .replica_id = leader_id,
                        .node_id = "node-2",
                        .ip = "127.0.0.2",
                        .port = 9100,
                        .role = ReplicaRole::LEADER,
                        .term = 11,
                    },
                    RouteEntry{
                        .replica_id = follower_id,
                        .node_id = "node-3",
                        .ip = "127.0.0.3",
                        .port = 9101,
                        .role = ReplicaRole::FOLLOWER,
                        .term = 10,
                    },
                },
        };

        return record;
    }

    static void expect_record_equal(const SdmPersistedRecord& actual,
                                    const SdmPersistedRecord& expected) {
        ASSERT_EQ(actual.tables.size(), expected.tables.size());
        for (const auto& [id, expected_table] : expected.tables) {
            auto it = actual.tables.find(id);
            ASSERT_NE(it, actual.tables.end());
            const Table& actual_table = it->second;
            EXPECT_EQ(actual_table.table_id, expected_table.table_id);
            EXPECT_EQ(actual_table.spec.table_name,
                      expected_table.spec.table_name);
            EXPECT_EQ(actual_table.spec.db_id, expected_table.spec.db_id);
            EXPECT_EQ(actual_table.spec.db_name, expected_table.spec.db_name);
            EXPECT_EQ(actual_table.spec.shard_count,
                      expected_table.spec.shard_count);
            EXPECT_EQ(actual_table.spec.replica_count,
                      expected_table.spec.replica_count);
            EXPECT_EQ(actual_table.spec.resource_pool,
                      expected_table.spec.resource_pool);
            EXPECT_EQ(actual_table.spec.operation_id,
                      expected_table.spec.operation_id);
            EXPECT_EQ(actual_table.state.desired, expected_table.state.desired);
            EXPECT_EQ(actual_table.state.phase, expected_table.state.phase);
            EXPECT_EQ(actual_table.state.last_error_msg,
                      expected_table.state.last_error_msg);
            EXPECT_EQ(actual_table.state.update_ts,
                      expected_table.state.update_ts);
        }

        ASSERT_EQ(actual.nodes.size(), expected.nodes.size());
        for (const auto& [id, expected_node] : expected.nodes) {
            auto it = actual.nodes.find(id);
            ASSERT_NE(it, actual.nodes.end());
            const Node& actual_node = it->second;
            EXPECT_EQ(actual_node.id, expected_node.id);
            EXPECT_EQ(actual_node.spec.resource_pool,
                      expected_node.spec.resource_pool);
            EXPECT_EQ(actual_node.spec.dc, expected_node.spec.dc);
            EXPECT_EQ(actual_node.spec.status, expected_node.spec.status);
            EXPECT_EQ(actual_node.state.endpoint, expected_node.state.endpoint);
            EXPECT_EQ(actual_node.state.last_heartbeat_ts,
                      expected_node.state.last_heartbeat_ts);
            EXPECT_EQ(actual_node.derived.owned_replica_count,
                      expected_node.derived.owned_replica_count);
            EXPECT_EQ(actual_node.derived.owned_leader_count,
                      expected_node.derived.owned_leader_count);
        }

        ASSERT_EQ(actual.replicas.size(), expected.replicas.size());
        for (const auto& [id, expected_replica] : expected.replicas) {
            auto it = actual.replicas.find(id);
            ASSERT_NE(it, actual.replicas.end());
            const Replica& actual_replica = it->second;
            EXPECT_EQ(actual_replica.replica_id, expected_replica.replica_id);
            EXPECT_EQ(actual_replica.spec.dc, expected_replica.spec.dc);
            EXPECT_EQ(actual_replica.spec.assign_node_id,
                      expected_replica.spec.assign_node_id);
            EXPECT_EQ(actual_replica.spec.engine_type,
                      expected_replica.spec.engine_type);
            ASSERT_EQ(actual_replica.spec.members.size(),
                      expected_replica.spec.members.size());
            for (size_t i = 0; i < expected_replica.spec.members.size(); ++i) {
                EXPECT_EQ(actual_replica.spec.members[i],
                          expected_replica.spec.members[i]);
            }
            EXPECT_EQ(actual_replica.state.desired,
                      expected_replica.state.desired);
            EXPECT_EQ(actual_replica.state.phase, expected_replica.state.phase);
            EXPECT_EQ(actual_replica.state.observed_role,
                      expected_replica.state.observed_role);
            EXPECT_EQ(actual_replica.state.observed_endpoint,
                      expected_replica.state.observed_endpoint);
            EXPECT_EQ(actual_replica.state.last_error_msg,
                      expected_replica.state.last_error_msg);
            EXPECT_EQ(actual_replica.state.update_ts,
                      expected_replica.state.update_ts);
            EXPECT_EQ(actual_replica.state.term, expected_replica.state.term);
        }

        ASSERT_EQ(actual.resource_pools.size(), expected.resource_pools.size());
        for (const auto& [name, expected_pool] : expected.resource_pools) {
            auto it = actual.resource_pools.find(name);
            ASSERT_NE(it, actual.resource_pools.end());
            EXPECT_EQ(it->second.name, expected_pool.name);
        }

        ASSERT_EQ(actual.shard_routes.size(), expected.shard_routes.size());
        for (const auto& [id, expected_route] : expected.shard_routes) {
            auto it = actual.shard_routes.find(id);
            ASSERT_NE(it, actual.shard_routes.end());
            const ShardRoute& actual_route = it->second;
            EXPECT_EQ(actual_route.shard_id, expected_route.shard_id);
            ASSERT_EQ(actual_route.replicas.size(),
                      expected_route.replicas.size());
            for (size_t i = 0; i < expected_route.replicas.size(); ++i) {
                const RouteEntry& actual_entry = actual_route.replicas[i];
                const RouteEntry& expected_entry = expected_route.replicas[i];
                EXPECT_EQ(actual_entry.replica_id, expected_entry.replica_id);
                EXPECT_EQ(actual_entry.node_id, expected_entry.node_id);
                EXPECT_EQ(actual_entry.ip, expected_entry.ip);
                EXPECT_EQ(actual_entry.port, expected_entry.port);
                EXPECT_EQ(actual_entry.role, expected_entry.role);
                EXPECT_EQ(actual_entry.term, expected_entry.term);
            }
        }
    }

    static inline int sequence_{0};
    fs::path base_dir_;
};

// 检测一下正常的 init、save、load 流程。
TEST_F(SdmPersistEngineTest, InitSaveAndLoadRoundTrip) {
    SdmPersistEngine engine(base_dir_.string());
    Status status = engine.init();
    ASSERT_TRUE(status.ok());

    SdmPersistedRecord record = make_record();
    status = engine.save_sdm_meta(record);
    ASSERT_TRUE(status.ok());

    SdmPersistedRecord loaded;
    status = engine.load_sdm_meta(loaded);
    ASSERT_TRUE(status.ok());

    expect_record_equal(loaded, record);
}

// 检测一下还没有save的时候，load 返回空数据且不报错。
TEST_F(SdmPersistEngineTest, LoadMissingMetaReturnsEmptyRecord) {
    SdmPersistEngine engine(base_dir_.string());
    Status status = engine.init();
    ASSERT_TRUE(status.ok());

    SdmPersistedRecord loaded = make_record();
    status = engine.load_sdm_meta(loaded);
    ASSERT_TRUE(status.ok());

    EXPECT_TRUE(loaded.tables.empty());
    EXPECT_TRUE(loaded.nodes.empty());
    EXPECT_TRUE(loaded.replicas.empty());
    EXPECT_TRUE(loaded.resource_pools.empty());
    EXPECT_TRUE(loaded.shard_routes.empty());
}

// 检测一下空的save和load是否正确
TEST_F(SdmPersistEngineTest, SaveAndLoadEmptyRecord) {
    SdmPersistEngine engine(base_dir_.string());
    Status status = engine.init();
    ASSERT_TRUE(status.ok());

    SdmPersistedRecord record;
    status = engine.save_sdm_meta(record);
    ASSERT_TRUE(status.ok());

    SdmPersistedRecord loaded = make_record();
    status = engine.load_sdm_meta(loaded);
    ASSERT_TRUE(status.ok());

    expect_record_equal(loaded, record);
}

// 检测一下当save了多次之后，load出来的是最新save的数据
TEST_F(SdmPersistEngineTest, OverwriteSaveKeepsLatestRecord) {
    SdmPersistEngine engine(base_dir_.string());
    Status status = engine.init();
    ASSERT_TRUE(status.ok());

    {
        SdmPersistedRecord old = make_record();
        old.nodes["node-999"] = old.nodes["node-2"];
        status = engine.save_sdm_meta(old);
        ASSERT_TRUE(status.ok());
    }

    SdmPersistedRecord latest = make_record();
    status = engine.save_sdm_meta(latest);
    ASSERT_TRUE(status.ok());

    SdmPersistedRecord loaded;
    status = engine.load_sdm_meta(loaded);
    ASSERT_TRUE(status.ok());

    expect_record_equal(loaded, latest);
    EXPECT_EQ(loaded.nodes.count("node-999"), 0U);
}

// 检测没有 init 时进行 save 或 load 会返回明确的未初始化错误。
TEST_F(SdmPersistEngineTest, SaveOrLoadWithoutInitReturnsNotInit) {
    SdmPersistEngine engine(base_dir_.string());

    Status status = engine.save_sdm_meta(make_record());
    EXPECT_EQ(status.code(), StatusCode::NOT_INIT);

    SdmPersistedRecord loaded;
    status = engine.load_sdm_meta(loaded);
    EXPECT_EQ(status.code(), StatusCode::NOT_INIT);
}

// 检测 load 一段错误数据时返回错误。
TEST_F(SdmPersistEngineTest, LoadCorruptedSdmMetaFails) {
    {
        std::ofstream out(meta_path(), std::ios::binary);
        ASSERT_TRUE(out.is_open());
        out << "corrupted sdm meta";
    }

    SdmPersistEngine engine(base_dir_.string());
    Status status = engine.init();
    ASSERT_TRUE(status.ok());

    SdmPersistedRecord loaded;
    status = engine.load_sdm_meta(loaded);
    EXPECT_TRUE(status.fail());
}

}  // namespace adviskv::sdm