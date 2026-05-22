#include <fmt/format.h>
#include <gtest/gtest.h>

#include <vector>

#include "sdm/model/sdm_store.h"

#define private public
#include "sdm/background/routeupdate_check_task.h"
#undef private

namespace adviskv::sdm {
namespace {

constexpr TableID TEST_TABLE_ID = 1001;
constexpr ShardIndex TEST_SHARD_INDEX = 0;

Table make_table() {
    return Table{
        .table_id = TEST_TABLE_ID,
        .spec{
            .table_name = "orders",
            .db_id = 11,
            .db_name = "commerce",
            .shard_count = 1,
            .replica_count = 3,
            .resource_pool = "pool-a",
            .operation_id = "create-1001",
        },
        .state{
            .desired = TableDesired::PRESENT,
            .phase = TablePhase::READY,
        },
    };
}

Node make_node(const NodeID& node_id, int32_t port) {
    return Node{
        .id = node_id,
        .spec{
            .resource_pool = "pool-a",
            .dc = "dc-a",
            .status = NodeStatus::ONLINE,
        },
        .state{
            .endpoint = Endpoint{.ip = "127.0.0.1", .port = port},
            .last_heartbeat_ts = 1,
        },
    };
}

Replica make_replica(ReplicaIndex replica_index, const NodeID& node_id,
                     int32_t port, ReplicaRole role, Term term) {
    return Replica{
        .replica_id = ReplicaID{.table_id = TEST_TABLE_ID,
                                .shard_index = TEST_SHARD_INDEX,
                                .replica_index = replica_index},
        .spec{
            .dc = "dc-a",
            .assign_node_id = node_id,
            .engine_type = EngineType::MAP,
        },
        .state{
            .desired = ReplicaDesired::PRESENT,
            .phase = ReplicaPhase::READY,
            .observed_role = role,
            .observed_endpoint = Endpoint{.ip = "127.0.0.1", .port = port},
            .term = term,
        },
    };
}

void put_nodes(SdmStore& store, int count) {
    for (int i = 0; i < count; ++i) {
        ASSERT_TRUE(
            store.put_node(make_node(fmt::format("node-{}", i), 18080 + i))
                .ok());
    }
}

ShardID test_shard_id() {
    return ShardID{.table_id = TEST_TABLE_ID, .shard_index = TEST_SHARD_INDEX};
}

}  // namespace

// 检测一下，当这个leader的数量是小于一的时候，
// 会调用这个函数delete_shard_route
TEST(RouteUpdateCheckTaskTest, DeleteShardRouteWhenLeaderCountLessThanOne) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_nodes(store, 2);
    ASSERT_TRUE(store
                    .put_replica(make_replica(0, "node-0", 18080,
                                              ReplicaRole::FOLLOWER, 10))
                    .ok());
    ASSERT_TRUE(store
                    .put_replica(make_replica(1, "node-1", 18081,
                                              ReplicaRole::FOLLOWER, 10))
                    .ok());
    ASSERT_TRUE(
        store
            .put_shard_route(ShardRoute{
                .shard_id = test_shard_id(),
                .replicas = {RouteEntry{
                    .replica_id = ReplicaID{TEST_TABLE_ID, TEST_SHARD_INDEX, 0},
                    .node_id = "node-0",
                    .ip = "127.0.0.1",
                    .port = 18080,
                    .role = ReplicaRole::LEADER,
                }},
            })
            .ok());

    RouteUpdateCheckTask task(&store);
    Status status = task.check_shard_route(make_table(), TEST_SHARD_INDEX);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(status.msg(), "leader count < 1");
    
    ShardRouteOr route;
    ASSERT_TRUE(store.get_shard_route(test_shard_id(), route).ok());
    EXPECT_TRUE(route.is_empty());
}

// 检测一下，当这个leader的数量等于一的时候是否是正常的
TEST(RouteUpdateCheckTaskTest, PutShardRouteWhenLeaderCountEqualsOne) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_nodes(store, 3);
    ASSERT_TRUE(store
                    .put_replica(make_replica(0, "node-0", 18080,
                                              ReplicaRole::LEADER, 20))
                    .ok());
    ASSERT_TRUE(store
                    .put_replica(make_replica(1, "node-1", 18081,
                                              ReplicaRole::FOLLOWER, 20))
                    .ok());
    ASSERT_TRUE(store
                    .put_replica(make_replica(2, "node-2", 18082,
                                              ReplicaRole::FOLLOWER, 20))
                    .ok());

    RouteUpdateCheckTask task(&store);
    Status status = task.check_shard_route(make_table(), TEST_SHARD_INDEX);
    ASSERT_TRUE(status.ok());

    ShardRouteOr route;
    ASSERT_TRUE(store.get_shard_route(test_shard_id(), route).ok());
    ASSERT_FALSE(route.is_empty());
    ASSERT_EQ(route->replicas.size(), 3U);
    EXPECT_EQ(route->replicas[0].replica_id.replica_index, 0);
    EXPECT_EQ(route->replicas[0].role, ReplicaRole::LEADER);
    EXPECT_EQ(route->replicas[1].replica_id.replica_index, 1);
    EXPECT_EQ(route->replicas[1].role, ReplicaRole::FOLLOWER);
    EXPECT_EQ(route->replicas[2].replica_id.replica_index, 2);
    EXPECT_EQ(route->replicas[2].role, ReplicaRole::FOLLOWER);
}

// 检测一下，当leader的数量是大于一的时候，但是term是有多个相同的，
// 这个时候会返回一个状态码error
TEST(RouteUpdateCheckTaskTest, ReturnErrorWhenMultipleLeadersHaveSameTerm) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_nodes(store, 2);
    ASSERT_TRUE(store
                    .put_replica(make_replica(0, "node-0", 18080,
                                              ReplicaRole::LEADER, 30))
                    .ok());
    ASSERT_TRUE(store
                    .put_replica(make_replica(1, "node-1", 18081,
                                              ReplicaRole::LEADER, 30))
                    .ok());
    ASSERT_TRUE(
        store
            .put_shard_route(ShardRoute{
                .shard_id = test_shard_id(),
                .replicas = {RouteEntry{
                    .replica_id = ReplicaID{TEST_TABLE_ID, TEST_SHARD_INDEX, 0},
                    .node_id = "node-0",
                    .ip = "127.0.0.1",
                    .port = 18080,
                    .role = ReplicaRole::LEADER,
                }},
            })
            .ok());

    RouteUpdateCheckTask task(&store);
    Status status = task.check_shard_route(make_table(), TEST_SHARD_INDEX);
    EXPECT_TRUE(status.fail());

    ShardRouteOr route;
    ASSERT_TRUE(store.get_shard_route(test_shard_id(), route).ok());
    EXPECT_TRUE(route.is_empty());
}

// 检测一下，当leader的数量是大于一的时候，但是term是各不相同的。
// 此时是按照我们的预期来走的
TEST(RouteUpdateCheckTaskTest,
     MultipleLeadersWithDifferentTermsChooseMaxTermLeader) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_nodes(store, 4);
    ASSERT_TRUE(store
                    .put_replica(make_replica(0, "node-0", 18080,
                                              ReplicaRole::LEADER, 10))
                    .ok());
    ASSERT_TRUE(store
                    .put_replica(make_replica(1, "node-1", 18081,
                                              ReplicaRole::LEADER, 30))
                    .ok());
    ASSERT_TRUE(store
                    .put_replica(make_replica(2, "node-2", 18082,
                                              ReplicaRole::LEADER, 20))
                    .ok());
    ASSERT_TRUE(store
                    .put_replica(make_replica(3, "node-3", 18083,
                                              ReplicaRole::FOLLOWER, 30))
                    .ok());

    RouteUpdateCheckTask task(&store);
    Status status = task.check_shard_route(make_table(), TEST_SHARD_INDEX);
    ASSERT_TRUE(status.ok());

    ShardRouteOr route;
    ASSERT_TRUE(store.get_shard_route(test_shard_id(), route).ok());
    ASSERT_FALSE(route.is_empty());
    ASSERT_EQ(route->replicas.size(), 4U);

    EXPECT_EQ(route->replicas[0].replica_id.replica_index, 1);
    EXPECT_EQ(route->replicas[0].role, ReplicaRole::LEADER);
    EXPECT_EQ(route->replicas[0].term, 30);

    EXPECT_EQ(route->replicas[1].replica_id.replica_index, 0);
    EXPECT_EQ(route->replicas[1].role, ReplicaRole::FOLLOWER);
    EXPECT_EQ(route->replicas[2].replica_id.replica_index, 2);
    EXPECT_EQ(route->replicas[2].role, ReplicaRole::FOLLOWER);
    EXPECT_EQ(route->replicas[3].replica_id.replica_index, 3);
    EXPECT_EQ(route->replicas[3].role, ReplicaRole::FOLLOWER);
}

}  // namespace adviskv::sdm