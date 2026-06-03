#include <fmt/format.h>
#include <gtest/gtest.h>

#include <vector>

#include "sdm/model/sdm_store.h"

#include "sdm/background/routeupdate_check_task.h"

namespace adviskv::sdm {
namespace {

constexpr TableID TEST_TABLE_ID = 1001;
constexpr ShardIndex TEST_SHARD_INDEX = 0;

class RouteUpdateCheckTaskTmp : public RouteUpdateCheckTask {
   public:
    using RouteUpdateCheckTask::RouteUpdateCheckTask;
    Status check_shard_route_tmp(const Table& table, ShardIndex shard_index) {
        return check_shard_route(table, shard_index);
    }
};

Table make_table() {
    TableState state{};
    state.desired = TableDesired::PRESENT;
    state.phase = TablePhase::READY;
    return Table{TEST_TABLE_ID,
                 TableSpec{"orders", 11, "commerce", 1, 3, "pool-a",
                           "create-1001"},
                 state};
}

Node make_node(const NodeID& node_id, int32_t port) {
    return Node{node_id,
                NodeSpec{"pool-a", "dc-a", NodeStatus::ONLINE},
                NodeState{Endpoint{"127.0.0.1", port}, 1},
                NodeDerived{}};
}

Replica make_replica(ReplicaIndex replica_index, const NodeID& node_id,
                     int32_t port, ReplicaRole role, Term term) {
    ReplicaState state{};
    state.desired = ReplicaDesired::PRESENT;
    state.phase = ReplicaPhase::READY;
    state.observed_role = role;
    state.observed_endpoint = Endpoint{"127.0.0.1", port};
    state.term = term;
    return Replica{ReplicaID{TEST_TABLE_ID, TEST_SHARD_INDEX, replica_index},
                   ReplicaSpec{"dc-a", node_id, EngineType::MAP, {}},
                   state};
}

void put_nodes(SdmStore& store, int count) {
    for (int i = 0; i < count; ++i) {
        ASSERT_TRUE(
            store.put_node(make_node(fmt::format("node-{}", i), 18080 + i))
                .ok());
    }
}

ShardID test_shard_id() {
    return ShardID{TEST_TABLE_ID, TEST_SHARD_INDEX};
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
                test_shard_id(),
                {RouteEntry{ReplicaID{TEST_TABLE_ID, TEST_SHARD_INDEX, 0},
                            "node-0",
                            "127.0.0.1",
                            18080,
                            ReplicaRole::LEADER}},
            })
            .ok());

    RouteUpdateCheckTaskTmp task(&store);
    Status status = task.check_shard_route_tmp(make_table(), TEST_SHARD_INDEX);
    EXPECT_EQ(status.code(), StatusCode::ROUTE_NOT_FOUND);
    EXPECT_NE(status.msg().find("writable leader route is not ready"),
              std::string::npos);

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

    RouteUpdateCheckTaskTmp task(&store);
    Status status = task.check_shard_route_tmp(make_table(), TEST_SHARD_INDEX);
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

// 检测一下，当leader的数量是大于一的时候，但是最高 term 出现歧义，
// 无论输入顺序怎样，route 都应该被清掉。
TEST(RouteUpdateCheckTaskTest, ReturnRouteNotReadyWhenLeadersShareMaxTerm) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_nodes(store, 3);
    ASSERT_TRUE(store
                    .put_replica(make_replica(0, "node-0", 18080,
                                              ReplicaRole::LEADER, 30))
                    .ok());
    ASSERT_TRUE(store
                    .put_replica(make_replica(1, "node-1", 18081,
                                              ReplicaRole::LEADER, 20))
                    .ok());
    ASSERT_TRUE(store
                    .put_replica(make_replica(2, "node-2", 18082,
                                              ReplicaRole::LEADER, 30))
                    .ok());
    ASSERT_TRUE(
        store
            .put_shard_route(ShardRoute{
                test_shard_id(),
                {RouteEntry{ReplicaID{TEST_TABLE_ID, TEST_SHARD_INDEX, 0},
                            "node-0",
                            "127.0.0.1",
                            18080,
                            ReplicaRole::LEADER}},
            })
            .ok());

    RouteUpdateCheckTaskTmp task(&store);
    Status status = task.check_shard_route_tmp(make_table(), TEST_SHARD_INDEX);
    EXPECT_EQ(status.code(), StatusCode::ROUTE_NOT_FOUND);

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

    RouteUpdateCheckTaskTmp task(&store);
    Status status = task.check_shard_route_tmp(make_table(), TEST_SHARD_INDEX);
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

// 已经发布出去的旧 route，如果当前没有 leader 了，应该先被删除；
// 等 observed leader 恢复后，再重新发布新的合法 route。
TEST(RouteUpdateCheckTaskTest,
     DeleteStaleRouteAndRepublishAfterLeaderRecovery) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_nodes(store, 2);
    ASSERT_TRUE(store
                    .put_replica(make_replica(0, "node-0", 18080,
                                              ReplicaRole::LEADER, 10))
                    .ok());
    ASSERT_TRUE(store
                    .put_replica(make_replica(1, "node-1", 18081,
                                              ReplicaRole::FOLLOWER, 10))
                    .ok());
    ASSERT_TRUE(
        store
            .put_shard_route(ShardRoute{
                test_shard_id(),
                {RouteEntry{ReplicaID{TEST_TABLE_ID, TEST_SHARD_INDEX, 0},
                            "node-0",
                            "127.0.0.1",
                            18080,
                            ReplicaRole::LEADER,
                            10},
                 RouteEntry{ReplicaID{TEST_TABLE_ID, TEST_SHARD_INDEX, 1},
                            "node-1",
                            "127.0.0.1",
                            18081,
                            ReplicaRole::FOLLOWER,
                            10}},
            })
            .ok());

    RouteUpdateCheckTaskTmp task(&store);
    ASSERT_TRUE(store
                    .put_replica(make_replica(0, "node-0", 18080,
                                              ReplicaRole::FOLLOWER, 11))
                    .ok());
    ASSERT_TRUE(store
                    .put_replica(make_replica(1, "node-1", 18081,
                                              ReplicaRole::FOLLOWER, 11))
                    .ok());

    Status status = task.check_shard_route_tmp(make_table(), TEST_SHARD_INDEX);
    EXPECT_EQ(status.code(), StatusCode::ROUTE_NOT_FOUND);

    ShardRouteOr route;
    ASSERT_TRUE(store.get_shard_route(test_shard_id(), route).ok());
    EXPECT_TRUE(route.is_empty());

    ASSERT_TRUE(store
                    .put_replica(make_replica(0, "node-0", 18080,
                                              ReplicaRole::FOLLOWER, 12))
                    .ok());
    ASSERT_TRUE(store
                    .put_replica(make_replica(1, "node-1", 18081,
                                              ReplicaRole::LEADER, 12))
                    .ok());

    status = task.check_shard_route_tmp(make_table(), TEST_SHARD_INDEX);
    ASSERT_TRUE(status.ok());

    ASSERT_TRUE(store.get_shard_route(test_shard_id(), route).ok());
    ASSERT_FALSE(route.is_empty());
    ASSERT_EQ(route->replicas.size(), 2U);
    EXPECT_EQ(route->replicas[0].replica_id.replica_index, 1);
    EXPECT_EQ(route->replicas[0].role, ReplicaRole::LEADER);
    EXPECT_EQ(route->replicas[0].term, 12);
    EXPECT_EQ(route->replicas[1].replica_id.replica_index, 0);
    EXPECT_EQ(route->replicas[1].role, ReplicaRole::FOLLOWER);
    EXPECT_EQ(route->replicas[1].term, 12);
}

}  // namespace adviskv::sdm