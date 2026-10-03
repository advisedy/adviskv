#include "meta/service/route_service.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <vector>

#include "meta/store/meta_store.h"
#include "test/meta/store/meta_store_test_helper.h"

namespace adviskv::meta {
namespace {

constexpr TableID K_TEST_TABLE_ID = 1001;
constexpr ShardIndex K_TEST_SHARD_INDEX = 0;

Table make_table() {
    TableState state{};
    state.phase = TablePhase::READY;
    return Table{
        K_TEST_TABLE_ID,
        TableSpec{11, "commerce", "orders", EngineType::MAP, make_shard_ranges(1), 3, "pool-a"},
        state};
}

Node make_node(const NodeID& node_id, int32_t port) {
    return Node{node_id, NodeMeta{"pool-a", "dc-a"},
                NodeState{NodeStatus::ONLINE, Endpoint{"127.0.0.1", port}, 1}};
}

struct ReplicaFixture {
    Replica replica;
    ReplicaObserved observed;
};

ReplicaFixture make_replica(ReplicaIndex replica_index, const NodeID& node_id,
                            int32_t port, ReplicaRole role, Term term) {
    ReplicaState state{};
    state.phase = ReplicaPhase::READY;
    ReplicaObserved observed{};
    observed.raft_role = role;
    observed.member_type = RaftMemberType::VOTER;
    observed.endpoint = Endpoint{"127.0.0.1", port};
    observed.term = term;
    return {Replica{ReplicaID{K_TEST_TABLE_ID, K_TEST_SHARD_INDEX, replica_index},
                    ReplicaSpec{"dc-a", node_id, EngineType::MAP}, state},
            observed};
}

Status put_replica(MetaStore& store, const ReplicaFixture& fixture) {
    return store.write_with([&](MetaStoreTxn& txn) -> Status {
        RETURN_IF_INVALID_STATUS(txn.put_replica(fixture.replica))
        return txn.put_replica_observed(fixture.replica.replica_id, fixture.observed);
    });
}

void put_nodes(MetaStore& store, int count) {
    for (int i = 0; i < count; ++i) {
        ASSERT_TRUE(store_test::put_node(
                        store, make_node(fmt::format("node-{}", i), 18080 + i))
                        .ok());
    }
}

void put_test_table(MetaStore& store) {
    ASSERT_TRUE(store_test::put_table(store, make_table()).ok());
}

ShardID test_shard_id() { return ShardID{K_TEST_TABLE_ID, K_TEST_SHARD_INDEX}; }

}  // namespace

// 单个 shard 路由不可用时不阻断整轮收敛。
TEST(RouteServiceReconcileTest, DeleteShardRouteWhenLeaderCountLessThanOne) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    put_test_table(store);
    put_nodes(store, 2);
    ASSERT_TRUE(
        put_replica(
            store, make_replica(0, "node-0", 18080, ReplicaRole::FOLLOWER, 10))
            .ok());
    ASSERT_TRUE(
        put_replica(
            store, make_replica(1, "node-1", 18081, ReplicaRole::FOLLOWER, 10))
            .ok());
    ASSERT_TRUE(
        store_test::put_shard_route(
            store,
            ShardRoute{
                test_shard_id(),
                {RouteEntry{ReplicaID{K_TEST_TABLE_ID, K_TEST_SHARD_INDEX, 0},
                            "node-0", "127.0.0.1", 18080, ReplicaRole::LEADER}},
            })
            .ok());

    RouteService ctrl(&store);
    ASSERT_TRUE(ctrl.reconcile_all().ok());

    ShardRouteOr route;
    ASSERT_TRUE(
        store_test::get_shard_route(store, test_shard_id(), route).ok());
    EXPECT_TRUE(route.is_empty());
}

// 检测恰好一个 leader 时，路由会被正确写入，leader 在前、follower 按 seq 排序。
TEST(RouteServiceReconcileTest, PutShardRouteWhenLeaderCountEqualsOne) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    put_test_table(store);
    put_nodes(store, 3);
    ASSERT_TRUE(
        put_replica(
            store, make_replica(0, "node-0", 18080, ReplicaRole::LEADER, 20))
            .ok());
    ASSERT_TRUE(
        put_replica(
            store, make_replica(1, "node-1", 18081, ReplicaRole::FOLLOWER, 20))
            .ok());
    ASSERT_TRUE(
        put_replica(
            store, make_replica(2, "node-2", 18082, ReplicaRole::FOLLOWER, 20))
            .ok());

    RouteService ctrl(&store);
    Status status = ctrl.reconcile_all();
    ASSERT_TRUE(status.ok());

    ShardRouteOr route;
    ASSERT_TRUE(
        store_test::get_shard_route(store, test_shard_id(), route).ok());
    ASSERT_FALSE(route.is_empty());
    ASSERT_EQ(route->replicas.size(), 3U);
    EXPECT_EQ(route->replicas[0].replica_id.replica_seq, 0);
    EXPECT_EQ(route->replicas[0].role, ReplicaRole::LEADER);
    EXPECT_EQ(route->replicas[1].replica_id.replica_seq, 1);
    EXPECT_EQ(route->replicas[1].role, ReplicaRole::FOLLOWER);
    EXPECT_EQ(route->replicas[2].replica_id.replica_seq, 2);
    EXPECT_EQ(route->replicas[2].role, ReplicaRole::FOLLOWER);
}

// candidate 处于选举中，不能作为可写路由或 follower 路由暴露给客户端。
TEST(RouteServiceReconcileTest, CandidateIsExcludedFromRoute) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    put_test_table(store);
    put_nodes(store, 3);
    ASSERT_TRUE(
        put_replica(
            store, make_replica(0, "node-0", 18080, ReplicaRole::LEADER, 20))
            .ok());
    ASSERT_TRUE(
        put_replica(
            store, make_replica(1, "node-1", 18081, ReplicaRole::CANDIDATE, 21))
            .ok());
    ASSERT_TRUE(
        put_replica(
            store, make_replica(2, "node-2", 18082, ReplicaRole::FOLLOWER, 20))
            .ok());

    RouteService ctrl(&store);
    ASSERT_TRUE(ctrl.reconcile_all().ok());

    ShardRouteOr route;
    ASSERT_TRUE(store_test::get_shard_route(store, test_shard_id(), route).ok());
    ASSERT_FALSE(route.is_empty());
    ASSERT_EQ(route->replicas.size(), 2U);
    EXPECT_EQ(route->replicas[0].replica_id.replica_seq, 0);
    EXPECT_EQ(route->replicas[0].role, ReplicaRole::LEADER);
    EXPECT_EQ(route->replicas[1].replica_id.replica_seq, 2);
    EXPECT_EQ(route->replicas[1].role, ReplicaRole::FOLLOWER);
}

// 检测多个 leader 拥有相同最大 term 时，路由被删除（无法确定谁是真正的 leader）。
TEST(RouteServiceReconcileTest, ClearsRouteWhenLeadersShareMaxTerm) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    put_test_table(store);
    put_nodes(store, 3);
    ASSERT_TRUE(
        put_replica(
            store, make_replica(0, "node-0", 18080, ReplicaRole::LEADER, 30))
            .ok());
    ASSERT_TRUE(
        put_replica(
            store, make_replica(1, "node-1", 18081, ReplicaRole::LEADER, 20))
            .ok());
    ASSERT_TRUE(
        put_replica(
            store, make_replica(2, "node-2", 18082, ReplicaRole::LEADER, 30))
            .ok());
    ASSERT_TRUE(
        store_test::put_shard_route(
            store,
            ShardRoute{
                test_shard_id(),
                {RouteEntry{ReplicaID{K_TEST_TABLE_ID, K_TEST_SHARD_INDEX, 0},
                            "node-0", "127.0.0.1", 18080, ReplicaRole::LEADER}},
            })
            .ok());

    RouteService ctrl(&store);
    ASSERT_TRUE(ctrl.reconcile_all().ok());

    ShardRouteOr route;
    ASSERT_TRUE(
        store_test::get_shard_route(store, test_shard_id(), route).ok());
    EXPECT_TRUE(route.is_empty());
}

// 检测多个 leader 的 term 不同时，只保留最高 term 的 leader；低 term 的过期
// leader 不进入 route
TEST(RouteServiceReconcileTest, MultipleLeadersWithDifferentTermsChooseMaxTermLeader) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    put_test_table(store);
    put_nodes(store, 4);
    ASSERT_TRUE(
        put_replica(
            store, make_replica(0, "node-0", 18080, ReplicaRole::LEADER, 10))
            .ok());
    ASSERT_TRUE(
        put_replica(
            store, make_replica(1, "node-1", 18081, ReplicaRole::LEADER, 30))
            .ok());
    ASSERT_TRUE(
        put_replica(
            store, make_replica(2, "node-2", 18082, ReplicaRole::LEADER, 20))
            .ok());
    ASSERT_TRUE(
        put_replica(
            store, make_replica(3, "node-3", 18083, ReplicaRole::FOLLOWER, 30))
            .ok());

    RouteService ctrl(&store);
    Status status = ctrl.reconcile_all();
    ASSERT_TRUE(status.ok());

    ShardRouteOr route;
    ASSERT_TRUE(
        store_test::get_shard_route(store, test_shard_id(), route).ok());
    ASSERT_FALSE(route.is_empty());
    
    ASSERT_EQ(route->replicas.size(), 2U);

    EXPECT_EQ(route->replicas[0].replica_id.replica_seq, 1);
    EXPECT_EQ(route->replicas[0].role, ReplicaRole::LEADER);
    EXPECT_EQ(route->replicas[0].term, 30);

    EXPECT_EQ(route->replicas[1].replica_id.replica_seq, 3);
    EXPECT_EQ(route->replicas[1].role, ReplicaRole::FOLLOWER);
    EXPECT_EQ(route->replicas[1].term, 30);
}

// 检测 leader 切换后，旧路由被删除、新路由被重新发布。
TEST(RouteServiceReconcileTest, DeleteStaleRouteAndRepublishAfterLeaderRecovery) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    put_test_table(store);
    put_nodes(store, 2);
    ASSERT_TRUE(
        put_replica(
            store, make_replica(0, "node-0", 18080, ReplicaRole::LEADER, 10))
            .ok());
    ASSERT_TRUE(
        put_replica(
            store, make_replica(1, "node-1", 18081, ReplicaRole::FOLLOWER, 10))
            .ok());
    ASSERT_TRUE(
        store_test::put_shard_route(
            store,
            ShardRoute{
                test_shard_id(),
                {RouteEntry{ReplicaID{K_TEST_TABLE_ID, K_TEST_SHARD_INDEX, 0},
                            "node-0", "127.0.0.1", 18080, ReplicaRole::LEADER,
                            10},
                 RouteEntry{ReplicaID{K_TEST_TABLE_ID, K_TEST_SHARD_INDEX, 1},
                            "node-1", "127.0.0.1", 18081, ReplicaRole::FOLLOWER,
                            10}},
            })
            .ok());

    RouteService ctrl(&store);
    ASSERT_TRUE(
        put_replica(
            store, make_replica(0, "node-0", 18080, ReplicaRole::FOLLOWER, 11))
            .ok());
    ASSERT_TRUE(
        put_replica(
            store, make_replica(1, "node-1", 18081, ReplicaRole::FOLLOWER, 11))
            .ok());

    ASSERT_TRUE(ctrl.reconcile_all().ok());

    ShardRouteOr route;
    ASSERT_TRUE(
        store_test::get_shard_route(store, test_shard_id(), route).ok());
    EXPECT_TRUE(route.is_empty());

    ASSERT_TRUE(
        put_replica(
            store, make_replica(0, "node-0", 18080, ReplicaRole::FOLLOWER, 12))
            .ok());
    ASSERT_TRUE(
        put_replica(
            store, make_replica(1, "node-1", 18081, ReplicaRole::LEADER, 12))
            .ok());

    ASSERT_TRUE(ctrl.reconcile_all().ok());

    ASSERT_TRUE(
        store_test::get_shard_route(store, test_shard_id(), route).ok());
    ASSERT_FALSE(route.is_empty());
    ASSERT_EQ(route->replicas.size(), 2U);
    EXPECT_EQ(route->replicas[0].replica_id.replica_seq, 1);
    EXPECT_EQ(route->replicas[0].role, ReplicaRole::LEADER);
    EXPECT_EQ(route->replicas[0].term, 12);
    EXPECT_EQ(route->replicas[1].replica_id.replica_seq, 0);
    EXPECT_EQ(route->replicas[1].role, ReplicaRole::FOLLOWER);
    EXPECT_EQ(route->replicas[1].term, 12);
}

TEST(RouteServiceReconcileTest, DeletedTableRouteIsCleared) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    put_test_table(store);
    put_nodes(store, 1);
    ASSERT_TRUE(
        put_replica(
            store, make_replica(0, "node-0", 18080, ReplicaRole::LEADER, 10))
            .ok());
    ASSERT_TRUE(
        store_test::put_shard_route(
            store,
            ShardRoute{
                test_shard_id(),
                {RouteEntry{ReplicaID{K_TEST_TABLE_ID, K_TEST_SHARD_INDEX, 0},
                            "node-0", "127.0.0.1", 18080, ReplicaRole::LEADER,
                            10}},
            })
            .ok());

    Table current = make_table();
    current.state.phase = TablePhase::DELETING;
    ASSERT_TRUE(store_test::put_table(store, current).ok());

    RouteService ctrl(&store);
    ASSERT_TRUE(ctrl.reconcile_all().ok());

    ShardRouteOr route;
    ASSERT_TRUE(
        store_test::get_shard_route(store, test_shard_id(), route).ok());
    EXPECT_TRUE(route.is_empty());
}

}  // namespace adviskv::meta
