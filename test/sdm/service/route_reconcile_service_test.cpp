#include "sdm/service/route_service.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "sdm/store/sdm_store.h"
#include "sdm/store/sdm_store_test_helper.h"
#include "sdm/selector/node_selector/node_selector.h"

namespace adviskv::sdm {
namespace {

constexpr TableID TEST_TABLE_ID = 1001;
constexpr ShardIndex TEST_SHARD_INDEX = 0;

class TestRouteService : public RouteService {
   public:
    using RouteService::RouteService;
    Status check_shard_route_test(const Table& table, ShardIndex shard_index) {
        return check_shard_route(table, shard_index);
    }
};

Table make_table() {
    TableState state{};
    state.desired = TableDesired::PRESENT;
    state.phase = TablePhase::READY;
    return Table{
        TEST_TABLE_ID,
        TableSpec{"orders", 11, "commerce", 1, 3, "pool-a", "create-1001"},
        state};
}

Node make_node(const NodeID& node_id, int32_t port) {
    return Node{node_id, NodeMeta{"pool-a", "dc-a"},
                NodeState{NodeStatus::ONLINE, Endpoint{"127.0.0.1", port}, 1},
                NodeDerived{}};
}

Replica make_replica(ReplicaIndex replica_index, const NodeID& node_id,
                     int32_t port, ReplicaRole role, Term term) {
    ReplicaState state{};
    state.desired = ReplicaDesired::PRESENT;
    state.phase = ReplicaPhase::READY;
    state.observed_raft_role = role;
    state.observed_member_type = RaftMemberType::VOTER;
    state.observed_endpoint = Endpoint{"127.0.0.1", port};
    state.term = term;
    return Replica{ReplicaID{TEST_TABLE_ID, TEST_SHARD_INDEX, replica_index},
                   ReplicaSpec{"dc-a", node_id, EngineType::MAP}, state};
}

void put_nodes(SdmStore& store, int count) {
    for (int i = 0; i < count; ++i) {
        ASSERT_TRUE(store_test::put_node(
                        store, make_node(fmt::format("node-{}", i), 18080 + i))
                        .ok());
    }
}

void put_test_table(SdmStore& store) {
    ASSERT_TRUE(store_test::put_table(store, make_table()).ok());
}

ShardID test_shard_id() { return ShardID{TEST_TABLE_ID, TEST_SHARD_INDEX}; }

}  // namespace

// 检测没有 leader 时，路由会被删除并返回 ROUTE_NOT_FOUND。
TEST(RouteServiceReconcileTest, DeleteShardRouteWhenLeaderCountLessThanOne) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store.init().ok());
    put_test_table(store);
    put_nodes(store, 2);
    ASSERT_TRUE(
        store_test::put_replica(
            store, make_replica(0, "node-0", 18080, ReplicaRole::FOLLOWER, 10))
            .ok());
    ASSERT_TRUE(
        store_test::put_replica(
            store, make_replica(1, "node-1", 18081, ReplicaRole::FOLLOWER, 10))
            .ok());
    ASSERT_TRUE(
        store_test::put_shard_route(
            store,
            ShardRoute{
                test_shard_id(),
                {RouteEntry{ReplicaID{TEST_TABLE_ID, TEST_SHARD_INDEX, 0},
                            "node-0", "127.0.0.1", 18080, ReplicaRole::LEADER}},
            })
            .ok());

    TestRouteService ctrl(&store);
    Status status = ctrl.check_shard_route_test(make_table(), TEST_SHARD_INDEX);
    EXPECT_EQ(status.code(), StatusCode::ROUTE_NOT_FOUND);
    EXPECT_NE(status.msg().find("writable leader route is not ready"),
              std::string::npos);

    ShardRouteOr route;
    ASSERT_TRUE(
        store_test::get_shard_route(store, test_shard_id(), route).ok());
    EXPECT_TRUE(route.is_empty());
}

// 检测恰好一个 leader 时，路由会被正确写入，leader 在前、follower 按 seq 排序。
TEST(RouteServiceReconcileTest, PutShardRouteWhenLeaderCountEqualsOne) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store.init().ok());
    put_test_table(store);
    put_nodes(store, 3);
    ASSERT_TRUE(
        store_test::put_replica(
            store, make_replica(0, "node-0", 18080, ReplicaRole::LEADER, 20))
            .ok());
    ASSERT_TRUE(
        store_test::put_replica(
            store, make_replica(1, "node-1", 18081, ReplicaRole::FOLLOWER, 20))
            .ok());
    ASSERT_TRUE(
        store_test::put_replica(
            store, make_replica(2, "node-2", 18082, ReplicaRole::FOLLOWER, 20))
            .ok());

    TestRouteService ctrl(&store);
    Status status = ctrl.check_shard_route_test(make_table(), TEST_SHARD_INDEX);
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

// 检测多个 leader 拥有相同最大 term 时，路由被删除（无法确定谁是真正的 leader）。
TEST(RouteServiceReconcileTest, ReturnRouteNotReadyWhenLeadersShareMaxTerm) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store.init().ok());
    put_test_table(store);
    put_nodes(store, 3);
    ASSERT_TRUE(
        store_test::put_replica(
            store, make_replica(0, "node-0", 18080, ReplicaRole::LEADER, 30))
            .ok());
    ASSERT_TRUE(
        store_test::put_replica(
            store, make_replica(1, "node-1", 18081, ReplicaRole::LEADER, 20))
            .ok());
    ASSERT_TRUE(
        store_test::put_replica(
            store, make_replica(2, "node-2", 18082, ReplicaRole::LEADER, 30))
            .ok());
    ASSERT_TRUE(
        store_test::put_shard_route(
            store,
            ShardRoute{
                test_shard_id(),
                {RouteEntry{ReplicaID{TEST_TABLE_ID, TEST_SHARD_INDEX, 0},
                            "node-0", "127.0.0.1", 18080, ReplicaRole::LEADER}},
            })
            .ok());

    TestRouteService ctrl(&store);
    Status status = ctrl.check_shard_route_test(make_table(), TEST_SHARD_INDEX);
    EXPECT_EQ(status.code(), StatusCode::ROUTE_NOT_FOUND);

    ShardRouteOr route;
    ASSERT_TRUE(
        store_test::get_shard_route(store, test_shard_id(), route).ok());
    EXPECT_TRUE(route.is_empty());
}

// 检测多个 leader 的 term 不同时，选择最大 term 的 leader 作为主路由，其余降级为 follower。
TEST(RouteServiceReconcileTest, MultipleLeadersWithDifferentTermsChooseMaxTermLeader) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store.init().ok());
    put_test_table(store);
    put_nodes(store, 4);
    ASSERT_TRUE(
        store_test::put_replica(
            store, make_replica(0, "node-0", 18080, ReplicaRole::LEADER, 10))
            .ok());
    ASSERT_TRUE(
        store_test::put_replica(
            store, make_replica(1, "node-1", 18081, ReplicaRole::LEADER, 30))
            .ok());
    ASSERT_TRUE(
        store_test::put_replica(
            store, make_replica(2, "node-2", 18082, ReplicaRole::LEADER, 20))
            .ok());
    ASSERT_TRUE(
        store_test::put_replica(
            store, make_replica(3, "node-3", 18083, ReplicaRole::FOLLOWER, 30))
            .ok());

    TestRouteService ctrl(&store);
    Status status = ctrl.check_shard_route_test(make_table(), TEST_SHARD_INDEX);
    ASSERT_TRUE(status.ok());

    ShardRouteOr route;
    ASSERT_TRUE(
        store_test::get_shard_route(store, test_shard_id(), route).ok());
    ASSERT_FALSE(route.is_empty());
    ASSERT_EQ(route->replicas.size(), 4U);

    EXPECT_EQ(route->replicas[0].replica_id.replica_seq, 1);
    EXPECT_EQ(route->replicas[0].role, ReplicaRole::LEADER);
    EXPECT_EQ(route->replicas[0].term, 30);

    EXPECT_EQ(route->replicas[1].replica_id.replica_seq, 0);
    EXPECT_EQ(route->replicas[1].role, ReplicaRole::FOLLOWER);
    EXPECT_EQ(route->replicas[2].replica_id.replica_seq, 2);
    EXPECT_EQ(route->replicas[2].role, ReplicaRole::FOLLOWER);
    EXPECT_EQ(route->replicas[3].replica_id.replica_seq, 3);
    EXPECT_EQ(route->replicas[3].role, ReplicaRole::FOLLOWER);
}

// 检测 leader 切换后，旧路由被删除、新路由被重新发布。
TEST(RouteServiceReconcileTest, DeleteStaleRouteAndRepublishAfterLeaderRecovery) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store.init().ok());
    put_test_table(store);
    put_nodes(store, 2);
    ASSERT_TRUE(
        store_test::put_replica(
            store, make_replica(0, "node-0", 18080, ReplicaRole::LEADER, 10))
            .ok());
    ASSERT_TRUE(
        store_test::put_replica(
            store, make_replica(1, "node-1", 18081, ReplicaRole::FOLLOWER, 10))
            .ok());
    ASSERT_TRUE(
        store_test::put_shard_route(
            store,
            ShardRoute{
                test_shard_id(),
                {RouteEntry{ReplicaID{TEST_TABLE_ID, TEST_SHARD_INDEX, 0},
                            "node-0", "127.0.0.1", 18080, ReplicaRole::LEADER,
                            10},
                 RouteEntry{ReplicaID{TEST_TABLE_ID, TEST_SHARD_INDEX, 1},
                            "node-1", "127.0.0.1", 18081, ReplicaRole::FOLLOWER,
                            10}},
            })
            .ok());

    TestRouteService ctrl(&store);
    ASSERT_TRUE(
        store_test::put_replica(
            store, make_replica(0, "node-0", 18080, ReplicaRole::FOLLOWER, 11))
            .ok());
    ASSERT_TRUE(
        store_test::put_replica(
            store, make_replica(1, "node-1", 18081, ReplicaRole::FOLLOWER, 11))
            .ok());

    Status status = ctrl.check_shard_route_test(make_table(), TEST_SHARD_INDEX);
    EXPECT_EQ(status.code(), StatusCode::ROUTE_NOT_FOUND);

    ShardRouteOr route;
    ASSERT_TRUE(
        store_test::get_shard_route(store, test_shard_id(), route).ok());
    EXPECT_TRUE(route.is_empty());

    ASSERT_TRUE(
        store_test::put_replica(
            store, make_replica(0, "node-0", 18080, ReplicaRole::FOLLOWER, 12))
            .ok());
    ASSERT_TRUE(
        store_test::put_replica(
            store, make_replica(1, "node-1", 18081, ReplicaRole::LEADER, 12))
            .ok());

    status = ctrl.check_shard_route_test(make_table(), TEST_SHARD_INDEX);
    ASSERT_TRUE(status.ok());

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

// 检测 reconcile 时会重新读取最新的 table 状态，不因传入的过期 table 对象而误建路由。
TEST(RouteServiceReconcileTest, CheckShardRouteReloadsCurrentTableState) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store.init().ok());
    Table stale_table = make_table();
    put_test_table(store);
    put_nodes(store, 1);
    ASSERT_TRUE(
        store_test::put_replica(
            store, make_replica(0, "node-0", 18080, ReplicaRole::LEADER, 10))
            .ok());
    ASSERT_TRUE(
        store_test::put_shard_route(
            store,
            ShardRoute{
                test_shard_id(),
                {RouteEntry{ReplicaID{TEST_TABLE_ID, TEST_SHARD_INDEX, 0},
                            "node-0", "127.0.0.1", 18080, ReplicaRole::LEADER,
                            10}},
            })
            .ok());

    Table current = stale_table;
    current.state.desired = TableDesired::ABSENT;
    current.state.phase = TablePhase::DELETING;
    ASSERT_TRUE(store_test::put_table(store, current).ok());

    TestRouteService ctrl(&store);
    ASSERT_TRUE(ctrl.check_shard_route_test(stale_table, TEST_SHARD_INDEX).ok());

    ShardRouteOr route;
    ASSERT_TRUE(
        store_test::get_shard_route(store, test_shard_id(), route).ok());
    EXPECT_TRUE(route.is_empty());
}

}  // namespace adviskv::sdm
