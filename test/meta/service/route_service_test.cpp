#include "meta/service/route_service.h"

#include <gtest/gtest.h>

#include <memory>

#include "common/status.h"
#include "meta/store/meta_store.h"
#include "test/meta/store/meta_store_test_helper.h"
namespace adviskv::meta {
namespace {

Table make_table() {
    TableState state{};
    state.desired = TableDesired::PRESENT;
    state.phase = TablePhase::READY;
    return Table{1001,
                 TableSpec{"orders", 11, "commerce", 4, 2, "pool-a",
                           "create-table-1001", EngineType::MAP, make_shard_ranges(4)},
                 state};
}

ShardID shard_for_key(const Table& table, const Key& key) {
    const auto position = find_shard_range(table.spec.shard_ranges, hash_bucket_for_key(key));
    return ShardID(table.table_id, table.spec.shard_ranges[position].shard_id);
}

ShardRoute make_route(const ShardID& shard_id,
                      std::vector<ReplicaRole> roles = {ReplicaRole::LEADER,
                                                        ReplicaRole::FOLLOWER},
                      bool valid_leader_endpoint = true) {
    ShardRoute route;
    route.shard_id = shard_id;
    for (size_t i = 0; i < roles.size(); ++i) {
        const bool leader = roles[i] == ReplicaRole::LEADER;
        route.replicas.push_back(RouteEntry{
            ReplicaID{shard_id.table_id, shard_id.shard_index,
                      static_cast<ReplicaIndex>(i)},
            "node-" + std::to_string(i),
            leader && !valid_leader_endpoint
                ? ""
                : "127.0.0." + std::to_string(i + 1),
            leader && !valid_leader_endpoint ? 0
                                             : static_cast<int32_t>(18080 + i),
            roles[i],
            7,
        });
    }
    return route;
}

GetRouteParam make_get_route_param(const Key& key = "user-123") {
    return GetRouteParam{"commerce", "orders", key};
}

}  // namespace

TEST(RouteServiceTest, InspectionReturnsAllRangesWithoutLiveReplicas) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    const auto table = make_table();
    ASSERT_TRUE(store_test::put_table(store, table).ok());
    RouteService service(&store);
    std::vector<ShardRoute> routes;
    GetTableRoutesParam param{"commerce", "orders", true};
    ASSERT_TRUE(service.get_table_routes(param, nullptr, &routes).ok());
    ASSERT_EQ(routes.size(), 4U);
    for (size_t i = 0; i < routes.size(); ++i) {
        EXPECT_EQ(routes[i].hash_range, table.spec.shard_ranges[i].range);
        EXPECT_TRUE(routes[i].replicas.empty());
    }
    param.include_unavailable = false;
    EXPECT_EQ(service.get_table_routes(param, nullptr, &routes).code(), StatusCode::ROUTE_NOT_FOUND);
}

TEST(RouteServiceTest, UsesPersistedUnevenRangesAndSparseIds) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    auto table = make_table();
    const auto h = hash_bucket_for_key("user-123");
    ASSERT_GT(h, 0U);
    table.spec.shard_ranges = {{17, {0, h}}, {42, {h, K_HASH_BUCKET_COUNT}}};
    ASSERT_TRUE(store_test::put_table(store, table).ok());
    ASSERT_TRUE(store_test::put_shard_route(store, make_route({table.table_id, 42})).ok());
    RouteService service(&store);
    ShardRoute route;
    ASSERT_TRUE(service.get_route(make_get_route_param(), &route).ok());
    EXPECT_EQ(route.shard_id.shard_index, 42);
    EXPECT_EQ(route.hash_range.start, h);
    auto by_id = make_get_route_param();
    by_id.use_id = true;
    by_id.table_id = table.table_id;
    by_id.shard_id = 42;
    ASSERT_TRUE(service.get_route(by_id, &route).ok());
}

// 检测 RouteService 会根据 DB、Table 和 Key 计算 shard 并返回对应路由。
TEST(RouteServiceTest, GetRouteReturnsRouteForTableAndKey) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    Table table = make_table();
    const Key key = "user-123";
    ShardID shard_id = shard_for_key(table, key);
    ASSERT_TRUE(store_test::put_table(store, table).ok());
    ASSERT_TRUE(store_test::put_shard_route(store, make_route(shard_id)).ok());
    RouteService service(&store);

    ShardRoute route;
    Status status = service.get_route(make_get_route_param(key), &route);

    ASSERT_TRUE(status.ok()) << status.msg();
    EXPECT_EQ(route.shard_id, shard_id);
    ASSERT_EQ(route.replicas.size(), 2U);
    EXPECT_EQ(route.replicas[0].role, ReplicaRole::LEADER);
    EXPECT_EQ(route.replicas[0].ip, "127.0.0.1");
    EXPECT_EQ(route.replicas[1].role, ReplicaRole::FOLLOWER);
}

// 检测非法 get_route 参数会被 RouteService 拒绝。
TEST(RouteServiceTest, GetRouteRejectsInvalidParam) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    RouteService service(&store);
    ShardRoute route;

    Status status =
        service.get_route(GetRouteParam{"", "orders", "key"}, &route);

    EXPECT_EQ(status.code(), StatusCode::INVALID_ARGUMENT);
}

// 检测 Table 或者  shard route 不存在
TEST(RouteServiceTest, GetRouteReturnsTableNotFoundWhenTableMissing) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    RouteService service(&store);
    ShardRoute route;
    {
        Status status = service.get_route(make_get_route_param(), &route);

        EXPECT_EQ(status.code(), StatusCode::TABLE_NOT_FOUND);
    }
    ASSERT_TRUE(store_test::put_table(store, make_table()).ok());
    {
        Status status = service.get_route(make_get_route_param(), &route);

        EXPECT_EQ(status.code(), StatusCode::ROUTE_NOT_FOUND);
    }
}

// 检测没有可写 leader、多个 leader 或 leader endpoint 无效时，RouteService 返回
// ROUTE_NOT_FOUND。
TEST(RouteServiceTest, GetRouteRequiresExactlyOneWritableLeader) {
    {
        MetaStore store{MemoryMetaStoreParam{}};
        ASSERT_TRUE(store.init().ok());
        Table table = make_table();
        ShardID shard_id = shard_for_key(table, "user-123");
        ASSERT_TRUE(store_test::put_table(store, table).ok());
        ASSERT_TRUE(store_test::put_shard_route(
                        store, make_route(shard_id, {ReplicaRole::FOLLOWER,
                                                     ReplicaRole::FOLLOWER}))
                        .ok());
        RouteService service(&store);
        ShardRoute route;
        EXPECT_EQ(service.get_route(make_get_route_param(), &route).code(),
                  StatusCode::ROUTE_NOT_FOUND);
    }
    {
        MetaStore store{MemoryMetaStoreParam{}};
        ASSERT_TRUE(store.init().ok());
        Table table = make_table();
        ShardID shard_id = shard_for_key(table, "user-123");
        ASSERT_TRUE(store_test::put_table(store, table).ok());
        ASSERT_TRUE(store_test::put_shard_route(
                        store, make_route(shard_id, {ReplicaRole::LEADER,
                                                     ReplicaRole::LEADER}))
                        .ok());
        RouteService service(&store);
        ShardRoute route;
        EXPECT_EQ(service.get_route(make_get_route_param(), &route).code(),
                  StatusCode::ROUTE_NOT_FOUND);
    }
    {
        MetaStore store{MemoryMetaStoreParam{}};
        ASSERT_TRUE(store.init().ok());
        Table table = make_table();
        ShardID shard_id = shard_for_key(table, "user-123");
        ASSERT_TRUE(store_test::put_table(store, table).ok());
        ASSERT_TRUE(
            store_test::put_shard_route(
                store, make_route(shard_id, {ReplicaRole::LEADER}, false))
                .ok());
        RouteService service(&store);
        ShardRoute route;
        EXPECT_EQ(service.get_route(make_get_route_param(), &route).code(),
                  StatusCode::ROUTE_NOT_FOUND);
    }
}

}  // namespace adviskv::meta
