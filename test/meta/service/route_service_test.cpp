#include "meta/service/route_service.h"

#include <gtest/gtest.h>

#include "common/status.h"
#include "meta/store/meta_store.h"
#include "test/meta/store/meta_store_test_helper.h"
namespace adviskv::meta {
namespace {

Table make_table() {
    TableState state{};
    state.phase = TablePhase::READY;
    return Table{1001,
                 TableSpec{11, "commerce", "orders", EngineType::MAP, make_shard_ranges(4), 2, "pool-a"},
                 state};
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

TEST(RouteServiceTest, TableRoutesRequireExactlyOneWritableLeader) {
    {
        MetaStore store{MemoryMetaStoreParam{}};
        ASSERT_TRUE(store.init().ok());
        const auto table = make_table();
        ASSERT_TRUE(store_test::put_table(store, table).ok());
        ASSERT_TRUE(store_test::put_shard_route(
                        store, make_route({table.table_id, 0}, {ReplicaRole::FOLLOWER,
                                                                ReplicaRole::FOLLOWER}))
                        .ok());
        RouteService service(&store);
        std::vector<ShardRoute> routes;
        GetTableRoutesParam param{"commerce", "orders", false};
        EXPECT_EQ(service.get_table_routes(param, nullptr, &routes).code(), StatusCode::ROUTE_NOT_FOUND);
    }
    {
        MetaStore store{MemoryMetaStoreParam{}};
        ASSERT_TRUE(store.init().ok());
        const auto table = make_table();
        ASSERT_TRUE(store_test::put_table(store, table).ok());
        ASSERT_TRUE(store_test::put_shard_route(
                        store, make_route({table.table_id, 0}, {ReplicaRole::LEADER,
                                                                ReplicaRole::LEADER}))
                        .ok());
        RouteService service(&store);
        std::vector<ShardRoute> routes;
        GetTableRoutesParam param{"commerce", "orders", false};
        EXPECT_EQ(service.get_table_routes(param, nullptr, &routes).code(), StatusCode::ROUTE_NOT_FOUND);
    }
    {
        MetaStore store{MemoryMetaStoreParam{}};
        ASSERT_TRUE(store.init().ok());
        const auto table = make_table();
        ASSERT_TRUE(store_test::put_table(store, table).ok());
        ASSERT_TRUE(
            store_test::put_shard_route(
                store, make_route({table.table_id, 0}, {ReplicaRole::LEADER}, false))
                .ok());
        RouteService service(&store);
        std::vector<ShardRoute> routes;
        GetTableRoutesParam param{"commerce", "orders", false};
        EXPECT_EQ(service.get_table_routes(param, nullptr, &routes).code(), StatusCode::ROUTE_NOT_FOUND);
    }
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
    std::vector<ShardRoute> routes;
    // 缺失 shard 路由时，仅 include_unavailable 可返回整表。
    GetTableRoutesParam param{"commerce", "orders", true};
    ASSERT_TRUE(service.get_table_routes(param, nullptr, &routes).ok());
    ASSERT_EQ(routes.size(), 2U);
    EXPECT_EQ(routes[0].hash_range, (HashRange{0, h}));
    EXPECT_TRUE(routes[0].replicas.empty());
    EXPECT_EQ(routes[1].hash_range, (HashRange{h, K_HASH_BUCKET_COUNT}));
    EXPECT_EQ(routes[1].replicas.size(), 2U);
    EXPECT_EQ(routes[1].replicas[0].role, ReplicaRole::LEADER);
    EXPECT_EQ(routes[1].replicas[1].role, ReplicaRole::FOLLOWER);

    param.include_unavailable = false;
    EXPECT_EQ(service.get_table_routes(param, nullptr, &routes).code(), StatusCode::ROUTE_NOT_FOUND);
}

TEST(RouteServiceTest, TableRoutesReturnsTableNotFoundWhenTableMissing) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    RouteService service(&store);
    std::vector<ShardRoute> routes;
    GetTableRoutesParam param{"commerce", "orders", true};
    EXPECT_EQ(service.get_table_routes(param, nullptr, &routes).code(), StatusCode::TABLE_NOT_FOUND);
}

}  // namespace adviskv::meta
