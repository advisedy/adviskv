#include "sdm/service/route_service.h"

#include <gtest/gtest.h>

#include <functional>

#include "common/status.h"
#include "sdm/model/sdm_store.h"

namespace adviskv::sdm {
namespace {

Table make_table() {
    return Table{
        .table_id = 1001,
        .spec{
            .table_name = "orders",
            .db_id = 11,
            .db_name = "commerce",
            .shard_count = 4,
            .replica_count = 2,
            .resource_pool = "pool-a",
            .operation_id = "create-table-1001",
        },
        .state{
            .desired = TableDesired::PRESENT,
            .phase = TablePhase::READY,
        },
    };
}

ShardID shard_for_key(const Table& table, const Key& key) {
    return ShardID{
        .table_id = table.table_id,
        .shard_index = static_cast<ShardIndex>(std::hash<Key>{}(key) %
                                               table.spec.shard_count),
    };
}

ShardRoute make_route(const ShardID& shard_id,
                      std::vector<ReplicaRole> roles = {ReplicaRole::LEADER,
                                                        ReplicaRole::FOLLOWER},
                      bool valid_leader_endpoint = true) {
    ShardRoute route{.shard_id = shard_id};
    for (size_t i = 0; i < roles.size(); ++i) {
        const bool leader = roles[i] == ReplicaRole::LEADER;
        route.replicas.push_back(RouteEntry{
            .replica_id =
                ReplicaID{.table_id = shard_id.table_id,
                          .shard_index = shard_id.shard_index,
                          .replica_index = static_cast<ReplicaIndex>(i)},
            .node_id = "node-" + std::to_string(i),
            .ip = leader && !valid_leader_endpoint
                      ? ""
                      : "127.0.0." + std::to_string(i + 1),
            .port = leader && !valid_leader_endpoint
                        ? 0
                        : static_cast<int32_t>(18080 + i),
            .role = roles[i],
            .term = 7,
        });
    }
    return route;
}

GetRouteParam make_get_route_param(const Key& key = "user-123") {
    return GetRouteParam{
        .db_name = "commerce",
        .table_name = "orders",
        .key = key,
    };
}

}  // namespace

// 检测 RouteService 会根据 DB、Table 和 Key 计算 shard 并返回对应路由。
TEST(RouteServiceTest, GetRouteReturnsRouteForTableAndKey) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    Table table = make_table();
    const Key key = "user-123";
    ShardID shard_id = shard_for_key(table, key);
    ASSERT_TRUE(store.put_table(table).ok());
    ASSERT_TRUE(store.put_shard_route(make_route(shard_id)).ok());
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
    SdmStore store{SdmMetaStoreType::MEMORY};
    RouteService service(&store);
    ShardRoute route;

    Status status = service.get_route(
        GetRouteParam{.db_name = "", .table_name = "orders", .key = "key"},
        &route);

    EXPECT_EQ(status.code(), StatusCode::INVALID_ARGUMENT);
}

// 检测 Table 或者  shard route 不存在
TEST(RouteServiceTest, GetRouteReturnsTableNotFoundWhenTableMissing) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    RouteService service(&store);
    ShardRoute route;
    {
        Status status = service.get_route(make_get_route_param(), &route);

        EXPECT_EQ(status.code(), StatusCode::TABLE_NOT_FOUND);
    }
    ASSERT_TRUE(store.put_table(make_table()).ok());
    {
        Status status = service.get_route(make_get_route_param(), &route);

        EXPECT_EQ(status.code(), StatusCode::ROUTE_NOT_FOUND);
    }
}

// 检测没有可写 leader、多个 leader 或 leader endpoint 无效时，RouteService 返回
// ROUTE_NOT_FOUND。
TEST(RouteServiceTest, GetRouteRequiresExactlyOneWritableLeader) {
    {
        SdmStore store{SdmMetaStoreType::MEMORY};
        Table table = make_table();
        ShardID shard_id = shard_for_key(table, "user-123");
        ASSERT_TRUE(store.put_table(table).ok());
        ASSERT_TRUE(
            store
                .put_shard_route(make_route(
                    shard_id, {ReplicaRole::FOLLOWER, ReplicaRole::FOLLOWER}))
                .ok());
        RouteService service(&store);
        ShardRoute route;
        EXPECT_EQ(service.get_route(make_get_route_param(), &route).code(),
                  StatusCode::ROUTE_NOT_FOUND);
    }
    {
        SdmStore store{SdmMetaStoreType::MEMORY};
        Table table = make_table();
        ShardID shard_id = shard_for_key(table, "user-123");
        ASSERT_TRUE(store.put_table(table).ok());
        ASSERT_TRUE(
            store
                .put_shard_route(make_route(
                    shard_id, {ReplicaRole::LEADER, ReplicaRole::LEADER}))
                .ok());
        RouteService service(&store);
        ShardRoute route;
        EXPECT_EQ(service.get_route(make_get_route_param(), &route).code(),
                  StatusCode::ROUTE_NOT_FOUND);
    }
    {
        SdmStore store{SdmMetaStoreType::MEMORY};
        Table table = make_table();
        ShardID shard_id = shard_for_key(table, "user-123");
        ASSERT_TRUE(store.put_table(table).ok());
        ASSERT_TRUE(store
                        .put_shard_route(
                            make_route(shard_id, {ReplicaRole::LEADER}, false))
                        .ok());
        RouteService service(&store);
        ShardRoute route;
        EXPECT_EQ(service.get_route(make_get_route_param(), &route).code(),
                  StatusCode::ROUTE_NOT_FOUND);
    }
}

}  // namespace adviskv::sdm