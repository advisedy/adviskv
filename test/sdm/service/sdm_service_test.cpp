#include <gtest/gtest.h>

#include <functional>
#include <string>
#include <vector>

#include "common/status.h"
#include "sdm/model/sdm_store.h"
#include "sdm/selector/node_selector/node_selector.h"
#include "sdm/service/heartbeat_service.h"
#include "sdm/service/node_service.h"
#include "sdm/service/route_service.h"
#include "sdm/service/table_service.h"

namespace adviskv::sdm {
namespace {

RegisterNodeParam make_register_node_param() {
    return RegisterNodeParam{
        .node_id = "node-a",
        .ip = "127.0.0.1",
        .port = 18080,
        .resource_pool = "pool-a",
        .dc = "dc-a",
        .last_heartbeat_ts = 123456,
    };
}

Node make_node(const NodeID& id, const std::string& pool, int32_t port) {
    return Node{
        .id = id,
        .spec{
            .resource_pool = pool,
            .dc = "dc-a",
            .status = NodeStatus::ONLINE,
        },
        .state{
            .endpoint = Endpoint{.ip = "127.0.0.1", .port = port},
            .last_heartbeat_ts = 1,
        },
    };
}

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
            .operation_id = "create-table-1001-test",
        },
        .state{
            .desired = TableDesired::PRESENT,
            .phase = TablePhase::READY,
        },
    };
}

PlaceTableParam make_place_table_param() {
    return PlaceTableParam{
        .db_id = 11,
        .table_id = 1002,
        .db_name = "commerce",
        .table_name = "payments",
        .replica_count = 2,
        .shard_count = 3,
        .resource_pool = "pool-a",
        .operation_id = "create-table-1002-test",
    };
}

Replica make_replica(const ReplicaID& replica_id, const NodeID& node_id,
                     ReplicaStatus status = ReplicaStatus::ADDING) {
    return Replica{
        .replica_id = replica_id,
        .spec{
            .dc = "dc-a",
            .assign_node_id = node_id,
            .engine_type = EngineType::MAP,
        },
        .state{
            .desired = ReplicaDesired::PRESENT,
            .phase = status == ReplicaStatus::READY ? ReplicaPhase::READY
                                                     : ReplicaPhase::CREATING,
            .observed_role = ReplicaRole::FOLLOWER,
            .observed_endpoint = Endpoint{.ip = "127.0.0.1", .port = 18080},
        },
    };
}

ShardRoute make_route(const ShardID& shard_id) {
    return ShardRoute{
        .shard_id = shard_id,
        .replicas =
            {
                RouteEntry{
                    .replica_id =
                        ReplicaID{shard_id.table_id, shard_id.shard_index, 0},
                    .node_id = "node-a",
                    .ip = "127.0.0.1",
                    .port = 18080,
                    .role = ReplicaRole::LEADER,
                },
                RouteEntry{
                    .replica_id =
                        ReplicaID{shard_id.table_id, shard_id.shard_index, 1},
                    .node_id = "node-b",
                    .ip = "127.0.0.2",
                    .port = 18081,
                    .role = ReplicaRole::FOLLOWER,
                },
            },
    };
}

// 验证NodeService注册合法节点时，会把节点信息写入SdmStore。
TEST(NodeServiceTest, RegisterNodeStoresValidNodeMeta) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    NodeService service(&store);

    Status status = service.register_node(make_register_node_param());

    ASSERT_TRUE(status.ok()) << status.msg();
    NodePtr stored;
    ASSERT_TRUE(store.get_node("node-a", stored).ok());
    ASSERT_NE(stored, nullptr);
    EXPECT_EQ(stored->id, "node-a");
    EXPECT_EQ(stored->spec.resource_pool, "pool-a");
    EXPECT_EQ(stored->spec.dc, "dc-a");
    EXPECT_EQ(stored->spec.status, NodeStatus::ONLINE);
    EXPECT_EQ(stored->state.endpoint.ip, "127.0.0.1");
    EXPECT_EQ(stored->state.endpoint.port, 18080);
    EXPECT_EQ(stored->state.last_heartbeat_ts, 123456);
}

// 验证NodeService会拒绝缺少node_id、ip或端口非法的注册参数。
TEST(NodeServiceTest, RegisterNodeRejectsInvalidParams) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    NodeService service(&store);
    std::vector<RegisterNodeParam> invalid_params;

    RegisterNodeParam empty_node_id = make_register_node_param();
    empty_node_id.node_id.clear();
    invalid_params.push_back(empty_node_id);

    RegisterNodeParam empty_ip = make_register_node_param();
    empty_ip.ip.clear();
    invalid_params.push_back(empty_ip);

    RegisterNodeParam invalid_port = make_register_node_param();
    invalid_port.port = 0;
    invalid_params.push_back(invalid_port);

    for (const RegisterNodeParam& param : invalid_params) {
        Status status = service.register_node(param);
        EXPECT_EQ(status.code(), StatusCode::INVALID_ARGUMENT);
    }
}

// 验证TableService会把建表参数转换为声明式Table desired state。
TEST(TableServiceTest, PlaceTableConvertsParamToDesiredTableState) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    TableService service(&store);

    PlaceTableParam param = make_place_table_param();
    Status status = service.place_table(param);

    ASSERT_TRUE(status.ok()) << status.msg();
    TablePtr stored;
    ASSERT_TRUE(store.get_table(param.table_id, stored).ok());
    ASSERT_NE(stored, nullptr);
    EXPECT_EQ(stored->table_id, param.table_id);
    EXPECT_EQ(stored->spec.table_name, param.table_name);
    EXPECT_EQ(stored->spec.db_name, param.db_name);
    EXPECT_EQ(stored->spec.db_id, param.db_id);
    EXPECT_EQ(stored->spec.replica_count, param.replica_count);
    EXPECT_EQ(stored->spec.shard_count, param.shard_count);
    EXPECT_EQ(stored->spec.resource_pool, param.resource_pool);
    EXPECT_EQ(stored->spec.operation_id, param.operation_id);
    EXPECT_EQ(stored->state.desired, TableDesired::PRESENT);
    EXPECT_EQ(stored->state.phase, TablePhase::CREATING);
}

// 验证同一个operation_id的重试会被TableService当成幂等成功处理。
TEST(TableServiceTest, PlaceTableTreatsSameOperationIdRetryAsIdempotent) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    TableService service(&store);

    PlaceTableParam param = make_place_table_param();
    ASSERT_TRUE(service.place_table(param).ok());

    Status retry_status = service.place_table(param);

    EXPECT_TRUE(retry_status.ok()) << retry_status.msg();
}

// 验证相同table_id但不同operation_id不会被误认为幂等重试。
TEST(TableServiceTest, PlaceTableRejectsConflictingOperationId) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    TableService service(&store);

    PlaceTableParam param = make_place_table_param();
    ASSERT_TRUE(service.place_table(param).ok());
    PlaceTableParam conflict = param;
    conflict.operation_id = "create-table-1002-other";

    Status status = service.place_table(conflict);

    EXPECT_EQ(status.code(), StatusCode::ALREADY_EXIST);
}

// 验证TableService可以按table_id和operation_id查询当前建表状态。
TEST(TableServiceTest, GetTableStatusReturnsStoredTableState) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    TableService service(&store);

    PlaceTableParam param = make_place_table_param();
    ASSERT_TRUE(service.place_table(param).ok());

    Table table;
    Status status = service.get_table_status(
        GetTableStatusParam{
            .operation_id = param.operation_id,
            .table_id = param.table_id,
        },
        &table);

    ASSERT_TRUE(status.ok()) << status.msg();
    EXPECT_EQ(table.table_id, param.table_id);
    EXPECT_EQ(table.spec.operation_id, param.operation_id);
    EXPECT_EQ(table.state.desired, TableDesired::PRESENT);
    EXPECT_EQ(table.state.phase, TablePhase::CREATING);
}

// 验证TableService会在写入desired state前拦截非法建表参数。
TEST(TableServiceTest, PlaceTableRejectsInvalidParamBeforePersist) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    TableService service(&store);

    PlaceTableParam param = make_place_table_param();
    param.table_name.clear();

    Status status = service.place_table(param);

    EXPECT_EQ(status.code(), StatusCode::INVALID_ARGUMENT);
    TablePtr stored;
    ASSERT_TRUE(store.get_table(param.table_id, stored).ok());
    EXPECT_EQ(stored, nullptr);
}

// 验证RouteService能根据DB、Table和Key计算shard并返回对应路由。
TEST(RouteServiceTest, GetRouteReturnsRouteForTableAndKey) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    Table table = make_table();
    const Key key = "user-123";
    ShardID shard_id{
        .table_id = table.table_id,
        .shard_index = static_cast<ShardIndex>(std::hash<Key>{}(key) %
                                               table.spec.shard_count),
    };
    ASSERT_TRUE(store.put_table(table).ok());
    ASSERT_TRUE(store.put_shard_route(make_route(shard_id)).ok());
    RouteService service(&store);

    ShardRoute route;
    Status status = service.get_route(
        GetRouteParam{
            .db_name = table.spec.db_name,
            .table_name = table.spec.table_name,
            .key = key,
        },
        &route);

    ASSERT_TRUE(status.ok()) << status.msg();
    EXPECT_EQ(route.shard_id, shard_id);
    ASSERT_EQ(route.replicas.size(), 2U);
    EXPECT_EQ(route.replicas[0].node_id, "node-a");
    EXPECT_EQ(route.replicas[0].role, ReplicaRole::LEADER);
    EXPECT_EQ(route.replicas[1].node_id, "node-b");
    EXPECT_EQ(route.replicas[1].role, ReplicaRole::FOLLOWER);
}

// 验证RouteService在Table不存在时返回TABLE_NOT_FOUND。
TEST(RouteServiceTest, GetRouteReturnsTableNotFoundWhenTableMissing) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    RouteService service(&store);
    ShardRoute route;

    Status status = service.get_route(
        GetRouteParam{
            .db_name = "commerce",
            .table_name = "missing",
            .key = "user-123",
        },
        &route);

    EXPECT_EQ(status.code(), StatusCode::TABLE_NOT_FOUND);
}

// 验证RouteService在shard路由不存在时返回ROUTE_NOT_FOUND。
TEST(RouteServiceTest, GetRouteReturnsRouteNotFoundWhenRouteMissing) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    Table table = make_table();
    ASSERT_TRUE(store.put_table(table).ok());
    RouteService service(&store);
    ShardRoute route;

    Status status = service.get_route(
        GetRouteParam{
            .db_name = table.spec.db_name,
            .table_name = table.spec.table_name,
            .key = "user-123",
        },
        &route);

    EXPECT_EQ(status.code(), StatusCode::ROUTE_NOT_FOUND);
}

// 验证HeartbeatService会更新节点心跳信息，并同步更新该节点上的Replica状态。
TEST(HeartBeatServiceTest, HeartbeatUpdatesNodeAndAssignedReplicas) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store.put_node(make_node("node-a", "pool-a", 18080)).ok());
    ASSERT_TRUE(
        store.put_replica(make_replica(ReplicaID{1001, 0, 0}, "node-a")).ok());
    ASSERT_TRUE(
        store.put_replica(make_replica(ReplicaID{1001, 0, 1}, "node-b")).ok());
    HeartBeatService service(&store);

    Status status = service.heartbeat(HeartBeatParam{
        .node_id = "node-a",
        .ip = "10.0.0.1",
        .port = 19090,
        .resoure_pool_name = "pool-a",
        .dc = "dc-a",
        .replica_list =
            {
                HeartBeatReplicaInfo{
                    .shard_id = ShardID{.table_id = 1001, .shard_index = 0},
                    .replica_index = 0,
                    .role = ReplicaRole::LEADER,
                    .status = ReplicaStatus::READY,
                },
                HeartBeatReplicaInfo{
                    .shard_id = ShardID{.table_id = 1001, .shard_index = 0},
                    .replica_index = 1,
                    .role = ReplicaRole::FOLLOWER,
                    .status = ReplicaStatus::READY,
                },
            },
        .last_heartbeat_ts = 987654,
    });

    ASSERT_TRUE(status.ok()) << status.msg();
    NodePtr node;
    ASSERT_TRUE(store.get_node("node-a", node).ok());
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->state.endpoint.ip, "10.0.0.1");
    EXPECT_EQ(node->state.endpoint.port, 19090);
    EXPECT_EQ(node->state.last_heartbeat_ts, 987654);

    ReplicaPtr assigned_replica;
    ASSERT_TRUE(
        store.get_replica(ReplicaID{1001, 0, 0}, assigned_replica).ok());
    ASSERT_NE(assigned_replica, nullptr);
    EXPECT_EQ(assigned_replica->state.phase, ReplicaPhase::READY);
    EXPECT_EQ(assigned_replica->state.observed_role, ReplicaRole::LEADER);
    EXPECT_EQ(assigned_replica->state.observed_endpoint.ip, "10.0.0.1");
    EXPECT_EQ(assigned_replica->state.observed_endpoint.port, 19090);

    ReplicaPtr other_node_replica;
    ASSERT_TRUE(
        store.get_replica(ReplicaID{1001, 0, 1}, other_node_replica).ok());
    ASSERT_NE(other_node_replica, nullptr);
    EXPECT_EQ(other_node_replica->state.phase, ReplicaPhase::CREATING);
    EXPECT_EQ(other_node_replica->state.observed_role, ReplicaRole::FOLLOWER);
    EXPECT_EQ(other_node_replica->state.observed_endpoint.ip, "127.0.0.1");
    EXPECT_EQ(other_node_replica->state.observed_endpoint.port, 18080);
}

// 验证HeartbeatService会拒绝缺少node_id等关键字段的非法心跳参数。
TEST(HeartBeatServiceTest, HeartbeatRejectsInvalidParam) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    HeartBeatService service(&store);

    Status status = service.heartbeat(HeartBeatParam{
        .node_id = "",
        .ip = "10.0.0.1",
        .port = 19090,
        .resoure_pool_name = "pool-a",
        .dc = "dc-a",
    });

    EXPECT_EQ(status.code(), StatusCode::INVALID_ARGUMENT);
}

// 还要再添加一些关于例如结合route_updater，然后route_service的表现
// 当leader的个数大于1的时候， 等于1的时候，小于1的时候


}  // namespace
}  // namespace adviskv::sdm
