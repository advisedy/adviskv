#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

#include "sdm/model/i_sdm_metastore.h"

namespace adviskv::sdm {
namespace {

Table make_table(TableID table_id, const std::string& table_name,
                 TablePhase phase = TablePhase::CREATING) {
    return Table{
        .table_id = table_id,
        .spec{
            .table_name = table_name,
            .db_id = 11,
            .db_name = "commerce",
            .shard_count = 2,
            .replica_count = 2,
            .resource_pool = "pool-a",
            .operation_id = "create-table-" + std::to_string(table_id),
        },
        .state{
            .desired = TableDesired::PRESENT,
            .phase = phase,
            .update_ts = 100 + table_id,
        },
    };
}

Node make_node(const NodeID& id, const std::string& resource_pool,
               int32_t port = 18080) {
    return Node{
        .id = id,
        .spec{
            .resource_pool = resource_pool,
            .dc = "dc-a",
            .status = NodeStatus::ONLINE,
        },
        .state{
            .endpoint = Endpoint{.ip = "127.0.0.1", .port = port},
            .last_heartbeat_ts = 123456,
        },
        .derived{
            .owned_replica_count = 3,
            .owned_leader_count = 1,
        },
    };
}

Replica make_replica(const ReplicaID& replica_id, const NodeID& node_id,
                     ReplicaRole role = ReplicaRole::FOLLOWER) {
    return Replica{
        .replica_id = replica_id,
        .spec{
            .dc = "dc-a",
            .assign_node_id = node_id,
            .engine_type = EngineType::MAP,
        },
        .state{
            .desired = ReplicaDesired::PRESENT,
            .phase = ReplicaPhase::READY,
            .observed_role = role,
            .observed_endpoint = Endpoint{.ip = "127.0.0.1", .port = 18080},
            .term = 7,
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
                    .term = 8,
                },
                RouteEntry{
                    .replica_id =
                        ReplicaID{shard_id.table_id, shard_id.shard_index, 1},
                    .node_id = "node-b",
                    .ip = "127.0.0.2",
                    .port = 18081,
                    .role = ReplicaRole::FOLLOWER,
                    .term = 8,
                },
            },
    };
}

std::vector<TableID> sorted_table_ids(const std::vector<TablePtr>& tables) {
    std::vector<TableID> ids;
    ids.reserve(tables.size());
    for (const auto& table : tables) {
        ids.push_back(table->table_id);
    }
    std::sort(ids.begin(), ids.end());
    return ids;
}

std::vector<NodeID> sorted_node_ids(const std::vector<NodePtr>& nodes) {
    std::vector<NodeID> ids;
    ids.reserve(nodes.size());
    for (const auto& node : nodes) {
        ids.push_back(node->id);
    }
    std::sort(ids.begin(), ids.end());
    return ids;
}

std::vector<ReplicaID> sorted_replica_ids(
    const std::vector<ReplicaPtr>& replicas) {
    std::vector<ReplicaID> ids;
    ids.reserve(replicas.size());
    for (const auto& replica : replicas) {
        ids.push_back(replica->replica_id);
    }
    std::sort(ids.begin(), ids.end(),
              [](const ReplicaID& lhs, const ReplicaID& rhs) {
                  if (lhs.table_id != rhs.table_id) {
                      return lhs.table_id < rhs.table_id;
                  }
                  if (lhs.shard_index != rhs.shard_index) {
                      return lhs.shard_index < rhs.shard_index;
                  }
                  return lhs.replica_index < rhs.replica_index;
              });
    return ids;
}

std::vector<std::string> sorted_pool_names(
    const std::vector<ResourcePoolPtr>& pools) {
    std::vector<std::string> names;
    names.reserve(pools.size());
    for (const auto& pool : pools) {
        names.push_back(pool->name);
    }
    std::sort(names.begin(), names.end());
    return names;
}

std::vector<ShardID> sorted_shard_ids(
    const std::vector<ShardRoutePtr>& routes) {
    std::vector<ShardID> ids;
    ids.reserve(routes.size());
    for (const auto& route : routes) {
        ids.push_back(route->shard_id);
    }
    std::sort(ids.begin(), ids.end(),
              [](const ShardID& lhs, const ShardID& rhs) {
                  if (lhs.table_id != rhs.table_id) {
                      return lhs.table_id < rhs.table_id;
                  }
                  return lhs.shard_index < rhs.shard_index;
              });
    return ids;
}

// 验证Table这几个基础接口能不能正常新增、覆盖、查询、列表和删除。
TEST(MemoryMetaStoreTest, TableUpsertGetListAndDeleteWork) {
    MemoryMetaStore store;
    Table table_a = make_table(1001, "orders");
    Table table_b = make_table(1002, "payments");

    ASSERT_TRUE(store.upsert_table(table_a).ok());
    ASSERT_TRUE(store.upsert_table(table_b).ok());

    TablePtr out;
    ASSERT_TRUE(store.get_table(table_a.table_id, out).ok());
    ASSERT_TRUE(out != nullptr);
    EXPECT_EQ(out->spec.table_name, "orders");
    EXPECT_EQ(out->state.phase, TablePhase::CREATING);

    Table updated =
        make_table(table_a.table_id, "orders_v2", TablePhase::READY);
    ASSERT_TRUE(store.upsert_table(updated).ok());
    ASSERT_TRUE(store.get_table(table_a.table_id, out).ok());
    ASSERT_TRUE(out != nullptr);
    EXPECT_EQ(out->spec.table_name, "orders_v2");
    EXPECT_EQ(out->state.phase, TablePhase::READY);

    std::vector<TablePtr> tables;
    ASSERT_TRUE(store.list_tables(tables).ok());
    EXPECT_EQ(sorted_table_ids(tables),
              std::vector<TableID>({table_a.table_id, table_b.table_id}));

    ASSERT_TRUE(store.delete_table(table_a.table_id).ok());
    ASSERT_TRUE(store.get_table(table_a.table_id, out).ok());
    EXPECT_EQ(out, nullptr);

    ASSERT_TRUE(store.list_tables(tables).ok());
    EXPECT_EQ(sorted_table_ids(tables),
              std::vector<TableID>({table_b.table_id}));
}

// 验证Node这边的写入、覆盖、查询和列表能力，缺失时也别乱返回脏指针。
TEST(MemoryMetaStoreTest, NodeUpsertGetAndListWork) {
    MemoryMetaStore store;
    Node node_a = make_node("node-a", "pool-a");
    Node node_b = make_node("node-b", "pool-b", 18081);

    ASSERT_TRUE(store.upsert_node(node_a).ok());
    ASSERT_TRUE(store.upsert_node(node_b).ok());

    NodePtr out;
    ASSERT_TRUE(store.get_node(node_a.id, out).ok());
    ASSERT_TRUE(out != nullptr);
    EXPECT_EQ(out->spec.resource_pool, "pool-a");
    EXPECT_EQ(out->state.endpoint.port, 18080);
    EXPECT_EQ(out->derived.owned_replica_count, 3);

    Node updated = make_node(node_a.id, "pool-c", 19080);
    ASSERT_TRUE(store.upsert_node(updated).ok());
    ASSERT_TRUE(store.get_node(node_a.id, out).ok());
    ASSERT_TRUE(out != nullptr);
    EXPECT_EQ(out->spec.resource_pool, "pool-c");
    EXPECT_EQ(out->state.endpoint.port, 19080);

    std::vector<NodePtr> nodes;
    ASSERT_TRUE(store.list_nodes(nodes).ok());
    EXPECT_EQ(sorted_node_ids(nodes),
              std::vector<NodeID>({"node-a", "node-b"}));

    ASSERT_TRUE(store.get_node("missing-node", out).ok());
    EXPECT_EQ(out, nullptr);
}

// 验证Replica单个写入、批量写入、覆盖、查询、列表和删除这些基本路径。
TEST(MemoryMetaStoreTest, ReplicaUpsertBatchGetListAndDeleteWork) {
    MemoryMetaStore store;
    Replica replica_a = make_replica(ReplicaID{1001, 0, 0}, "node-a");
    Replica replica_b = make_replica(ReplicaID{1001, 0, 1}, "node-b");
    Replica replica_c = make_replica(ReplicaID{1001, 1, 0}, "node-c");

    ASSERT_TRUE(store.upsert_replica(replica_a).ok());
    ASSERT_TRUE(store.upsert_replicas({replica_b, replica_c}).ok());

    ReplicaPtr out;
    ASSERT_TRUE(store.get_replica(replica_a.replica_id, out).ok());
    ASSERT_TRUE(out != nullptr);
    EXPECT_EQ(out->spec.assign_node_id, "node-a");
    EXPECT_EQ(out->state.phase, ReplicaPhase::READY);

    Replica updated =
        make_replica(replica_a.replica_id, "node-d", ReplicaRole::LEADER);
    ASSERT_TRUE(store.upsert_replica(updated).ok());
    ASSERT_TRUE(store.get_replica(replica_a.replica_id, out).ok());
    ASSERT_TRUE(out != nullptr);
    EXPECT_EQ(out->spec.assign_node_id, "node-d");
    EXPECT_EQ(out->state.observed_role, ReplicaRole::LEADER);

    std::vector<ReplicaPtr> replicas;
    ASSERT_TRUE(store.list_replicas(replicas).ok());
    EXPECT_EQ(
        sorted_replica_ids(replicas),
        std::vector<ReplicaID>({replica_a.replica_id, replica_b.replica_id,
                                replica_c.replica_id}));

    ASSERT_TRUE(store.delete_replica(replica_b.replica_id).ok());
    ASSERT_TRUE(store.get_replica(replica_b.replica_id, out).ok());
    EXPECT_EQ(out, nullptr);

    ASSERT_TRUE(store.list_replicas(replicas).ok());
    EXPECT_EQ(
        sorted_replica_ids(replicas),
        std::vector<ReplicaID>({replica_a.replica_id, replica_c.replica_id}));
}

// 验证ResourcePool按名字维护，写入、查询、列表、删除这几个接口都能跑通。
TEST(MemoryMetaStoreTest, ResourcePoolUpsertGetListAndDeleteWork) {
    MemoryMetaStore store;

    ASSERT_TRUE(
        store.upsert_resource_pool(ResourcePool{.name = "pool-a"}).ok());
    ASSERT_TRUE(
        store.upsert_resource_pool(ResourcePool{.name = "pool-b"}).ok());

    ResourcePoolPtr out;
    ASSERT_TRUE(store.get_resource_pool("pool-a", out).ok());
    ASSERT_TRUE(out != nullptr);
    EXPECT_EQ(out->name, "pool-a");

    ASSERT_TRUE(
        store.upsert_resource_pool(ResourcePool{.name = "pool-a-renamed"})
            .ok());
    ASSERT_TRUE(store.get_resource_pool("pool-a-renamed", out).ok());
    ASSERT_TRUE(out != nullptr);
    EXPECT_EQ(out->name, "pool-a-renamed");

    std::vector<ResourcePoolPtr> pools;
    ASSERT_TRUE(store.list_resource_pools(pools).ok());
    EXPECT_EQ(sorted_pool_names(pools),
              std::vector<std::string>({"pool-a", "pool-a-renamed", "pool-b"}));

    ASSERT_TRUE(store.delete_resource_pool("pool-b").ok());
    ASSERT_TRUE(store.get_resource_pool("pool-b", out).ok());
    EXPECT_EQ(out, nullptr);
}

// 验证ShardRoute按ShardID维护，路由的写入、覆盖、查询、列表、删除都正常。
TEST(MemoryMetaStoreTest, ShardRouteUpsertGetListAndDeleteWork) {
    MemoryMetaStore store;
    ShardRoute route_a = make_route(ShardID{1001, 0});
    ShardRoute route_b = make_route(ShardID{1001, 1});

    ASSERT_TRUE(store.upsert_shard_route(route_a).ok());
    ASSERT_TRUE(store.upsert_shard_route(route_b).ok());

    ShardRoutePtr out;
    ASSERT_TRUE(store.get_shard_route(route_a.shard_id, out).ok());
    ASSERT_TRUE(out != nullptr);
    ASSERT_EQ(out->replicas.size(), 2);
    EXPECT_EQ(out->replicas[0].replica_id, ReplicaID({1001, 0, 0}));
    EXPECT_EQ(out->replicas[0].role, ReplicaRole::LEADER);
    EXPECT_EQ(out->replicas[1].node_id, "node-b");

    ShardRoute updated = make_route(route_a.shard_id);
    updated.replicas[0].port = 19080;
    ASSERT_TRUE(store.upsert_shard_route(updated).ok());
    ASSERT_TRUE(store.get_shard_route(route_a.shard_id, out).ok());
    ASSERT_TRUE(out != nullptr);
    ASSERT_EQ(out->replicas.size(), 2);
    EXPECT_EQ(out->replicas[0].port, 19080);

    std::vector<ShardRoutePtr> routes;
    ASSERT_TRUE(store.list_shard_routes(routes).ok());
    EXPECT_EQ(sorted_shard_ids(routes),
              std::vector<ShardID>({route_a.shard_id, route_b.shard_id}));

    ASSERT_TRUE(store.delete_shard_route(route_a.shard_id).ok());
    ASSERT_TRUE(store.get_shard_route(route_a.shard_id, out).ok());
    EXPECT_EQ(out, nullptr);
}

// 验证clone_memory_snapshot真的是内存快照，后面原store再改也不应该污染它。
TEST(MemoryMetaStoreTest, CloneMemorySnapshotCopiesCurrentDataIndependently) {
    MemoryMetaStore store;
    TableID table_id = 1001;
    NodeID node_id = "node-a";
    ReplicaID replica_id{1001, 0, 0};
    ShardID shard_id{1001, 0};

    ASSERT_TRUE(store.upsert_table(make_table(table_id, "orders")).ok());
    ASSERT_TRUE(store.upsert_node(make_node(node_id, "pool-a")).ok());
    ASSERT_TRUE(store.upsert_replica(make_replica(replica_id, node_id)).ok());
    ASSERT_TRUE(
        store.upsert_resource_pool(ResourcePool{.name = "pool-a"}).ok());
    ASSERT_TRUE(store.upsert_shard_route(make_route(shard_id)).ok());

    std::unique_ptr<ISdmMetaStore> snapshot = store.clone_memory_snapshot();
    ASSERT_TRUE(snapshot != nullptr);

    ASSERT_TRUE(store.delete_table(table_id).ok());
    ASSERT_TRUE(store.upsert_node(make_node(node_id, "pool-b", 19080)).ok());
    ASSERT_TRUE(store.delete_replica(replica_id).ok());
    ASSERT_TRUE(store.delete_resource_pool("pool-a").ok());
    ASSERT_TRUE(store.delete_shard_route(shard_id).ok());

    TablePtr table;
    ASSERT_TRUE(snapshot->get_table(table_id, table).ok());
    ASSERT_TRUE(table != nullptr);
    EXPECT_EQ(table->spec.table_name, "orders");

    NodePtr node;
    ASSERT_TRUE(snapshot->get_node(node_id, node).ok());
    ASSERT_TRUE(node != nullptr);
    EXPECT_EQ(node->spec.resource_pool, "pool-a");
    EXPECT_EQ(node->state.endpoint.port, 18080);

    ReplicaPtr replica;
    ASSERT_TRUE(snapshot->get_replica(replica_id, replica).ok());
    ASSERT_TRUE(replica != nullptr);
    EXPECT_EQ(replica->spec.assign_node_id, node_id);

    ResourcePoolPtr pool;
    ASSERT_TRUE(snapshot->get_resource_pool("pool-a", pool).ok());
    ASSERT_TRUE(pool != nullptr);
    EXPECT_EQ(pool->name, "pool-a");

    ShardRoutePtr route;
    ASSERT_TRUE(snapshot->get_shard_route(shard_id, route).ok());
    ASSERT_TRUE(route != nullptr);
    ASSERT_EQ(route->replicas.size(), 2);
    EXPECT_EQ(route->replicas[0].replica_id, replica_id);
}

}  // namespace
}  // namespace adviskv::sdm