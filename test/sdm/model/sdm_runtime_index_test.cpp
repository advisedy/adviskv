#include "sdm/model/sdm_runtime_index.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <vector>

namespace adviskv::sdm {
namespace {

Table make_table(TableID table_id, const std::string& db_name,
                 const std::string& table_name,
                 TableDesired desired = TableDesired::PRESENT) {
    TableState state{};
    state.desired = desired;
    state.phase = TablePhase::CREATING;
    return Table{table_id,
                 TableSpec{table_name, 11, db_name, 2, 2, "pool-a",
                           "create-table-" + std::to_string(table_id)},
                 state};
}

Node make_node(const NodeID& id, const std::string& resource_pool) {
    return Node{id,
                NodeSpec{resource_pool, "dc-a", NodeStatus::ONLINE},
                NodeState{Endpoint{"127.0.0.1", 18080}},
                NodeDerived{}};
}

Replica make_replica(const ReplicaID& replica_id, const NodeID& node_id) {
    ReplicaState state{};
    state.desired = ReplicaDesired::PRESENT;
    state.phase = ReplicaPhase::READY;
    state.observed_raft_role = ReplicaRole::FOLLOWER;
    state.observed_endpoint = Endpoint{"127.0.0.1", 18080};
    return Replica{replica_id,
                   ReplicaSpec{"dc-a", node_id, EngineType::MAP, {}},
                   state};
}

ShardRoute make_route(const ShardID& shard_id) {
    return ShardRoute{shard_id,
                      {RouteEntry{ReplicaID{shard_id.table_id,
                                            shard_id.shard_index, 0},
                                  "node-a", "127.0.0.1", 18080,
                                  ReplicaRole::LEADER, 7},
                       RouteEntry{ReplicaID{shard_id.table_id,
                                            shard_id.shard_index, 1},
                                  "node-b", "127.0.0.2", 18081,
                                  ReplicaRole::FOLLOWER, 7}}};
}

std::vector<NodeID> sorted_nodes(std::vector<NodeID> nodes) {
    std::sort(nodes.begin(), nodes.end());
    return nodes;
}

std::vector<ReplicaID> sorted_replicas(std::vector<ReplicaID> replicas) {
    std::sort(replicas.begin(), replicas.end(),
              [](const ReplicaID& lhs, const ReplicaID& rhs) {
                  if (lhs.table_id != rhs.table_id) {
                      return lhs.table_id < rhs.table_id;
                  }
                  if (lhs.shard_index != rhs.shard_index) {
                      return lhs.shard_index < rhs.shard_index;
                  }
                  return lhs.replica_seq < rhs.replica_seq;
              });
    return replicas;
}

}  // namespace

// 检测 runtime index 的正常 upsert、查询、更新和删除流程。
TEST(SdmRuntimeIndexTest, NormalIndexFlowWorks) {
    SdmRuntimeIndex index;

    Table table = make_table(1001, "commerce", "orders");
    ASSERT_TRUE(index.on_table_upsert(nullptr, table).ok());
    TableID table_id = -1;
    ASSERT_TRUE(index.find_table_by_name("commerce", "orders", table_id).ok());
    EXPECT_EQ(table_id, 1001);

    Table renamed = make_table(1001, "commerce_v2", "orders_v2");
    ASSERT_TRUE(index.on_table_upsert(&table, renamed).ok());
    EXPECT_EQ(index.find_table_by_name("commerce", "orders", table_id).code(),
              StatusCode::TABLE_NOT_FOUND);
    ASSERT_TRUE(
        index.find_table_by_name("commerce_v2", "orders_v2", table_id).ok());
    EXPECT_EQ(table_id, 1001);

    Node node_a = make_node("node-a", "pool-a");
    Node node_b = make_node("node-b", "pool-a");
    ASSERT_TRUE(index.on_node_upsert(nullptr, node_a).ok());
    ASSERT_TRUE(index.on_node_upsert(nullptr, node_b).ok());
    std::vector<NodeID> nodes;
    ASSERT_TRUE(index.list_nodes_by_resource_pool("pool-a", nodes).ok());
    EXPECT_EQ(sorted_nodes(nodes), std::vector<NodeID>({"node-a", "node-b"}));

    Node migrated_node_a = make_node("node-a", "pool-b");
    ASSERT_TRUE(index.on_node_upsert(&node_a, migrated_node_a).ok());
    ASSERT_TRUE(index.list_nodes_by_resource_pool("pool-a", nodes).ok());
    EXPECT_EQ(sorted_nodes(nodes), std::vector<NodeID>({"node-b"}));
    ASSERT_TRUE(index.list_nodes_by_resource_pool("pool-b", nodes).ok());
    EXPECT_EQ(sorted_nodes(nodes), std::vector<NodeID>({"node-a"}));

    ReplicaID replica_id{1001, 0, 0};
    Replica replica = make_replica(replica_id, "node-a");
    ASSERT_TRUE(index.on_replica_upsert(nullptr, replica).ok());
    std::vector<ReplicaID> replicas;
    ASSERT_TRUE(index.list_replicas_by_shard(ShardID{1001, 0}, replicas).ok());
    EXPECT_EQ(sorted_replicas(replicas), std::vector<ReplicaID>({replica_id}));
    ASSERT_TRUE(index.list_replicas_by_node("node-a", replicas).ok());
    EXPECT_EQ(sorted_replicas(replicas), std::vector<ReplicaID>({replica_id}));

    Replica moved_replica = make_replica(replica_id, "node-b");
    ASSERT_TRUE(index.on_replica_upsert(&replica, moved_replica).ok());
    ASSERT_TRUE(index.list_replicas_by_node("node-a", replicas).ok());
    EXPECT_TRUE(replicas.empty());
    ASSERT_TRUE(index.list_replicas_by_node("node-b", replicas).ok());
    EXPECT_EQ(sorted_replicas(replicas), std::vector<ReplicaID>({replica_id}));

    ShardID shard_id{1001, 0};
    ShardRoute route = make_route(shard_id);
    ASSERT_TRUE(index.put_shard_route(route).ok());
    ShardRoutePtr loaded_route;
    ASSERT_TRUE(index.get_shard_route(shard_id, loaded_route).ok());
    ASSERT_NE(loaded_route, nullptr);
    ASSERT_EQ(loaded_route->replicas.size(), 2U);

    ASSERT_TRUE(
        index.del_shard_route_entry(shard_id, ReplicaID{1001, 0, 1}).ok());
    ASSERT_TRUE(index.get_shard_route(shard_id, loaded_route).ok());
    ASSERT_NE(loaded_route, nullptr);
    ASSERT_EQ(loaded_route->replicas.size(), 1U);
    EXPECT_EQ(loaded_route->replicas[0].replica_id, (ReplicaID{1001, 0, 0}));

    ASSERT_TRUE(index.on_replica_delete(moved_replica).ok());
    ASSERT_TRUE(index.list_replicas_by_shard(shard_id, replicas).ok());
    EXPECT_TRUE(replicas.empty());
    ASSERT_TRUE(index.on_node_delete(migrated_node_a).ok());
    ASSERT_TRUE(index.list_nodes_by_resource_pool("pool-b", nodes).ok());
    EXPECT_TRUE(nodes.empty());
    ASSERT_TRUE(index.on_table_delete(renamed).ok());
    EXPECT_EQ(
        index.find_table_by_name("commerce_v2", "orders_v2", table_id).code(),
        StatusCode::TABLE_NOT_FOUND);
    ASSERT_TRUE(index.delete_shard_route(shard_id).ok());
    ASSERT_TRUE(index.get_shard_route(shard_id, loaded_route).ok());
    EXPECT_EQ(loaded_route, nullptr);
}

}  // namespace adviskv::sdm