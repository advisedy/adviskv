#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

#include "sdm/store/runtime_store.h"

namespace adviskv::sdm {
namespace {

Node make_node(const NodeID& id, const std::string& resource_pool,
               int32_t port = 18080) {
    return Node{id,
                NodeMeta{resource_pool, "dc-a"},
                NodeState{NodeStatus::ONLINE, Endpoint{"127.0.0.1", port},
                          123456},
                NodeDerived{3, 1}};
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

std::vector<NodeID> sorted_node_ids(const std::vector<NodePtr>& nodes) {
    std::vector<NodeID> ids;
    ids.reserve(nodes.size());
    for (const auto& node : nodes) {
        ids.push_back(node->id);
    }
    std::sort(ids.begin(), ids.end());
    return ids;
}

}  // namespace

TEST(SdmRuntimeStoreTest, NodeUpsertGetAndListWork) {
    SdmRuntimeStore store;
    Node node_a = make_node("node-a", "pool-a");
    Node node_b = make_node("node-b", "pool-b", 18081);

    ASSERT_TRUE(store.upsert_node(node_a).ok());
    ASSERT_TRUE(store.upsert_node(node_b).ok());

    NodePtr out;
    ASSERT_TRUE(store.get_node(node_a.id, out).ok());
    ASSERT_TRUE(out != nullptr);
    EXPECT_EQ(out->meta.resource_pool, "pool-a");
    EXPECT_EQ(out->state.endpoint.port, 18080);
    EXPECT_EQ(out->derived.owned_replica_count, 3);

    Node updated = make_node(node_a.id, "pool-c", 19080);
    ASSERT_TRUE(store.upsert_node(updated).ok());
    ASSERT_TRUE(store.get_node(node_a.id, out).ok());
    ASSERT_TRUE(out != nullptr);
    EXPECT_EQ(out->meta.resource_pool, "pool-c");
    EXPECT_EQ(out->state.endpoint.port, 19080);

    std::vector<NodePtr> nodes;
    ASSERT_TRUE(store.list_nodes(nodes).ok());
    EXPECT_EQ(sorted_node_ids(nodes),
              std::vector<NodeID>({"node-a", "node-b"}));

    ASSERT_TRUE(store.get_node("missing-node", out).ok());
    EXPECT_EQ(out, nullptr);
}

TEST(SdmRuntimeStoreTest, RoutePutGetDeleteAndEntryMutationWork) {
    SdmRuntimeStore store;
    ShardID shard_id{1001, 0};

    ASSERT_TRUE(store.put_shard_route(make_route(shard_id)).ok());

    ShardRoutePtr route;
    ASSERT_TRUE(store.get_shard_route(shard_id, route).ok());
    ASSERT_NE(route, nullptr);
    ASSERT_EQ(route->replicas.size(), 2U);

    ASSERT_TRUE(
        store.del_shard_route_entry(shard_id, ReplicaID{1001, 0, 1}).ok());
    ASSERT_TRUE(store.get_shard_route(shard_id, route).ok());
    ASSERT_NE(route, nullptr);
    ASSERT_EQ(route->replicas.size(), 1U);
    EXPECT_EQ(route->replicas[0].replica_id, (ReplicaID{1001, 0, 0}));

    ASSERT_TRUE(store.delete_shard_route(shard_id).ok());
    ASSERT_TRUE(store.get_shard_route(shard_id, route).ok());
    EXPECT_EQ(route, nullptr);
}

TEST(SdmRuntimeStoreTest, CloneCopiesRuntimeObjectsIndependently) {
    SdmRuntimeStore store;
    NodeID node_id = "node-a";
    ShardID shard_id{1001, 0};

    ASSERT_TRUE(store.upsert_node(make_node(node_id, "pool-a")).ok());
    ASSERT_TRUE(store.put_shard_route(make_route(shard_id)).ok());

    std::unique_ptr<SdmRuntimeStore> snapshot = store.clone();
    ASSERT_TRUE(snapshot != nullptr);

    ASSERT_TRUE(store.upsert_node(make_node(node_id, "pool-b", 19080)).ok());
    ASSERT_TRUE(
        store.del_shard_route_entry(shard_id, ReplicaID{1001, 0, 1}).ok());

    NodePtr node;
    ASSERT_TRUE(snapshot->get_node(node_id, node).ok());
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->meta.resource_pool, "pool-a");
    EXPECT_EQ(node->state.endpoint.port, 18080);

    ShardRoutePtr route;
    ASSERT_TRUE(snapshot->get_shard_route(shard_id, route).ok());
    ASSERT_NE(route, nullptr);
    ASSERT_EQ(route->replicas.size(), 2U);
}

}  // namespace adviskv::sdm
