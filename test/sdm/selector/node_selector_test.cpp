#include "sdm/selector/node_selector/node_selector.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

#include "common/status.h"
#include "sdm/model/sdm_store.h"

namespace adviskv::sdm {
namespace {

Node make_node(const NodeID& id, const std::string& resource_pool, int32_t port,
               NodeStatus status = NodeStatus::ONLINE,
               const std::string& ip = "127.0.0.1") {
    return Node{id,
                NodeSpec{resource_pool, "dc-a", status},
                NodeState{Endpoint{ip, port}, 100},
                NodeDerived{}};
}

Replica make_replica(const ReplicaID& replica_id, const NodeID& node_id,
                     ReplicaDesired desired = ReplicaDesired::PRESENT) {
    return Replica{replica_id,
                   ReplicaSpec{"dc-a", node_id, EngineType::MAP, {}},
                   ReplicaState{desired, ReplicaPhase::READY,
                                ReplicaRole::FOLLOWER,
                                Endpoint{"127.0.0.1", 18080}}};
}

PlaceNodesParam make_param(int32_t shard_count = 1, int32_t replica_count = 2,
                           const std::string& resource_pool = "pool-a") {
    return PlaceNodesParam{resource_pool, shard_count, replica_count};
}

std::vector<NodeID> node_ids(const std::vector<Node>& nodes) {
    std::vector<NodeID> ids;
    ids.reserve(nodes.size());
    for (const Node& node : nodes) {
        ids.push_back(node.id);
    }
    return ids;
}

void expect_node_ids(const std::vector<Node>& nodes,
                     const std::vector<NodeID>& expected) {
    EXPECT_EQ(node_ids(nodes), expected);
}

}  // namespace

// 检测一下param有问题的时候
TEST(NodeSelectorTest, InvalidParamReturnsError) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    DefaultNodeSelector selector(&store);
    TablePlacementResult result;

    Status status = selector.select_table_nodes(
        PlaceNodesParam{"", 1, 1},
        result);
    EXPECT_EQ(status.code(), StatusCode::INVALID_ARGUMENT);

    status = selector.select_table_nodes(
        PlaceNodesParam{"pool-a", 0, 1},
        result);
    EXPECT_EQ(status.code(), StatusCode::INVALID_ARGUMENT);

    status = selector.select_table_nodes(
        PlaceNodesParam{"pool-a", 1, 0},
        result);
    EXPECT_EQ(status.code(), StatusCode::INVALID_ARGUMENT);
}

// 检测一下store的空指针
TEST(NodeSelectorTest, NullStoreReturnsError) {
    DefaultNodeSelector selector(nullptr);
    TablePlacementResult result;

    Status status = selector.select_table_nodes(make_param(), result);
    EXPECT_EQ(status.code(), StatusCode::INVALID_ARGUMENT);
}

// 检测是否会选择到不该被选到的node
TEST(NodeSelectorTest, SelectsOnlyHealthyNodesFromResourcePool) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store.put_node(make_node("node-a", "pool-a", 18080)).ok());
    ASSERT_TRUE(store.put_node(make_node("node-b", "pool-a", 18081)).ok());
    ASSERT_TRUE(store
                    .put_node(make_node("node-offline", "pool-a", 18082,
                                        NodeStatus::OFFLINE))
                    .ok());
    ASSERT_TRUE(store
                    .put_node(make_node("node-empty-ip", "pool-a", 18083,
                                        NodeStatus::ONLINE, ""))
                    .ok());
    ASSERT_TRUE(store.put_node(make_node("node-bad-port", "pool-a", 0)).ok());
    ASSERT_TRUE(
        store.put_node(make_node("node-other-pool", "pool-b", 18084)).ok());

    DefaultNodeSelector selector(&store);
    TablePlacementResult result;
    Status status = selector.select_table_nodes(make_param(), result);
    ASSERT_TRUE(status.ok());

    ASSERT_EQ(result.shards.size(), 1U);
    EXPECT_EQ(result.shards[0].shard_index, 0);
    expect_node_ids(result.shards[0].nodes, {"node-a", "node-b"});
}

// - 检测一下可选节点数量不足的情况
TEST(NodeSelectorTest, NotEnoughHealthyNodesReturnsResourceExhausted) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store.put_node(make_node("node-a", "pool-a", 18080)).ok());
    ASSERT_TRUE(store
                    .put_node(make_node("node-offline", "pool-a", 18081,
                                        NodeStatus::OFFLINE))
                    .ok());

    DefaultNodeSelector selector(&store);
    TablePlacementResult result;
    Status status = selector.select_table_nodes(make_param(), result);

    EXPECT_EQ(status.code(), StatusCode::RESOURCE_EXHAUSTED);
    EXPECT_TRUE(result.shards.empty());
}

// 检测一下是否会优先选择负载replica更少的node
TEST(NodeSelectorTest, PrefersNodesWithFewerPresentReplicas) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store.put_node(make_node("node-a", "pool-a", 18080)).ok());
    ASSERT_TRUE(store.put_node(make_node("node-b", "pool-a", 18081)).ok());
    ASSERT_TRUE(store.put_node(make_node("node-c", "pool-a", 18082)).ok());

    ASSERT_TRUE(
        store
            .put_replica(make_replica(
                ReplicaID{1, 0, 0},
                "node-a"))
            .ok());
    ASSERT_TRUE(
        store
            .put_replica(make_replica(
                ReplicaID{1, 1, 0},
                "node-a"))
            .ok());
    ASSERT_TRUE(
        store
            .put_replica(make_replica(
                ReplicaID{2, 0, 0},
                "node-b"))
            .ok());
    ASSERT_TRUE(
        store
            .put_replica(make_replica(
                ReplicaID{3, 0, 0},
                "node-c", ReplicaDesired::ABSENT))
            .ok());

    DefaultNodeSelector selector(&store);
    TablePlacementResult result;
    Status status = selector.select_table_nodes(make_param(), result);
    ASSERT_TRUE(status.ok());

    ASSERT_EQ(result.shards.size(), 1U);
    expect_node_ids(result.shards[0].nodes, {"node-c", "node-b"});
}

// - 检测一下新增 replica 计入后续 shard 的负载
TEST(NodeSelectorTest, BalancesLoadAcrossMultipleShards) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store.put_node(make_node("node-a", "pool-a", 18080)).ok());
    ASSERT_TRUE(store.put_node(make_node("node-b", "pool-a", 18081)).ok());
    ASSERT_TRUE(store.put_node(make_node("node-c", "pool-a", 18082)).ok());

    DefaultNodeSelector selector(&store);
    TablePlacementResult result;
    Status status = selector.select_table_nodes(make_param(3, 2), result);
    ASSERT_TRUE(status.ok());

    ASSERT_EQ(result.shards.size(), 3U);
    EXPECT_EQ(result.shards[0].shard_index, 0);
    EXPECT_EQ(result.shards[1].shard_index, 1);
    EXPECT_EQ(result.shards[2].shard_index, 2);
    expect_node_ids(result.shards[0].nodes, {"node-a", "node-b"});
    expect_node_ids(result.shards[1].nodes, {"node-c", "node-a"});
    expect_node_ids(result.shards[2].nodes, {"node-b", "node-c"});
}

}  // namespace adviskv::sdm