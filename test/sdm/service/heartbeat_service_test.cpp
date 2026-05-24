#include "sdm/service/heartbeat_service.h"

#include <gtest/gtest.h>

#include "common/status.h"
#include "sdm/model/sdm_store.h"

namespace adviskv::sdm {
namespace {

Node make_node(const NodeID& id, int32_t port = 18080) {
    return Node{
        .id = id,
        .spec{
            .resource_pool = "pool-a",
            .dc = "dc-a",
            .status = NodeStatus::ONLINE,
        },
        .state{
            .endpoint = Endpoint{.ip = "127.0.0.1", .port = port},
            .last_heartbeat_ts = 1,
        },
    };
}

Replica make_replica(const ReplicaID& replica_id, const NodeID& node_id) {
    return Replica{
        .replica_id = replica_id,
        .spec{
            .dc = "dc-a",
            .assign_node_id = node_id,
            .engine_type = EngineType::MAP,
        },
        .state{
            .desired = ReplicaDesired::PRESENT,
            .phase = ReplicaPhase::CREATING,
            .observed_role = ReplicaRole::FOLLOWER,
            .observed_endpoint = Endpoint{.ip = "127.0.0.1", .port = 18080},
            .term = 1,
        },
    };
}

HeartBeatParam make_heartbeat_param() {
    return HeartBeatParam{
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
                    .term = 7,
                },
            },
        .last_heartbeat_ts = 987654,
    };
}

}  // namespace

// 检测 heartbeat 会更新节点心跳信息，并同步更新本节点上的 replica 状态。
TEST(HeartBeatServiceTest, HeartbeatUpdatesNodeAndAssignedReplicas) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store.put_node(make_node("node-a")).ok());
    ASSERT_TRUE(
        store.put_replica(make_replica(ReplicaID{1001, 0, 0}, "node-a")).ok());
    HeartBeatService service(&store);

    Status status = service.heartbeat(make_heartbeat_param());

    ASSERT_TRUE(status.ok()) << status.msg();
    NodeOr node;
    ASSERT_TRUE(store.get_node("node-a", node).ok());
    ASSERT_FALSE(node.is_empty());
    EXPECT_EQ(node->state.endpoint.ip, "10.0.0.1");
    EXPECT_EQ(node->state.endpoint.port, 19090);
    EXPECT_EQ(node->state.last_heartbeat_ts, 987654);

    ReplicaOr replica;
    ASSERT_TRUE(store.get_replica(ReplicaID{1001, 0, 0}, replica).ok());
    ASSERT_FALSE(replica.is_empty());
    EXPECT_EQ(replica->state.phase, ReplicaPhase::READY);
    EXPECT_EQ(replica->state.observed_role, ReplicaRole::LEADER);
    EXPECT_EQ(replica->state.observed_endpoint.ip, "10.0.0.1");
    EXPECT_EQ(replica->state.observed_endpoint.port, 19090);
    EXPECT_EQ(replica->state.term, 7);
}

// 检测 heartbeat 会忽略不存在的 replica 和不属于该节点的 replica。
TEST(HeartBeatServiceTest, HeartbeatIgnoresMissingAndOtherNodeReplicas) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store.put_node(make_node("node-a")).ok());
    ASSERT_TRUE(
        store.put_replica(make_replica(ReplicaID{1001, 0, 1}, "node-b")).ok());
    HeartBeatService service(&store);

    HeartBeatParam param = make_heartbeat_param();
    param.replica_list.push_back(HeartBeatReplicaInfo{
        .shard_id = ShardID{.table_id = 1001, .shard_index = 0},
        .replica_index = 1,
        .role = ReplicaRole::LEADER,
        .status = ReplicaStatus::READY,
        .term = 9,
    });
    param.replica_list.push_back(HeartBeatReplicaInfo{
        .shard_id = ShardID{.table_id = 9999, .shard_index = 0},
        .replica_index = 0,
        .role = ReplicaRole::LEADER,
        .status = ReplicaStatus::READY,
        .term = 9,
    });

    Status status = service.heartbeat(param);

    ASSERT_TRUE(status.ok()) << status.msg();
    ReplicaOr other;
    ASSERT_TRUE(store.get_replica(ReplicaID{1001, 0, 1}, other).ok());
    ASSERT_FALSE(other.is_empty());
    EXPECT_EQ(other->state.phase, ReplicaPhase::CREATING);
    EXPECT_EQ(other->state.observed_role, ReplicaRole::FOLLOWER);
    EXPECT_EQ(other->state.term, 1);
}

// 检测非法 heartbeat 参数会被拒绝。
TEST(HeartBeatServiceTest, HeartbeatRejectsInvalidParam) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    HeartBeatService service(&store);
    HeartBeatParam param = make_heartbeat_param();
    param.node_id.clear();

    Status status = service.heartbeat(param);

    EXPECT_EQ(status.code(), StatusCode::INVALID_ARGUMENT);
}

// 检测节点不存在时，heartbeat 返回错误且不会创建新节点。
TEST(HeartBeatServiceTest, HeartbeatReturnsErrorWhenNodeMissing) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    HeartBeatService service(&store);

    Status status = service.heartbeat(make_heartbeat_param());

    EXPECT_TRUE(status.fail());
    NodeOr node;
    ASSERT_TRUE(store.get_node("node-a", node).ok());
    EXPECT_TRUE(node.is_empty());
}

}  // namespace adviskv::sdm