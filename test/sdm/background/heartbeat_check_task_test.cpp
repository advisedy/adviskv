#include <gtest/gtest.h>

#include "common/background_task.h"
#include "common/func.h"
#include "common/type.h"
#include "sdm/model/sdm_store.h"

#define private public
#include "sdm/background/heartbeat_check_task.h"
#undef private

namespace adviskv::sdm {
namespace {

Node make_heartbeat_node(const NodeID& node_id, NodeStatus status,
                         int64_t last_heartbeat_ts, int32_t port) {
    return Node{
        .id = node_id,
        .spec{
            .resource_pool = "pool-a",
            .dc = "dc-a",
            .status = status,
        },
        .state{
            .endpoint = Endpoint{.ip = "127.0.0.1", .port = port},
            .last_heartbeat_ts = last_heartbeat_ts,
        },
    };
}

Replica make_replica(const NodeID& node_id, ReplicaIndex replica_index,
                     ReplicaPhase phase, int32_t port) {
    return Replica{
        .replica_id = ReplicaID{.table_id = 1001,
                                .shard_index = 0,
                                .replica_index = replica_index},
        .spec{
            .dc = "dc-a",
            .assign_node_id = node_id,
            .engine_type = EngineType::MAP,
        },
        .state{
            .desired = ReplicaDesired::PRESENT,
            .phase = phase,
            .observed_endpoint = Endpoint{.ip = "127.0.0.1", .port = port},
        },
    };
}

void expire_startup_grace(HeartBeatCheckTask& task) {
    task.start_ts_ms_ = func::get_current_ts_ms() -
                        HeartBeatCheckTask::STARTUP_GRACE_MS - 1000;
}

}  // namespace

// 直接测试一下当这个NODE从ONLINE到SUSPECT到OFFLINE的过程，
// 以及最后OFFLINE的时候，上面的replica是否状态改成了LOST
TEST(HeartBeatCheckTaskTest,
     OnlineNodeBecomesSuspectThenOfflineAndReplicasLost) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(
        store
            .put_node(make_heartbeat_node(
                "node-a", NodeStatus::ONLINE,
                func::get_current_ts_ms() -
                    HeartBeatCheckTask::SUSPECT_TIMEOUT_MS - 1000,
                18080))
            .ok());
    ASSERT_TRUE(
        store.put_replica(make_replica("node-a", 0, ReplicaPhase::READY, 18080))
            .ok());

    HeartBeatCheckTask task(&store);
    expire_startup_grace(task);

    ASSERT_TRUE(task.prepare().ok());

    NodeOr node;
    ASSERT_TRUE(store.get_node("node-a", node).ok());
    ASSERT_FALSE(node.is_empty());
    EXPECT_EQ(node->spec.status, NodeStatus::SUSPECT);

    ReplicaOr replica;
    ASSERT_TRUE(store.get_replica(ReplicaID{1001, 0, 0}, replica).ok());
    ASSERT_FALSE(replica.is_empty());
    EXPECT_EQ(replica->state.phase, ReplicaPhase::READY);

    node->state.last_heartbeat_ts =
        func::get_current_ts_ms() - HeartBeatCheckTask::OFFLINE_TIMEOUT_MS -
        1000;
    ASSERT_TRUE(store.put_node(*node).ok());
    ASSERT_TRUE(task.prepare().ok());

    ASSERT_TRUE(store.get_node("node-a", node).ok());
    ASSERT_FALSE(node.is_empty());
    EXPECT_EQ(node->spec.status, NodeStatus::OFFLINE);

    ASSERT_TRUE(store.get_replica(ReplicaID{1001, 0, 0}, replica).ok());
    ASSERT_FALSE(replica.is_empty());
    EXPECT_EQ(replica->state.phase, ReplicaPhase::LOST);
}

// 测试下一个SUSPECT的NODE变成ONLINE，
// 然后那些Replica的这个状态也进行了这个更新
TEST(HeartBeatCheckTaskTest, SuspectNodeBecomesOnlineAndReplicasUpdated) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(
        store
            .put_node(make_heartbeat_node("node-a", NodeStatus::SUSPECT,
                                          func::get_current_ts_ms(), 18080))
            .ok());
    ASSERT_TRUE(
        store.put_replica(make_replica("node-a", 0, ReplicaPhase::LOST, 18080))
            .ok());

    HeartBeatCheckTask task(&store);
    expire_startup_grace(task);
    ASSERT_TRUE(task.prepare().ok());

    NodeOr node;
    ASSERT_TRUE(store.get_node("node-a", node).ok());
    ASSERT_FALSE(node.is_empty());
    EXPECT_EQ(node->spec.status, NodeStatus::ONLINE);

    ReplicaOr replica;
    ASSERT_TRUE(store.get_replica(ReplicaID{1001, 0, 0}, replica).ok());
    ASSERT_FALSE(replica.is_empty());
    EXPECT_EQ(replica->state.phase, ReplicaPhase::CREATING);
}

// 测试一下，当所有的node都是online，结果这个SDM崩溃了，然后重新启动。
// 然后在刚开始的时候，这个node他们虽然这个心跳时间超过了
// 这个标记为这个offline的这个时间，但是依旧不会被标记成offline还是online
TEST(HeartBeatCheckTaskTest, StartupGraceKeepsStaleOnlineNodesOnline) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(
        store
            .put_node(make_heartbeat_node(
                "node-a", NodeStatus::ONLINE,
                func::get_current_ts_ms() -
                    HeartBeatCheckTask::OFFLINE_TIMEOUT_MS - 1000,
                18080))
            .ok());
    ASSERT_TRUE(
        store
            .put_node(make_heartbeat_node(
                "node-b", NodeStatus::ONLINE,
                func::get_current_ts_ms() -
                    HeartBeatCheckTask::OFFLINE_TIMEOUT_MS - 1000,
                18081))
            .ok());
    ASSERT_TRUE(
        store.put_replica(make_replica("node-a", 0, ReplicaPhase::READY, 18080))
            .ok());
    ASSERT_TRUE(
        store.put_replica(make_replica("node-b", 1, ReplicaPhase::READY, 18081))
            .ok());

    HeartBeatCheckTask task(&store);
    ASSERT_TRUE(task.prepare().ok());

    NodeOr node_a;
    ASSERT_TRUE(store.get_node("node-a", node_a).ok());
    ASSERT_FALSE(node_a.is_empty());
    EXPECT_EQ(node_a->spec.status, NodeStatus::ONLINE);

    NodeOr node_b;
    ASSERT_TRUE(store.get_node("node-b", node_b).ok());
    ASSERT_FALSE(node_b.is_empty());
    EXPECT_EQ(node_b->spec.status, NodeStatus::ONLINE);

    ReplicaOr replica;
    ASSERT_TRUE(store.get_replica(ReplicaID{1001, 0, 0}, replica).ok());
    ASSERT_FALSE(replica.is_empty());
    EXPECT_EQ(replica->state.phase, ReplicaPhase::READY);

    ASSERT_TRUE(store.get_replica(ReplicaID{1001, 0, 1}, replica).ok());
    ASSERT_FALSE(replica.is_empty());
    EXPECT_EQ(replica->state.phase, ReplicaPhase::READY);
}

// 还是上述的这个情况，SDM崩溃重新启动之后，然后之前node是offline的这个那些node，
// 然后它们在这个限制的这个时间里面，然后发出的这个心跳，但是它们是会更改的，
// 状态是可以进行更新的，检测这个
TEST(HeartBeatCheckTaskTest,
     StartupGraceAllowsOfflineNodesWithNewHeartbeatRecover) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(
        store
            .put_node(make_heartbeat_node("node-a", NodeStatus::OFFLINE,
                                          func::get_current_ts_ms(), 18080))
            .ok());
    ASSERT_TRUE(
        store
            .put_node(make_heartbeat_node("node-b", NodeStatus::OFFLINE,
                                          func::get_current_ts_ms(), 18081))
            .ok());
    ASSERT_TRUE(
        store
            .put_node(make_heartbeat_node("node-c", NodeStatus::SUSPECT,
                                          func::get_current_ts_ms(), 18082))
            .ok());
    ASSERT_TRUE(
        store.put_replica(make_replica("node-a", 0, ReplicaPhase::LOST, 18080))
            .ok());
    ASSERT_TRUE(
        store.put_replica(make_replica("node-b", 1, ReplicaPhase::LOST, 18081))
            .ok());
    ASSERT_TRUE(
        store.put_replica(make_replica("node-c", 2, ReplicaPhase::READY, 18082))
            .ok());

    HeartBeatCheckTask task(&store);
    ASSERT_TRUE(task.prepare().ok());

    NodeOr node_a;
    ASSERT_TRUE(store.get_node("node-a", node_a).ok());
    ASSERT_FALSE(node_a.is_empty());
    EXPECT_EQ(node_a->spec.status, NodeStatus::ONLINE);

    NodeOr node_b;
    ASSERT_TRUE(store.get_node("node-b", node_b).ok());
    ASSERT_FALSE(node_b.is_empty());
    EXPECT_EQ(node_b->spec.status, NodeStatus::ONLINE);

    NodeOr node_c;
    ASSERT_TRUE(store.get_node("node-c", node_c).ok());
    ASSERT_FALSE(node_c.is_empty());
    EXPECT_EQ(node_c->spec.status, NodeStatus::ONLINE);

    ReplicaOr replica;
    ASSERT_TRUE(store.get_replica(ReplicaID{1001, 0, 0}, replica).ok());
    ASSERT_FALSE(replica.is_empty());
    EXPECT_EQ(replica->state.phase, ReplicaPhase::CREATING);

    ASSERT_TRUE(store.get_replica(ReplicaID{1001, 0, 1}, replica).ok());
    ASSERT_FALSE(replica.is_empty());
    EXPECT_EQ(replica->state.phase, ReplicaPhase::CREATING);

    ASSERT_TRUE(store.get_replica(ReplicaID{1001, 0, 2}, replica).ok());
    ASSERT_FALSE(replica.is_empty());
    EXPECT_EQ(replica->state.phase, ReplicaPhase::READY);
}

}  // namespace adviskv::sdm