#include "sdm/service/node_service.h"

#include <gtest/gtest.h>

#include "common/func.h"
#include "common/type.h"
#include "sdm/service/replica_group_service.h"
#include "sdm/model/sdm_store.h"
#include "sdm/sdm_store_test_helper.h"

namespace adviskv::sdm {
namespace {

Node make_heartbeat_node(const NodeID& node_id, NodeStatus status,
                         int64_t last_heartbeat_ts, int32_t port) {
    return Node{node_id, NodeMeta{"pool-a", "dc-a"},
                NodeState{status, Endpoint{"127.0.0.1", port},
                          last_heartbeat_ts},
                NodeDerived{}};
}

Replica make_replica(const NodeID& node_id, ReplicaIndex replica_index,
                     ReplicaPhase phase, int32_t port) {
    ReplicaState state{};
    state.desired = ReplicaDesired::PRESENT;
    state.phase = phase;
    state.observed_raft_role = ReplicaRole::FOLLOWER;
    state.observed_endpoint = Endpoint{"127.0.0.1", port};
    state.observed_storage_status =
        phase == ReplicaPhase::READY ? StorageReplicaStatus::READY
                                     : StorageReplicaStatus::INITIALIZING;
    return Replica{ReplicaID{1001, 0, replica_index},
                   ReplicaSpec{"dc-a", node_id, EngineType::MAP}, state};
}

Table make_table() {
    TableState state{};
    state.desired = TableDesired::PRESENT;
    state.phase = TablePhase::CREATING;
    return Table{1001,
                 TableSpec{"orders", 11, "commerce", 1, 1, "pool-a",
                           "create-1001"},
                 state};
}

void expire_startup_grace(NodeService& ctrl) {
    ctrl.set_start_ts_ms_for_test(func::get_current_ts_ms() -
                                  NodeService::HEARTBEAT_STARTUP_GRACE_MS - 1000);
}

void run_node_and_replica_reconcile(NodeService& node_ctrl,
                                    ReplicaGroupService& rg_ctrl) {
    ASSERT_TRUE(node_ctrl.reconcile_all().ok());
    ASSERT_TRUE(rg_ctrl.reconcile_all().ok());
}

}  // namespace

// 检测节点不存在时，reconcile 不会错误地将 replica 标为 LOST。
TEST(NodeServiceReconcileTest, MissingNodeDoesNotMarkReplicaLost) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store.init().ok());
    ASSERT_TRUE(store_test::put_table(store, make_table()).ok());
    ASSERT_TRUE(
        store_test::put_replica(
            store, make_replica("node-a", 0, ReplicaPhase::READY, 18080))
            .ok());

    ReplicaGroupService rg_ctrl(&store);
    ASSERT_TRUE(rg_ctrl.reconcile_all().ok());

    ReplicaOr replica;
    ASSERT_TRUE(
        store_test::get_replica(store, ReplicaID{1001, 0, 0}, replica).ok());
    ASSERT_FALSE(replica.is_empty());
    EXPECT_EQ(replica->state.phase, ReplicaPhase::READY);
}

// 检测节点从 ONLINE 到 SUSPECT 到 OFFLINE 的完整过程，以及 OFFLINE 时其上的 replica 是否被标为 LOST。
TEST(NodeServiceReconcileTest, OnlineNodeBecomesSuspectThenOfflineAndReplicasLost) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store.init().ok());
    ASSERT_TRUE(
        store_test::put_node(
            store, make_heartbeat_node(
                       "node-a", NodeStatus::ONLINE,
                       func::get_current_ts_ms() -
                           NodeService::HEARTBEAT_SUSPECT_TIMEOUT_MS - 1000,
                       18080))
            .ok());
    ASSERT_TRUE(
        store_test::put_replica(
            store, make_replica("node-a", 0, ReplicaPhase::READY, 18080))
            .ok());

    NodeService ctrl(&store);
    ReplicaGroupService rg_ctrl(&store);
    expire_startup_grace(ctrl);

    run_node_and_replica_reconcile(ctrl, rg_ctrl);

    NodeOr node;
    ASSERT_TRUE(store_test::get_node(store, "node-a", node).ok());
    ASSERT_FALSE(node.is_empty());
    EXPECT_EQ(node->state.status, NodeStatus::SUSPECT);

    ReplicaOr replica;
    ASSERT_TRUE(
        store_test::get_replica(store, ReplicaID{1001, 0, 0}, replica).ok());
    ASSERT_FALSE(replica.is_empty());
    EXPECT_EQ(replica->state.phase, ReplicaPhase::READY);

    node->state.last_heartbeat_ts = func::get_current_ts_ms() -
                                    NodeService::HEARTBEAT_OFFLINE_TIMEOUT_MS - 1000;
    ASSERT_TRUE(store_test::put_node(store, *node).ok());
    run_node_and_replica_reconcile(ctrl, rg_ctrl);

    ASSERT_TRUE(store_test::get_node(store, "node-a", node).ok());
    ASSERT_FALSE(node.is_empty());
    EXPECT_EQ(node->state.status, NodeStatus::OFFLINE);

    ASSERT_TRUE(
        store_test::get_replica(store, ReplicaID{1001, 0, 0}, replica).ok());
    ASSERT_FALSE(replica.is_empty());
    EXPECT_EQ(replica->state.phase, ReplicaPhase::LOST);
}

// 检测 SUSPECT 节点恢复心跳后变回 ONLINE，其上的 LOST replica 被标回 CREATING。
TEST(NodeServiceReconcileTest, SuspectNodeBecomesOnlineAndReplicasUpdated) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store.init().ok());
    ASSERT_TRUE(
        store_test::put_node(
            store, make_heartbeat_node("node-a", NodeStatus::SUSPECT,
                                       func::get_current_ts_ms(), 18080))
            .ok());
    ASSERT_TRUE(store_test::put_replica(
                    store, make_replica("node-a", 0, ReplicaPhase::LOST, 18080))
                    .ok());

    NodeService ctrl(&store);
    ReplicaGroupService rg_ctrl(&store);
    expire_startup_grace(ctrl);
    run_node_and_replica_reconcile(ctrl, rg_ctrl);

    NodeOr node;
    ASSERT_TRUE(store_test::get_node(store, "node-a", node).ok());
    ASSERT_FALSE(node.is_empty());
    EXPECT_EQ(node->state.status, NodeStatus::ONLINE);

    ReplicaOr replica;
    ASSERT_TRUE(
        store_test::get_replica(store, ReplicaID{1001, 0, 0}, replica).ok());
    ASSERT_FALSE(replica.is_empty());
    EXPECT_EQ(replica->state.phase, ReplicaPhase::CREATING);
}

// 检测 OFFLINE 节点收到新心跳后变回 ONLINE。
TEST(NodeServiceReconcileTest, OfflineNodeWithFreshHeartbeatBecomesOnline) {
    Node node;
    node.id = "storage-1";
    node.state.status = NodeStatus::OFFLINE;
    node.state.last_heartbeat_ts = func::get_current_ts_ms();

    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store.init().ok());
    ASSERT_EQ(store_test::put_node(store, node).ok(), true);
    NodeService ctrl(&store);
    ASSERT_TRUE(ctrl.check_and_modify_node_for_test(node).ok());

    NodeOr saved;
    ASSERT_TRUE(store_test::get_node(store, "storage-1", saved).ok());
    ASSERT_FALSE(saved.is_empty());
    EXPECT_EQ(saved->state.status, NodeStatus::ONLINE);
}

// 检测 reconcile 时会重新读取最新的节点状态，避免用过期数据覆盖。
TEST(NodeServiceReconcileTest, CheckReloadsNodeBeforeWritingStatus) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store.init().ok());
    Node stale = make_heartbeat_node(
        "node-a", NodeStatus::ONLINE,
        func::get_current_ts_ms() - NodeService::HEARTBEAT_OFFLINE_TIMEOUT_MS - 1000,
        18080);
    ASSERT_TRUE(store_test::put_node(store, stale).ok());
    ASSERT_TRUE(
        store_test::put_replica(
            store, make_replica("node-a", 0, ReplicaPhase::READY, 18080))
            .ok());

    Node fresh = stale;
    fresh.state.last_heartbeat_ts = func::get_current_ts_ms();
    ASSERT_TRUE(store_test::put_node(store, fresh).ok());

    NodeService ctrl(&store);
    ReplicaGroupService rg_ctrl(&store);
    expire_startup_grace(ctrl);
    ASSERT_TRUE(ctrl.check_and_modify_node_for_test(stale).ok());

    NodeOr saved;
    ASSERT_TRUE(store_test::get_node(store, "node-a", saved).ok());
    ASSERT_FALSE(saved.is_empty());
    EXPECT_EQ(saved->state.status, NodeStatus::ONLINE);
    EXPECT_EQ(saved->state.last_heartbeat_ts, fresh.state.last_heartbeat_ts);

    ReplicaOr replica;
    ASSERT_TRUE(
        store_test::get_replica(store, ReplicaID{1001, 0, 0}, replica).ok());
    ASSERT_FALSE(replica.is_empty());
    EXPECT_EQ(replica->state.phase, ReplicaPhase::READY);
}

// 检测启动宽限期内，心跳过期的节点不会被标为 SUSPECT/OFFLINE。
TEST(NodeServiceReconcileTest, StartupGraceKeepsStaleOnlineNodesOnline) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store.init().ok());
    ASSERT_TRUE(
        store_test::put_node(
            store, make_heartbeat_node(
                       "node-a", NodeStatus::ONLINE,
                       func::get_current_ts_ms() -
                           NodeService::HEARTBEAT_OFFLINE_TIMEOUT_MS - 1000,
                       18080))
            .ok());
    ASSERT_TRUE(
        store_test::put_node(
            store, make_heartbeat_node(
                       "node-b", NodeStatus::ONLINE,
                       func::get_current_ts_ms() -
                           NodeService::HEARTBEAT_OFFLINE_TIMEOUT_MS - 1000,
                       18081))
            .ok());
    ASSERT_TRUE(
        store_test::put_replica(
            store, make_replica("node-a", 0, ReplicaPhase::READY, 18080))
            .ok());
    ASSERT_TRUE(
        store_test::put_replica(
            store, make_replica("node-b", 1, ReplicaPhase::READY, 18081))
            .ok());

    NodeService ctrl(&store);
    ReplicaGroupService rg_ctrl(&store);
    ASSERT_TRUE(ctrl.reconcile_all().ok());
    ASSERT_TRUE(rg_ctrl.reconcile_all().ok());

    NodeOr node_a;
    ASSERT_TRUE(store_test::get_node(store, "node-a", node_a).ok());
    ASSERT_FALSE(node_a.is_empty());
    EXPECT_EQ(node_a->state.status, NodeStatus::ONLINE);

    NodeOr node_b;
    ASSERT_TRUE(store_test::get_node(store, "node-b", node_b).ok());
    ASSERT_FALSE(node_b.is_empty());
    EXPECT_EQ(node_b->state.status, NodeStatus::ONLINE);

    ReplicaOr replica;
    ASSERT_TRUE(
        store_test::get_replica(store, ReplicaID{1001, 0, 0}, replica).ok());
    ASSERT_FALSE(replica.is_empty());
    EXPECT_EQ(replica->state.phase, ReplicaPhase::READY);

    ASSERT_TRUE(
        store_test::get_replica(store, ReplicaID{1001, 0, 1}, replica).ok());
    ASSERT_FALSE(replica.is_empty());
    EXPECT_EQ(replica->state.phase, ReplicaPhase::READY);
}

// 检测启动宽限期内，OFFLINE/SUSPECT 节点收到新心跳后可以恢复为 ONLINE。
TEST(NodeServiceReconcileTest, StartupGraceAllowsOfflineNodesWithNewHeartbeatRecover) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store.init().ok());
    ASSERT_TRUE(
        store_test::put_node(
            store, make_heartbeat_node("node-a", NodeStatus::OFFLINE,
                                       func::get_current_ts_ms(), 18080))
            .ok());
    ASSERT_TRUE(
        store_test::put_node(
            store, make_heartbeat_node("node-b", NodeStatus::OFFLINE,
                                       func::get_current_ts_ms(), 18081))
            .ok());
    ASSERT_TRUE(
        store_test::put_node(
            store, make_heartbeat_node("node-c", NodeStatus::SUSPECT,
                                       func::get_current_ts_ms(), 18082))
            .ok());
    ASSERT_TRUE(store_test::put_replica(
                    store, make_replica("node-a", 0, ReplicaPhase::LOST, 18080))
                    .ok());
    ASSERT_TRUE(store_test::put_replica(
                    store, make_replica("node-b", 1, ReplicaPhase::LOST, 18081))
                    .ok());
    ASSERT_TRUE(
        store_test::put_replica(
            store, make_replica("node-c", 2, ReplicaPhase::READY, 18082))
            .ok());

    NodeService ctrl(&store);
    ReplicaGroupService rg_ctrl(&store);
    run_node_and_replica_reconcile(ctrl, rg_ctrl);

    NodeOr node_a;
    ASSERT_TRUE(store_test::get_node(store, "node-a", node_a).ok());
    ASSERT_FALSE(node_a.is_empty());
    EXPECT_EQ(node_a->state.status, NodeStatus::ONLINE);

    NodeOr node_b;
    ASSERT_TRUE(store_test::get_node(store, "node-b", node_b).ok());
    ASSERT_FALSE(node_b.is_empty());
    EXPECT_EQ(node_b->state.status, NodeStatus::ONLINE);

    NodeOr node_c;
    ASSERT_TRUE(store_test::get_node(store, "node-c", node_c).ok());
    ASSERT_FALSE(node_c.is_empty());
    EXPECT_EQ(node_c->state.status, NodeStatus::ONLINE);

    ReplicaOr replica;
    ASSERT_TRUE(
        store_test::get_replica(store, ReplicaID{1001, 0, 0}, replica).ok());
    ASSERT_FALSE(replica.is_empty());
    EXPECT_EQ(replica->state.phase, ReplicaPhase::CREATING);

    ASSERT_TRUE(
        store_test::get_replica(store, ReplicaID{1001, 0, 1}, replica).ok());
    ASSERT_FALSE(replica.is_empty());
    EXPECT_EQ(replica->state.phase, ReplicaPhase::CREATING);

    ASSERT_TRUE(
        store_test::get_replica(store, ReplicaID{1001, 0, 2}, replica).ok());
    ASSERT_FALSE(replica.is_empty());
    EXPECT_EQ(replica->state.phase, ReplicaPhase::READY);
}

}  // namespace adviskv::sdm
