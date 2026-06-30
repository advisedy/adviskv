#include "sdm/service/node_service.h"

#include <gtest/gtest.h>
#include <memory>

#include "common/model/storage_replica_status.h"
#include "common/status.h"
#include "sdm/model/sdm_store.h"
#include "sdm/service/replica_group_service.h"
#include "sdm/sdm_store_test_helper.h"

namespace adviskv::sdm {
namespace {

Node make_node(const NodeID& id, int32_t port = 18080) {
    return Node{id, NodeMeta{"pool-a", "dc-a"},
                NodeState{NodeStatus::ONLINE, Endpoint{"127.0.0.1", port}, 1},
                NodeDerived{}};
}

Replica make_replica(const ReplicaID& replica_id, const NodeID& node_id) {
    ReplicaState state{};
    state.desired = ReplicaDesired::PRESENT;
    state.phase = ReplicaPhase::CREATING;
    state.observed_raft_role = ReplicaRole::FOLLOWER;
    state.observed_member_type = RaftMemberType::NON_MEMBER;
    state.observed_endpoint = Endpoint{"127.0.0.1", 18080};
    state.term = 1;
    return Replica{replica_id,
                   ReplicaSpec{"dc-a", node_id, EngineType::MAP}, state};
}

HeartBeatParam make_heartbeat_param() {
    return HeartBeatParam{
        "node-a",
        "10.0.0.1",
        19090,
        "pool-a",
        "dc-a",
        {HeartBeatReplicaInfo{ReplicaID{1001, 0, 0}, ReplicaRole::LEADER,
                              StorageReplicaStatus::READY, 7,
                              RaftMemberType::VOTER}},
        987654,
    };
}

}  // namespace

// 检测 heartbeat 会更新节点心跳信息，并同步更新本节点上的 replica 状态。
TEST(NodeHeartbeatServiceTest, HeartbeatUpdatesNodeAndAssignedReplicas) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store_test::put_node(store, make_node("node-a")).ok());
    ASSERT_TRUE(store_test::put_replica(
                    store, make_replica(ReplicaID{1001, 0, 0}, "node-a"))
                    .ok());
    NodeService service(&store);
    ReplicaGroupService replica_group_service(&store);

    Status status = service.heartbeat(make_heartbeat_param());

    ASSERT_TRUE(status.ok()) << status.msg();
    ASSERT_TRUE(replica_group_service.reconcile_all().ok());
    NodeOr node;
    ASSERT_TRUE(store_test::get_node(store, "node-a", node).ok());
    ASSERT_FALSE(node.is_empty());
    EXPECT_EQ(node->state.endpoint.ip, "10.0.0.1");
    EXPECT_EQ(node->state.endpoint.port, 19090);
    EXPECT_EQ(node->state.last_heartbeat_ts, 987654);

    ReplicaOr replica;
    ASSERT_TRUE(
        store_test::get_replica(store, ReplicaID{1001, 0, 0}, replica).ok());
    ASSERT_FALSE(replica.is_empty());
    EXPECT_EQ(replica->state.phase, ReplicaPhase::READY);
    EXPECT_EQ(replica->state.observed_raft_role, ReplicaRole::LEADER);
    EXPECT_EQ(replica->state.observed_member_type, RaftMemberType::VOTER);
    EXPECT_EQ(replica->state.observed_endpoint.ip, "10.0.0.1");
    EXPECT_EQ(replica->state.observed_endpoint.port, 19090);
    EXPECT_EQ(replica->state.term, 7);
}

// 检测 heartbeat 会忽略不存在的 replica 和不属于该节点的 replica。
TEST(NodeHeartbeatServiceTest, HeartbeatIgnoresMissingAndOtherNodeReplicas) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store_test::put_node(store, make_node("node-a")).ok());
    ASSERT_TRUE(store_test::put_replica(
                    store, make_replica(ReplicaID{1001, 0, 1}, "node-b"))
                    .ok());
    NodeService service(&store);

    HeartBeatParam param = make_heartbeat_param();
    param.replica_list.push_back(HeartBeatReplicaInfo{
        ReplicaID{1001, 0, 1},
        ReplicaRole::LEADER,
        StorageReplicaStatus::READY,
        9,
        RaftMemberType::VOTER,
    });
    param.replica_list.push_back(HeartBeatReplicaInfo{
        ReplicaID{9999, 0, 0},
        ReplicaRole::LEADER,
        StorageReplicaStatus::READY,
        9,
        RaftMemberType::VOTER,
    });

    Status status = service.heartbeat(param);

    ASSERT_TRUE(status.ok()) << status.msg();
    ReplicaOr other;
    ASSERT_TRUE(
        store_test::get_replica(store, ReplicaID{1001, 0, 1}, other).ok());
    ASSERT_FALSE(other.is_empty());
    EXPECT_EQ(other->state.phase, ReplicaPhase::CREATING);
    EXPECT_EQ(other->state.observed_raft_role, ReplicaRole::FOLLOWER);
    EXPECT_EQ(other->state.term, 1);
}

// 检测心跳没上报 DELETING 状态的 replica 时，会记录 no-exist 观测，
// ReplicaGroupService 再将其推进为 DELETED。
TEST(NodeHeartbeatServiceTest, HeartbeatMarksDeletingReplicaDeletedWhenMissing) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store_test::put_node(store, make_node("node-a")).ok());

    Replica replica = make_replica(ReplicaID{1001, 0, 0}, "node-a");
    replica.state.desired = ReplicaDesired::ABSENT;
    replica.state.phase = ReplicaPhase::DELETING;
    ASSERT_TRUE(store_test::put_replica(store, replica).ok());

    NodeService service(&store);
    HeartBeatParam param = make_heartbeat_param();
    param.replica_list.clear();

    Status status = service.heartbeat(param);

    ASSERT_TRUE(status.ok()) << status.msg();
    ReplicaOr stored;
    ASSERT_TRUE(
        store_test::get_replica(store, ReplicaID{1001, 0, 0}, stored).ok());
    ASSERT_FALSE(stored.is_empty());
    EXPECT_EQ(stored->state.desired, ReplicaDesired::ABSENT);
    EXPECT_EQ(stored->state.phase, ReplicaPhase::DELETING);
    EXPECT_TRUE(stored->state.observed_no_exist);

    ReplicaGroupService replica_group_service(&store);
    ASSERT_TRUE(replica_group_service.reconcile_all().ok());
    ASSERT_TRUE(
        store_test::get_replica(store, ReplicaID{1001, 0, 0}, stored).ok());
    ASSERT_FALSE(stored.is_empty());
    EXPECT_EQ(stored->state.phase, ReplicaPhase::DELETED);
}

// 检测 ReplicaGroupService 能正确构建心跳响应：在 desired_members 中的 replica 发 PRESENT，不在的报 ABSENT。
TEST(NodeHeartbeatServiceTest, ReplicaGroupServiceBuildsHeartbeatExpectations) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store_test::put_node(store, make_node("node-a")).ok());

    Replica replica = make_replica(ReplicaID{1001, 0, 0}, "node-a");
    ASSERT_TRUE(store_test::put_replica(store, replica).ok());

    ReplicaGroup group;
    group.shard_id = ShardID{1001, 0};
    group.mode = ReplicaGroupMode::BOOTSTRAP;
    group.target_replica_count = 1;
    group.desired_members = {replica.replica_id};
    ASSERT_TRUE(store_test::put_replica_group(store, group).ok());

    std::vector<PeerMember> expected_members = {
        PeerMember{"node-a", replica.replica_id, Endpoint{"127.0.0.1", 18080}},
    };

    HeartBeatParam param = make_heartbeat_param();
    param.replica_list.clear();
    param.replica_list.push_back(HeartBeatReplicaInfo{
        ReplicaID{9999, 0, 0},
        ReplicaRole::FOLLOWER,
        StorageReplicaStatus::READY,
        1,
        RaftMemberType::VOTER,
    });

    ReplicaGroupService replica_group_service(&store);
    HeartBeatResult result;
    Status status =
        replica_group_service.build_heartbeat_result(param, result);

    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(result.expects.size(), 2U);

    const ExpectedReplica* present = nullptr;
    const ExpectedReplica* absent = nullptr;
    for (const ExpectedReplica& expect : result.expects) {
        if (expect.replica_id == replica.replica_id) {
            present = &expect;
        }
        if (expect.replica_id == ReplicaID{9999, 0, 0}) {
            absent = &expect;
        }
    }
    ASSERT_NE(present, nullptr);
    EXPECT_EQ(present->type, ExpectedReplicaType::PRESENT);
    EXPECT_EQ(present->engine_type, EngineType::MAP);
    EXPECT_EQ(present->initial_members, expected_members);

    ASSERT_NE(absent, nullptr);
    EXPECT_EQ(absent->type, ExpectedReplicaType::ABSENT);

    result.expects.clear();
    param = make_heartbeat_param();
    status = replica_group_service.build_heartbeat_result(param, result);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_TRUE(result.expects.empty());
}

// 检测非法 heartbeat 参数会被拒绝。
TEST(NodeHeartbeatServiceTest, HeartbeatRejectsInvalidParam) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    NodeService service(&store);
    HeartBeatParam param = make_heartbeat_param();
    param.node_id.clear();

    Status status = service.heartbeat(param);

    EXPECT_EQ(status.code(), StatusCode::INVALID_ARGUMENT);
}

// 检测节点不存在时，heartbeat 返回错误且不会创建新节点。
TEST(NodeHeartbeatServiceTest, HeartbeatReturnsErrorWhenNodeMissing) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    NodeService service(&store);

    Status status = service.heartbeat(make_heartbeat_param());

    EXPECT_TRUE(status.fail());
    NodeOr node;
    ASSERT_TRUE(store_test::get_node(store, "node-a", node).ok());
    EXPECT_TRUE(node.is_empty());
}

}  // namespace adviskv::sdm
