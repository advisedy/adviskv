#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "common/model/model.h"
#include "common/status.h"
#include "meta/service/node_service.h"
#include "meta/service/replica_group_service.h"
#include "meta/store/meta_store.h"
#include "test/meta/store/meta_store_test_helper.h"

namespace adviskv::meta {
namespace {

Node make_node(const NodeID& id, int32_t port = 18080) {
    return Node{id, NodeMeta{"pool-a", "dc-a"}, NodeState{NodeStatus::ONLINE, Endpoint{"127.0.0.1", port}, 1}};
}

Replica make_replica(const ReplicaID& replica_id, const NodeID& node_id) {
    ReplicaState state{};
    state.phase = ReplicaPhase::CREATING;
    return Replica{replica_id, ReplicaSpec{"dc-a", node_id, EngineType::MAP}, state};
}

// 观测态与期望态分离后，测试里显式给 replica 铺一条已有的观测记录，
// 对应线上"storage 心跳过至少一次"的初始状态。
ReplicaObserved make_observed(const ReplicaID& /*replica_id*/) {
    ReplicaObserved observed{};
    observed.raft_role = ReplicaRole::FOLLOWER;
    observed.member_type = RaftMemberType::NON_MEMBER;
    observed.endpoint = Endpoint{"127.0.0.1", 18080};
    observed.storage_status = StorageReplicaStatus::INITIALIZING;
    observed.term = 1;
    return observed;
}

HeartBeatParam make_heartbeat_param() {
    return HeartBeatParam{
            "node-a",
            "10.0.0.1",
            19090,
            "pool-a",
            "dc-a",
            {HeartBeatReplicaInfo{ReplicaID{1001, 0, 0},
                                  ReplicaRole::LEADER,
                                  StorageReplicaStatus::READY,
                                  7,
                                  {}}},
            987654,
    };
}

RaftMember make_heartbeat_member(const ReplicaID& rid, const NodeID& node_id, RaftMemberType member_type) {
    return RaftMember{
            PeerMember{node_id, rid, Endpoint{"127.0.0.1", 18080 + rid.replica_seq}},
            member_type,
    };
}

void put_group_with_replicas(MetaStore& store, const std::vector<ReplicaID>& rids) {
    ReplicaGroup group;
    group.shard_id = ShardID{1001, 0};
    group.mode = ReplicaGroupMode::RAFT_RECONFIG;
    group.target_replica_count = static_cast<int32_t>(rids.size());
    group.desired_members = rids;
    ASSERT_TRUE(store_test::put_replica_group(store, group).ok());
    for (size_t i = 0; i < rids.size(); ++i) {
        const NodeID node_id = "node-" + std::to_string(i);
        ASSERT_TRUE(store_test::put_node(store, make_node(node_id, 18080 + i)).ok());
        ASSERT_TRUE(store_test::put_replica(store, make_replica(rids[i], node_id)).ok());
        ASSERT_TRUE(store_test::put_replica_observed(store, rids[i], make_observed(rids[i])).ok());
    }
}

}  // namespace

// 检测 heartbeat 会更新节点心跳信息，并同步更新本节点上的 replica 状态。
TEST(NodeHeartbeatServiceTest, HeartbeatUpdatesNodeAndAssignedReplicas) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    ASSERT_TRUE(store_test::put_node(store, make_node("node-a")).ok());
    ASSERT_TRUE(store_test::put_replica(store, make_replica(ReplicaID{1001, 0, 0}, "node-a")).ok());
    NodeService service(&store);
    ReplicaGroupService replica_group_service(&store, nullptr);

    Status status = service.heartbeat(make_heartbeat_param());

    ASSERT_TRUE(status.ok()) << status.msg();
    ASSERT_TRUE(replica_group_service.reconcile_all().ok());
    NodeOr node;
    ASSERT_TRUE(store_test::get_node(store, "node-a", node).ok());
    ASSERT_FALSE(node.is_empty());
    EXPECT_EQ(node->state.endpoint.ip, "10.0.0.1");
    EXPECT_EQ(node->state.endpoint.port, 19090);
    EXPECT_GT(node->state.last_heartbeat_ts, 0);

    // 心跳只更新观测记录（期望态的 phase 推进交给 reconciler）。
    ReplicaOr replica;
    ASSERT_TRUE(store_test::get_replica(store, ReplicaID{1001, 0, 0}, replica).ok());
    ASSERT_FALSE(replica.is_empty());
    EXPECT_EQ(replica->state.phase, ReplicaPhase::READY);

    ReplicaObservedOr observed;
    ASSERT_TRUE(store_test::get_replica_observed(store, ReplicaID{1001, 0, 0}, observed).ok());
    ASSERT_FALSE(observed.is_empty());
    EXPECT_EQ(observed->raft_role, ReplicaRole::LEADER);
    EXPECT_EQ(observed->member_type, RaftMemberType::NON_MEMBER);
    EXPECT_EQ(observed->endpoint.ip, "10.0.0.1");
    EXPECT_EQ(observed->endpoint.port, 19090);
    EXPECT_EQ(observed->term, 7);
}

// 检测心跳 DC 不一致时，节点会被标记为拓扑异常离线。
TEST(NodeHeartbeatServiceTest, HeartbeatWithMismatchedDcMarksNodeOffline) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    ASSERT_TRUE(store_test::put_node(store, make_node("node-a")).ok());
    NodeService service(&store);

    HeartBeatParam param = make_heartbeat_param();
    param.replica_list.clear();
    param.dc = "dc-b";
    ASSERT_TRUE(service.heartbeat(param).ok());

    NodeOr node;
    ASSERT_TRUE(store_test::get_node(store, "node-a", node).ok());
    ASSERT_FALSE(node.is_empty());
    EXPECT_EQ(node->meta.dc, "dc-a");
    EXPECT_EQ(node->meta.resource_pool, "pool-a");
    EXPECT_EQ(node->state.status, NodeStatus::OFFLINE);
    EXPECT_EQ(node->state.offline_reason, NodeOfflineReason::TOPOLOGY_MISMATCH);
    EXPECT_EQ(node->state.endpoint.ip, "10.0.0.1");
    EXPECT_EQ(node->state.endpoint.port, 19090);
}

// 检测心跳资源池不一致时，节点会被标记为拓扑异常离线。
TEST(NodeHeartbeatServiceTest, HeartbeatWithMismatchedResourcePoolMarksNodeOffline) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    ASSERT_TRUE(store_test::put_node(store, make_node("node-a")).ok());
    NodeService service(&store);

    HeartBeatParam param = make_heartbeat_param();
    param.replica_list.clear();
    param.resoure_pool_name = "pool-b";
    ASSERT_TRUE(service.heartbeat(param).ok());

    NodeOr node;
    ASSERT_TRUE(store_test::get_node(store, "node-a", node).ok());
    ASSERT_FALSE(node.is_empty());
    EXPECT_EQ(node->meta.dc, "dc-a");
    EXPECT_EQ(node->meta.resource_pool, "pool-a");
    EXPECT_EQ(node->state.status, NodeStatus::OFFLINE);
    EXPECT_EQ(node->state.offline_reason, NodeOfflineReason::TOPOLOGY_MISMATCH);
}

// 检测拓扑恢复一致后，正确心跳会清除异常并恢复节点。
TEST(NodeHeartbeatServiceTest, MatchingHeartbeatRestoresTopologyMismatchNode) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    Node node = make_node("node-a");
    node.state.status = NodeStatus::OFFLINE;
    node.state.offline_reason = NodeOfflineReason::TOPOLOGY_MISMATCH;
    ASSERT_TRUE(store_test::put_node(store, node).ok());
    NodeService service(&store);

    HeartBeatParam param = make_heartbeat_param();
    param.replica_list.clear();
    ASSERT_TRUE(service.heartbeat(param).ok());

    NodeOr stored;
    ASSERT_TRUE(store_test::get_node(store, "node-a", stored).ok());
    ASSERT_FALSE(stored.is_empty());
    EXPECT_EQ(stored->state.status, NodeStatus::ONLINE);
    EXPECT_EQ(stored->state.offline_reason, NodeOfflineReason::NONE);
    EXPECT_EQ(stored->meta.dc, "dc-a");
    EXPECT_EQ(stored->meta.resource_pool, "pool-a");
}

// 检测只有 leader 上报的完整 membership 会投影 observed_member_type。
TEST(NodeHeartbeatServiceTest, LeaderMembershipProjectsObservedMemberTypes) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    ReplicaID r0{1001, 0, 0};
    ReplicaID r1{1001, 0, 1};
    ReplicaID r2{1001, 0, 2};
    put_group_with_replicas(store, {r0, r1, r2});

    NodeService service(&store);
    HeartBeatParam param = make_heartbeat_param();
    param.node_id = "node-0";
    param.replica_list = {HeartBeatReplicaInfo{r0,
                                               ReplicaRole::LEADER,
                                               StorageReplicaStatus::READY,
                                               7,
                                               {make_heartbeat_member(r0, "node-0", RaftMemberType::VOTER),
                                                make_heartbeat_member(r1, "node-1", RaftMemberType::LEARNER)}}};

    ASSERT_TRUE(service.heartbeat(param).ok());

    ReplicaObservedOr observed0;
    ReplicaObservedOr observed1;
    ReplicaObservedOr observed2;
    ASSERT_TRUE(store_test::get_replica_observed(store, r0, observed0).ok());
    ASSERT_TRUE(store_test::get_replica_observed(store, r1, observed1).ok());
    ASSERT_TRUE(store_test::get_replica_observed(store, r2, observed2).ok());
    ASSERT_FALSE(observed0.is_empty());
    ASSERT_FALSE(observed1.is_empty());
    ASSERT_FALSE(observed2.is_empty());
    EXPECT_EQ(observed0->member_type, RaftMemberType::VOTER);
    EXPECT_EQ(observed1->member_type, RaftMemberType::LEARNER);
    EXPECT_EQ(observed2->member_type, RaftMemberType::NON_MEMBER);

    // 观测住在 RuntimeData：leader/term 上报进 membership_observed，
    // 不再回写 durable 的 ReplicaGroup。
    MembershipObservedOr membership_observed;
    ASSERT_TRUE(store_test::get_membership_observed(store, ShardID{1001, 0}, membership_observed).ok());
    ASSERT_FALSE(membership_observed.is_empty());
    EXPECT_EQ(membership_observed->term, 7);
    EXPECT_EQ(membership_observed->leader, r0);
}

// 检测 leader 上报空 membership 时不会投影，也不会推进 authority term。
TEST(NodeHeartbeatServiceTest, LeaderEmptyMembershipDoesNotProject) {    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    ReplicaID r0{1001, 0, 0};
    ReplicaID r1{1001, 0, 1};
    put_group_with_replicas(store, {r0, r1});
    MembershipObserved seeded;
    seeded.term = 5;
    seeded.leader = r0;
    ASSERT_TRUE(store_test::put_membership_observed(store, ShardID{1001, 0}, seeded).ok());

    NodeService service(&store);
    HeartBeatParam param = make_heartbeat_param();
    param.node_id = "node-0";
    param.replica_list = {
            HeartBeatReplicaInfo{r0, ReplicaRole::LEADER, StorageReplicaStatus::READY, 6, {}}};

    ASSERT_TRUE(service.heartbeat(param).ok());

    MembershipObservedOr membership_observed;
    ASSERT_TRUE(store_test::get_membership_observed(store, ShardID{1001, 0}, membership_observed).ok());
    ASSERT_FALSE(membership_observed.is_empty());
    EXPECT_EQ(membership_observed->term, 5);
    EXPECT_EQ(membership_observed->leader, r0);
    ReplicaObservedOr observed1;
    ASSERT_TRUE(store_test::get_replica_observed(store, r1, observed1).ok());
    ASSERT_FALSE(observed1.is_empty());
    EXPECT_EQ(observed1->member_type, RaftMemberType::NON_MEMBER);
}

// 检测同 term 不同 leader 的 membership report 会被拒绝。
TEST(NodeHeartbeatServiceTest, SameTermDifferentLeaderMembershipRejected) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    ReplicaID r0{1001, 0, 0};
    ReplicaID r1{1001, 0, 1};
    put_group_with_replicas(store, {r0, r1});
    MembershipObserved seeded;
    seeded.term = 7;
    seeded.leader = r0;
    ASSERT_TRUE(store_test::put_membership_observed(store, ShardID{1001, 0}, seeded).ok());

    NodeService service(&store);
    HeartBeatParam param = make_heartbeat_param();
    param.node_id = "node-1";
    param.replica_list = {HeartBeatReplicaInfo{r1,
                                               ReplicaRole::LEADER,
                                               StorageReplicaStatus::READY,
                                               7,
                                               {make_heartbeat_member(r1, "node-1", RaftMemberType::VOTER)}}};

    ASSERT_TRUE(service.heartbeat(param).ok());

    MembershipObservedOr membership_observed;
    ASSERT_TRUE(store_test::get_membership_observed(store, ShardID{1001, 0}, membership_observed).ok());
    ASSERT_FALSE(membership_observed.is_empty());
    EXPECT_EQ(membership_observed->leader, r0);
    EXPECT_EQ(membership_observed->term, 7);
    ReplicaObservedOr observed1;
    ASSERT_TRUE(store_test::get_replica_observed(store, r1, observed1).ok());
    ASSERT_FALSE(observed1.is_empty());
    EXPECT_EQ(observed1->member_type, RaftMemberType::NON_MEMBER);
}

// 检测 heartbeat 会忽略不存在的 replica 和不属于该节点的 replica。
TEST(NodeHeartbeatServiceTest, HeartbeatIgnoresMissingAndOtherNodeReplicas) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    ASSERT_TRUE(store_test::put_node(store, make_node("node-a")).ok());
    ASSERT_TRUE(store_test::put_replica(store, make_replica(ReplicaID{1001, 0, 1}, "node-b")).ok());
    ASSERT_TRUE(
            store_test::put_replica_observed(store, ReplicaID{1001, 0, 1}, make_observed(ReplicaID{1001, 0, 1})).ok());
    NodeService service(&store);

    HeartBeatParam param = make_heartbeat_param();
    param.replica_list.push_back(HeartBeatReplicaInfo{
            ReplicaID{1001, 0, 1},
            ReplicaRole::LEADER,
            StorageReplicaStatus::READY,
            9,
            {},
    });
    param.replica_list.push_back(HeartBeatReplicaInfo{
            ReplicaID{9999, 0, 0},
            ReplicaRole::LEADER,
            StorageReplicaStatus::READY,
            9,
            {},
    });

    Status status = service.heartbeat(param);

    ASSERT_TRUE(status.ok()) << status.msg();
    ReplicaOr other;
    ASSERT_TRUE(store_test::get_replica(store, ReplicaID{1001, 0, 1}, other).ok());
    ASSERT_FALSE(other.is_empty());
    EXPECT_EQ(other->state.phase, ReplicaPhase::CREATING);
    ReplicaObservedOr other_observed;
    ASSERT_TRUE(store_test::get_replica_observed(store, ReplicaID{1001, 0, 1}, other_observed).ok());
    ASSERT_FALSE(other_observed.is_empty());
    EXPECT_EQ(other_observed->raft_role, ReplicaRole::FOLLOWER);
    EXPECT_EQ(other_observed->term, 1);
}

// 检测心跳没上报 DELETING 状态的 replica 时，会记录 no-exist 观测，
// ReplicaGroupService 再将其推进为 DELETED。
TEST(NodeHeartbeatServiceTest, HeartbeatMarksDeletingReplicaDeletedWhenMissing) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    ASSERT_TRUE(store_test::put_node(store, make_node("node-a")).ok());

    Replica replica = make_replica(ReplicaID{1001, 0, 0}, "node-a");
    replica.state.phase = ReplicaPhase::DELETING;
    ASSERT_TRUE(store_test::put_replica(store, replica).ok());

    NodeService service(&store);
    HeartBeatParam param = make_heartbeat_param();
    param.replica_list.clear();

    Status status = service.heartbeat(param);

    ASSERT_TRUE(status.ok()) << status.msg();
    ReplicaOr stored;
    ASSERT_TRUE(store_test::get_replica(store, ReplicaID{1001, 0, 0}, stored).ok());
    ASSERT_FALSE(stored.is_empty());
    EXPECT_EQ(stored->state.phase, ReplicaPhase::DELETING);

    ReplicaObservedOr observed;
    ASSERT_TRUE(store_test::get_replica_observed(store, ReplicaID{1001, 0, 0}, observed).ok());
    ASSERT_FALSE(observed.is_empty());
    EXPECT_TRUE(observed->no_exist);

    ReplicaGroupService replica_group_service(&store, nullptr);
    ASSERT_TRUE(replica_group_service.reconcile_all().ok());
    ASSERT_TRUE(store_test::get_replica(store, ReplicaID{1001, 0, 0}, stored).ok());
    ASSERT_FALSE(stored.is_empty());
    EXPECT_EQ(stored->state.phase, ReplicaPhase::DELETED);
}

// 检测 ReplicaGroupService 能正确构建心跳响应：在 desired_members 中的 replica 发 PRESENT，不在的报 ABSENT。
TEST(NodeHeartbeatServiceTest, ReplicaGroupServiceBuildsHeartbeatExpectations) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    ASSERT_TRUE(store_test::put_node(store, make_node("node-a")).ok());

    Replica replica = make_replica(ReplicaID{1001, 0, 0}, "node-a");
    replica.spec.engine_type = EngineType::MAP;
    ASSERT_TRUE(store_test::put_replica(store, replica).ok());
    // initial_members 从观测记录里取 endpoint，必须先铺一条观测。
    ASSERT_TRUE(store_test::put_replica_observed(store, replica.replica_id, make_observed(replica.replica_id)).ok());

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
            {},
    });

    ReplicaGroupService replica_group_service(&store, nullptr);
    HeartBeatResult result;
    Status status = replica_group_service.build_heartbeat_result(param, result);

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

TEST(NodeHeartbeatServiceTest, EmptyMetaDoesNotDeleteUnknownStorageReplicas) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());

    HeartBeatParam param = make_heartbeat_param();
    param.replica_list.push_back(HeartBeatReplicaInfo{
            ReplicaID{1001, 0, 0},
            ReplicaRole::LEADER,
            StorageReplicaStatus::READY,
            1,
            {},
    });

    ReplicaGroupService service(&store, nullptr);
    HeartBeatResult result;
    Status status = service.build_heartbeat_result(param, result);

    EXPECT_EQ(status.code(), StatusCode::CONDITION_NOT_MET);
    EXPECT_TRUE(result.expects.empty());
}

TEST(NodeHeartbeatServiceTest, PartiallyInitializedTableDoesNotDeleteUnknownStorageReplicas) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());

    Table table;
    table.table_id = 1001;
    table.spec.db_id = 1;
    table.spec.db_name = "commerce";
    table.spec.name = "orders";
    table.spec.shard_ranges = make_shard_ranges(1);
    table.state.phase = TablePhase::CREATING;
    ASSERT_TRUE(store_test::put_table(store, table).ok());

    HeartBeatParam param = make_heartbeat_param();
    ReplicaGroupService service(&store, nullptr);
    HeartBeatResult result;
    ASSERT_TRUE(service.build_heartbeat_result(param, result).ok());
    EXPECT_TRUE(result.expects.empty());

    table.state.phase = TablePhase::DELETING;
    ASSERT_TRUE(store_test::put_table(store, table).ok());
    result.expects.clear();
    ASSERT_TRUE(service.build_heartbeat_result(param, result).ok());
    ASSERT_EQ(result.expects.size(), 1U);
    EXPECT_EQ(result.expects.front().replica_id, param.replica_list.front().replica_id);
    EXPECT_EQ(result.expects.front().type, ExpectedReplicaType::ABSENT);
}

// 检测非法 heartbeat 参数会被拒绝。
TEST(NodeHeartbeatServiceTest, HeartbeatRejectsInvalidParam) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    NodeService service(&store);
    HeartBeatParam param = make_heartbeat_param();
    param.node_id.clear();

    Status status = service.heartbeat(param);

    EXPECT_EQ(status.code(), StatusCode::INVALID_ARGUMENT);
}

// 检测节点不存在时，heartbeat 返回错误且不会创建新节点。
TEST(NodeHeartbeatServiceTest, HeartbeatReturnsErrorWhenNodeMissing) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    NodeService service(&store);

    Status status = service.heartbeat(make_heartbeat_param());

    EXPECT_TRUE(status.fail());
    NodeOr node;
    ASSERT_TRUE(store_test::get_node(store, "node-a", node).ok());
    EXPECT_TRUE(node.is_empty());
}

TEST(NodeHeartbeatServiceTest, StaleLowerTermDoesNotOverwriteReplica) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    Replica replica = make_replica(ReplicaID{1001, 0, 0}, "node-a");
    ReplicaObserved observed = make_observed(ReplicaID{1001, 0, 0});
    observed.term = 10;
    ASSERT_TRUE(store_test::put_node(store, make_node("node-a")).ok());
    ASSERT_TRUE(store_test::put_replica(store, replica).ok());
    ASSERT_TRUE(store_test::put_replica_observed(store, ReplicaID{1001, 0, 0}, observed).ok());
    NodeService service(&store);

    HeartBeatParam param = make_heartbeat_param();
    param.replica_list[0].term = 9;
    param.replica_list[0].role = ReplicaRole::LEADER;
    ASSERT_TRUE(service.heartbeat(param).ok());

    ReplicaObservedOr loaded;
    ASSERT_TRUE(store_test::get_replica_observed(store, ReplicaID{1001, 0, 0}, loaded).ok());
    ASSERT_FALSE(loaded.is_empty());
    EXPECT_EQ(loaded->term, 10);
    EXPECT_EQ(loaded->raft_role, ReplicaRole::FOLLOWER);
}

TEST(NodeHeartbeatServiceTest, RemovedMemberLeaderDoesNotProjectMembership) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    ReplicaID r0{1001, 0, 0};
    ReplicaID old_leader{1001, 0, 9};
    put_group_with_replicas(store, {r0});
    ASSERT_TRUE(store_test::put_node(store, make_node("node-old", 18100)).ok());
    ASSERT_TRUE(store_test::put_replica(store, make_replica(old_leader, "node-old")).ok());

    MembershipObserved seeded;
    seeded.term = 3;
    seeded.leader = r0;
    ASSERT_TRUE(store_test::put_membership_observed(store, ShardID{1001, 0}, seeded).ok());

    NodeService service(&store);
    HeartBeatParam param = make_heartbeat_param();
    param.node_id = "node-old";
    param.replica_list = {HeartBeatReplicaInfo{
            old_leader,
            ReplicaRole::LEADER,
            StorageReplicaStatus::READY,
            99,
            {make_heartbeat_member(old_leader, "node-old", RaftMemberType::VOTER)}}};
    ASSERT_TRUE(service.heartbeat(param).ok());

    // old_leader 不在 desired_members 里，上报被过滤，观测不被覆盖。
    MembershipObservedOr membership_observed;
    ASSERT_TRUE(store_test::get_membership_observed(store, ShardID{1001, 0}, membership_observed).ok());
    ASSERT_FALSE(membership_observed.is_empty());
    EXPECT_EQ(membership_observed->term, 3);
    EXPECT_EQ(membership_observed->leader, r0);
}

}  // namespace adviskv::meta
