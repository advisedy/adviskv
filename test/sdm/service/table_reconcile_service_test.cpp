#include "sdm/service/replica_group_service.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/model/storage_replica_status.h"
#include "common/status.h"
#include "sdm/model/sdm_store.h"
#include "sdm/sdm_store_test_helper.h"
#include "sdm/selector/node_selector/node_selector.h"
#include "sdm/service/node_service.h"
#include "sdm/service/replica_group_service.h"
#include "sdm/service/route_service.h"
#include "sdm/service/table_service.h"

namespace adviskv::sdm {
namespace {

class FakeNodeSelector : public NodeSelector {
   public:
    explicit FakeNodeSelector(SdmStore* store) : default_selector(store) {}

    Status select_table_nodes(const PlaceNodesParam& param,
                              TablePlacementResult& res) const override {
        if (!statuses.empty()) {
            Status status = statuses.front();
            statuses.pop_front();
            return status;
        }
        return default_selector.select_table_nodes(param, res);
    }

    mutable std::deque<Status> statuses;
    DefaultNodeSelector default_selector;
};

struct ServiceHarness {
    ServiceHarness(SdmStore* store, NodeSelector* selector)
        : table(store),
          node(store),
          replica_group(store, selector),
          route(store) {}

    TableService table;
    NodeService node;
    ReplicaGroupService replica_group;
    RouteService route;
};

Node make_node(const NodeID& id, int32_t port) {
    return Node{id, NodeMeta{"pool-a", "dc-a"},
                NodeState{NodeStatus::ONLINE, Endpoint{"127.0.0.1", port}, 1},
                NodeDerived{}};
}

PlaceTableParam make_place_table_param() {
    return PlaceTableParam{11, 1001, "commerce", "orders",
                           2,  1,    "pool-a",   "create-1001"};
}

void put_default_nodes(SdmStore& store) {
    ASSERT_TRUE(store_test::put_node(store, make_node("node-a", 18080)).ok());
    ASSERT_TRUE(store_test::put_node(store, make_node("node-b", 18081)).ok());
}

void mark_node_offline(SdmStore& store, const NodeID& node_id) {
    NodeOr node;
    ASSERT_TRUE(store_test::get_node(store, node_id, node).ok());
    ASSERT_FALSE(node.is_empty());
    node->state.status = NodeStatus::OFFLINE;
    ASSERT_TRUE(store_test::put_node(store, node.value()).ok());
}

void place_default_table(ServiceHarness& services) {
    ASSERT_TRUE(services.table.place_table(make_place_table_param()).ok());
}

Table get_table_or_die(SdmStore& store) {
    TableOr table;
    EXPECT_TRUE(store_test::get_table(store, 1001, table).ok());
    EXPECT_FALSE(table.is_empty());
    return *table;
}

std::vector<Replica> list_replicas_or_die(SdmStore& store) {
    std::vector<Replica> replicas;
    EXPECT_TRUE(
        store_test::list_replicas_by_shard(store, ShardID{1001, 0}, replicas)
            .ok());
    return replicas;
}

void make_replicas_ready(SdmStore& store) {
    std::vector<Replica> replicas = list_replicas_or_die(store);
    for (Replica& replica : replicas) {
        replica.state.phase = ReplicaPhase::READY;
        replica.state.observed_storage_status = StorageReplicaStatus::READY;
        replica.state.observed_raft_role = replica.replica_id.replica_seq == 0
                                               ? ReplicaRole::LEADER
                                               : ReplicaRole::FOLLOWER;
        replica.state.observed_member_type = RaftMemberType::VOTER;
        replica.state.observed_endpoint =
            Endpoint{"127.0.0.1", 18080 + replica.replica_id.replica_seq};
        replica.state.term = 1;
        ASSERT_TRUE(store_test::put_replica(store, replica).ok());
    }
}

void reconcile_table_to_ready(ServiceHarness& services, SdmStore& store) {
    ASSERT_TRUE(services.table.reconcile_all().ok());
    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    make_replicas_ready(store);
    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    ASSERT_TRUE(services.route.reconcile_all().ok());
    ASSERT_TRUE(services.table.reconcile_all().ok());
    EXPECT_EQ(get_table_or_die(store).state.phase, TablePhase::READY);
}

void mark_table_absent(SdmStore& store) {
    Table table = get_table_or_die(store);
    table.state.desired = TableDesired::ABSENT;
    table.state.phase = TablePhase::DELETING;
    table.spec.operation_id = "drop-1001";
    table.state.last_error_msg.clear();
    ASSERT_TRUE(store_test::put_table(store, table).ok());
}

void expect_table_phase_and_msg(SdmStore& store, TablePhase phase,
                                const std::string& msg) {
    Table table = get_table_or_die(store);
    EXPECT_EQ(table.state.phase, phase);
    if (!msg.empty()) {
        EXPECT_NE(table.state.last_error_msg.find(msg), std::string::npos);
    }
}

void put_route(SdmStore& store, std::vector<RouteEntry> replicas) {
    ShardRoute route;
    route.shard_id = ShardID{1001, 0};
    route.replicas = std::move(replicas);
    ASSERT_TRUE(store_test::put_shard_route(store, route).ok());
}

GetRouteParam make_get_route_param(const Key& key = "user-123") {
    return GetRouteParam{"commerce", "orders", key};
}

void report_replica_heartbeat(ServiceHarness& services, const NodeID& node_id,
                              int32_t port, ReplicaIndex replica_index,
                              ReplicaRole role, StorageReplicaStatus status,
                              Term term,
                              RaftMemberType member_type =
                                  RaftMemberType::VOTER) {
    HeartBeatParam param;
    param.node_id = node_id;
    param.ip = "127.0.0.1";
    param.port = port;
    param.resoure_pool_name = "pool-a";
    param.dc = "dc-a";
    param.last_heartbeat_ts = 1000 + replica_index + term;
    param.replica_list.push_back(HeartBeatReplicaInfo{
        ReplicaID{1001, 0, replica_index},
        role,
        status,
        term,
        member_type,
    });
    ASSERT_TRUE(services.node.heartbeat(param).ok());
}

}  // namespace

// 检测建表流程：创建 Table → reconcile 生成 Replica → 标记 READY → 路由生成 → Table 到达 READY。
TEST(TableServiceReconcileTest, CreateTableMaterializesReplicasAndReachesReady) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_default_nodes(store);

    FakeNodeSelector selector(&store);
    ServiceHarness services(&store, &selector);
    place_default_table(services);

    ASSERT_TRUE(services.table.reconcile_all().ok());
    ASSERT_TRUE(services.replica_group.reconcile_all().ok());

    make_replicas_ready(store);

    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    ASSERT_TRUE(services.route.reconcile_all().ok());
    ASSERT_TRUE(services.table.reconcile_all().ok());

    EXPECT_EQ(get_table_or_die(store).state.phase, TablePhase::READY);
}

// 检测节点选择器失败时，Table 停留在 CREATING，下一轮选择器恢复后可以继续。
TEST(TableServiceReconcileTest, CreateTableSelectorErrorKeepsTableCreating) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_default_nodes(store);

    FakeNodeSelector selector(&store);
    selector.statuses.push_back(Status::ERROR("select node failed"));
    ServiceHarness services(&store, &selector);
    place_default_table(services);

    ASSERT_TRUE(services.table.reconcile_all().ok());
    ASSERT_TRUE(services.replica_group.reconcile_all().ok());

    expect_table_phase_and_msg(store, TablePhase::CREATING, "");
    EXPECT_TRUE(list_replicas_or_die(store).empty());

    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    EXPECT_EQ(list_replicas_or_die(store).size(), 2U);
}

// 检测可用节点不足时，Table 停留在 CREATING，不会创建 Replica。
TEST(TableServiceReconcileTest, CreateTableNotEnoughNodesKeepsCreating) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store_test::put_node(store, make_node("node-a", 18080)).ok());

    FakeNodeSelector selector(&store);
    ServiceHarness services(&store, &selector);
    place_default_table(services);

    ASSERT_TRUE(services.table.reconcile_all().ok());
    ASSERT_TRUE(services.replica_group.reconcile_all().ok());

    expect_table_phase_and_msg(store, TablePhase::CREATING, "");
    EXPECT_TRUE(list_replicas_or_die(store).empty());
}

// 检测 Table 必须等待 ReplicaGroup 将所有 Replica 推进到 READY+VOTER 后才能变成 READY。
TEST(TableServiceReconcileTest, TableWaitsForReplicaGroupToMakeReplicasReady) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_default_nodes(store);

    FakeNodeSelector selector(&store);
    ServiceHarness services(&store, &selector);
    place_default_table(services);

    ASSERT_TRUE(services.table.reconcile_all().ok());
    ASSERT_TRUE(services.replica_group.reconcile_all().ok());

    std::vector<Replica> replicas = list_replicas_or_die(store);
    ASSERT_EQ(replicas.size(), 2U);
    EXPECT_EQ(replicas[0].state.phase, ReplicaPhase::CREATING);
    EXPECT_EQ(replicas[1].state.phase, ReplicaPhase::CREATING);
    ReplicaGroupOr group;
    ASSERT_TRUE(
        store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
    ASSERT_FALSE(group.is_empty());
    EXPECT_EQ(group->mode, ReplicaGroupMode::BOOTSTRAP);
    expect_table_phase_and_msg(store, TablePhase::CREATING, "");

    ASSERT_TRUE(services.table.reconcile_all().ok());
    EXPECT_EQ(get_table_or_die(store).state.phase, TablePhase::CREATING);

    make_replicas_ready(store);
    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    ASSERT_TRUE(services.route.reconcile_all().ok());
    ASSERT_TRUE(services.table.reconcile_all().ok());
    ASSERT_TRUE(
        store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
    ASSERT_FALSE(group.is_empty());
    EXPECT_EQ(group->mode, ReplicaGroupMode::RAFT_RECONFIG);
    EXPECT_EQ(get_table_or_die(store).state.phase, TablePhase::READY);
}

// 检测 Table 可以在 group 还处于 BOOTSTRAP 模式时就变成 READY（group 模式切换在下一轮 reconcile 才发生）。
TEST(TableServiceReconcileTest, TableCanBecomeReadyWhileGroupIsBootstrapMode) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_default_nodes(store);

    FakeNodeSelector selector(&store);
    ServiceHarness services(&store, &selector);
    place_default_table(services);

    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    make_replicas_ready(store);
    ASSERT_TRUE(services.route.reconcile_all().ok());
    ASSERT_TRUE(services.table.reconcile_all().ok());

    ReplicaGroupOr group;
    ASSERT_TRUE(
        store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
    ASSERT_FALSE(group.is_empty());
    EXPECT_EQ(group->mode, ReplicaGroupMode::BOOTSTRAP);
    EXPECT_EQ(get_table_or_die(store).state.phase, TablePhase::READY);

    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    ASSERT_TRUE(
        store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
    ASSERT_FALSE(group.is_empty());
    EXPECT_EQ(group->mode, ReplicaGroupMode::RAFT_RECONFIG);
}

// 检测坏副本替换流程：LOST 副本触发补新副本 → 新副本 READY+NON_MEMBER → 心跳发 ADD_MEMBER → 变 VOTER → 心跳发 REMOVE_MEMBER 删坏副本。
TEST(TableServiceReconcileTest, ReplicaGroupReplacesBadMemberAfterBackfill) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_default_nodes(store);
    ASSERT_TRUE(store_test::put_node(store, make_node("node-c", 18082)).ok());

    FakeNodeSelector selector(&store);
    ServiceHarness services(&store, &selector);
    place_default_table(services);
    reconcile_table_to_ready(services, store);

    std::vector<Replica> replicas = list_replicas_or_die(store);
    ASSERT_EQ(replicas.size(), 2U);
    ReplicaID leader_id = replicas[0].replica_id;
    ReplicaID bad_id = replicas[1].replica_id;
    replicas[1].state.phase = ReplicaPhase::LOST;
    ASSERT_TRUE(store_test::put_replica(store, replicas[1]).ok());
    mark_node_offline(store, replicas[1].spec.assign_node_id);

    ASSERT_TRUE(services.replica_group.reconcile_all().ok());

    ReplicaGroupOr group;
    ASSERT_TRUE(
        store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
    ASSERT_FALSE(group.is_empty());
    EXPECT_EQ(group->target_replica_count, 2);
    EXPECT_EQ(group->desired_members.size(), 3U);
    ReplicaID replacement_id;
    bool found_replacement = false;
    for (const ReplicaID& rid : group->desired_members) {
        if (rid != leader_id && rid != bad_id) {
            replacement_id = rid;
            found_replacement = true;
            break;
        }
    }
    ASSERT_TRUE(found_replacement);

    ASSERT_TRUE(services.replica_group.reconcile_all().ok());

    ASSERT_TRUE(
        store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
    ASSERT_FALSE(group.is_empty());
    EXPECT_EQ(group->desired_members.size(), 3U);
    EXPECT_NE(std::find(group->desired_members.begin(),
                        group->desired_members.end(),
                        bad_id),
              group->desired_members.end());

    ReplicaOr bad_replica;
    ASSERT_TRUE(store_test::get_replica(store, bad_id, bad_replica).ok());
    ASSERT_FALSE(bad_replica.is_empty());
    EXPECT_EQ(bad_replica->state.desired, ReplicaDesired::PRESENT);
    EXPECT_EQ(bad_replica->state.phase, ReplicaPhase::LOST);

    ReplicaOr replacement_replica;
    ASSERT_TRUE(
        store_test::get_replica(store, replacement_id, replacement_replica)
            .ok());
    ASSERT_FALSE(replacement_replica.is_empty());
    replacement_replica->state.phase = ReplicaPhase::READY;
    replacement_replica->state.observed_storage_status =
        StorageReplicaStatus::READY;
    replacement_replica->state.observed_raft_role = ReplicaRole::FOLLOWER;
    replacement_replica->state.observed_member_type =
        RaftMemberType::NON_MEMBER;
    replacement_replica->state.term = 2;
    ASSERT_TRUE(store_test::put_replica(store, replacement_replica.value()).ok());

    HeartBeatParam param;
    param.node_id = "node-a";
    param.ip = "127.0.0.1";
    param.port = 18080;
    param.resoure_pool_name = "pool-a";
    param.dc = "dc-a";
    param.last_heartbeat_ts = 3000;
    param.replica_list.push_back(HeartBeatReplicaInfo{
        leader_id,
        ReplicaRole::LEADER,
        StorageReplicaStatus::READY,
        1,
        RaftMemberType::VOTER,
    });

    HeartBeatResult result;
    ASSERT_TRUE(services.replica_group.build_heartbeat_result(param, result)
                    .ok());
    auto add_it = std::find_if(
        result.expects.begin(), result.expects.end(),
        [&](const ExpectedReplica& expect) {
            return expect.type == ExpectedReplicaType::ADD_MEMBER &&
                   expect.replica_id == replacement_id;
        });
    EXPECT_NE(add_it, result.expects.end());
    auto remove_it = std::find_if(
        result.expects.begin(), result.expects.end(),
        [&](const ExpectedReplica& expect) {
            return expect.type == ExpectedReplicaType::REMOVE_MEMBER &&
                   expect.replica_id == bad_id;
        });
    EXPECT_EQ(remove_it, result.expects.end());

    replacement_replica->state.observed_member_type = RaftMemberType::VOTER;
    ASSERT_TRUE(store_test::put_replica(store, replacement_replica.value()).ok());

    result.expects.clear();
    ASSERT_TRUE(services.replica_group.build_heartbeat_result(param, result)
                    .ok());
    remove_it = std::find_if(
        result.expects.begin(), result.expects.end(),
        [&](const ExpectedReplica& expect) {
            return expect.type == ExpectedReplicaType::REMOVE_MEMBER &&
                   expect.replica_id == bad_id;
        });
    EXPECT_NE(remove_it, result.expects.end());

    bad_replica->state.observed_member_type = RaftMemberType::NON_MEMBER;
    ASSERT_TRUE(store_test::put_replica(store, bad_replica.value()).ok());

    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    ASSERT_TRUE(
        store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
    ASSERT_FALSE(group.is_empty());
    EXPECT_EQ(group->desired_members.size(), 2U);
    EXPECT_EQ(std::find(group->desired_members.begin(),
                        group->desired_members.end(),
                        bad_id),
              group->desired_members.end());
    EXPECT_NE(std::find(group->desired_members.begin(),
                        group->desired_members.end(),
                        replacement_id),
              group->desired_members.end());

    ASSERT_TRUE(store_test::get_replica(store, bad_id, bad_replica).ok());
    ASSERT_FALSE(bad_replica.is_empty());
    EXPECT_EQ(bad_replica->state.desired, ReplicaDesired::ABSENT);
    EXPECT_EQ(bad_replica->state.phase, ReplicaPhase::DELETING);
}

// 检测 READY 表发生内部副本替换时仍保持 READY，并且外部 alter 仍可进入 RESIZING。
TEST(TableServiceReconcileTest, ReplacementKeepsTableReadyAndAllowsAlter) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_default_nodes(store);
    ASSERT_TRUE(store_test::put_node(store, make_node("node-c", 18082)).ok());

    FakeNodeSelector selector(&store);
    ServiceHarness services(&store, &selector);
    place_default_table(services);
    reconcile_table_to_ready(services, store);

    std::vector<Replica> replicas = list_replicas_or_die(store);
    ASSERT_EQ(replicas.size(), 2U);
    replicas[1].state.phase = ReplicaPhase::LOST;
    ASSERT_TRUE(store_test::put_replica(store, replicas[1]).ok());
    mark_node_offline(store, replicas[1].spec.assign_node_id);

    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    EXPECT_EQ(get_table_or_die(store).state.phase, TablePhase::READY);

    Status alter_status = services.table.alter_table_replica_count(
        AlterReplicaCountParam{1001, 3, "expand-during-replacement"});
    ASSERT_TRUE(alter_status.ok()) << alter_status.to_string();
    EXPECT_EQ(get_table_or_die(store).state.phase, TablePhase::RESIZING);

    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    ReplicaGroupOr group;
    ASSERT_TRUE(
        store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
    ASSERT_FALSE(group.is_empty());
    EXPECT_EQ(group->target_replica_count, 3);
    EXPECT_EQ(group->desired_members.size(), 4U);
}

// 检测缩容流程：alter 减少副本数 → victim 变成 NON_MEMBER → reconciler 将其从 desired_members 移出并标 DELETING。
TEST(TableServiceReconcileTest, ReplicaGroupShrinksAfterObservedMemberRemoved) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_default_nodes(store);
    ASSERT_TRUE(store_test::put_node(store, make_node("node-c", 18082)).ok());

    FakeNodeSelector selector(&store);
    ServiceHarness services(&store, &selector);
    PlaceTableParam param = make_place_table_param();
    param.replica_count = 3;
    ASSERT_TRUE(services.table.place_table(param).ok());
    reconcile_table_to_ready(services, store);

    ASSERT_TRUE(services.table.alter_table_replica_count(
                            AlterReplicaCountParam{1001, 2, "shrink-1001"})
                    .ok());
    ASSERT_TRUE(services.replica_group.reconcile_all().ok());

    ReplicaGroupOr group;
    ASSERT_TRUE(
        store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
    ASSERT_FALSE(group.is_empty());
    ASSERT_EQ(group->desired_members.size(), 3U);
    EXPECT_EQ(group->target_replica_count, 2);
    ReplicaID leader_id = group->desired_members.front();
    ReplicaID victim_id = group->desired_members.back();

    ReplicaOr victim;
    ASSERT_TRUE(store_test::get_replica(store, victim_id, victim).ok());
    ASSERT_FALSE(victim.is_empty());
    victim->state.observed_member_type = RaftMemberType::NON_MEMBER;
    ASSERT_TRUE(store_test::put_replica(store, victim.value()).ok());

    HeartBeatParam hb;
    hb.node_id = "node-a";
    hb.ip = "127.0.0.1";
    hb.port = 18080;
    hb.resoure_pool_name = "pool-a";
    hb.dc = "dc-a";
    hb.last_heartbeat_ts = 4000;
    hb.replica_list.push_back(HeartBeatReplicaInfo{
        leader_id,
        ReplicaRole::LEADER,
        StorageReplicaStatus::READY,
        1,
        RaftMemberType::VOTER,
    });

    HeartBeatResult result;
    ASSERT_TRUE(services.replica_group.build_heartbeat_result(hb, result).ok());
    auto add_victim_it = std::find_if(
        result.expects.begin(), result.expects.end(),
        [&](const ExpectedReplica& expect) {
            return expect.type == ExpectedReplicaType::ADD_MEMBER &&
                   expect.replica_id == victim_id;
        });
    EXPECT_EQ(add_victim_it, result.expects.end());

    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    ASSERT_TRUE(
        store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
    ASSERT_FALSE(group.is_empty());
    EXPECT_EQ(group->desired_members.size(), 2U);
    EXPECT_EQ(std::find(group->desired_members.begin(),
                        group->desired_members.end(),
                        victim_id),
              group->desired_members.end());

    ASSERT_TRUE(store_test::get_replica(store, victim_id, victim).ok());
    ASSERT_FALSE(victim.is_empty());
    EXPECT_EQ(victim->state.desired, ReplicaDesired::ABSENT);
    EXPECT_EQ(victim->state.phase, ReplicaPhase::DELETING);
}

// 检测 Replica 出现 ERROR 时，Table 不会被标记为 FAILED，而是继续等待
// ReplicaGroupMembershipReconciler 替换坏副本。
TEST(TableServiceReconcileTest, ReplicaErrorKeepsTableWaiting) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_default_nodes(store);

    FakeNodeSelector selector(&store);
    ServiceHarness services(&store, &selector);
    place_default_table(services);
    ASSERT_TRUE(services.table.reconcile_all().ok());
    ASSERT_TRUE(services.replica_group.reconcile_all().ok());

    std::vector<Replica> replicas = list_replicas_or_die(store);
    ASSERT_EQ(replicas.size(), 2U);
    replicas[0].state.phase = ReplicaPhase::ERROR;
    replicas[0].state.last_error_msg = "replica reconcile failed";
    ASSERT_TRUE(store_test::put_replica(store, replicas[0]).ok());

    ASSERT_TRUE(services.table.reconcile_all().ok());

    expect_table_phase_and_msg(store, TablePhase::CREATING, "");
    EXPECT_TRUE(get_table_or_die(store).state.last_error_msg.empty());
}

// 检测路由中有多个 leader 时，Table 不会变成 READY。
TEST(TableServiceReconcileTest, TableDoesNotBecomeReadyWhenRouteHasMultipleLeaders) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_default_nodes(store);

    FakeNodeSelector selector(&store);
    ServiceHarness services(&store, &selector);
    place_default_table(services);

    ASSERT_TRUE(services.table.reconcile_all().ok());
    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    make_replicas_ready(store);
    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    ASSERT_TRUE(services.table.reconcile_all().ok());

    put_route(store, {RouteEntry{ReplicaID{1001, 0, 0}, "node-a", "127.0.0.1",
                                 18080, ReplicaRole::LEADER, 5},
                      RouteEntry{ReplicaID{1001, 0, 1}, "node-b", "127.0.0.1",
                                 18081, ReplicaRole::LEADER, 4}});

    ASSERT_TRUE(services.table.reconcile_all().ok());
    EXPECT_EQ(get_table_or_die(store).state.phase, TablePhase::CREATING);
}

// 检测 leader 的 endpoint 无效时，Table 不会变成 READY。
TEST(TableServiceReconcileTest, TableDoesNotBecomeReadyWhenLeaderEndpointInvalid) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_default_nodes(store);

    FakeNodeSelector selector(&store);
    ServiceHarness services(&store, &selector);
    place_default_table(services);

    ASSERT_TRUE(services.table.reconcile_all().ok());
    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    make_replicas_ready(store);
    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    ASSERT_TRUE(services.table.reconcile_all().ok());

    put_route(store, {RouteEntry{ReplicaID{1001, 0, 0}, "node-a", "", 0,
                                 ReplicaRole::LEADER, 5},
                      RouteEntry{ReplicaID{1001, 0, 1}, "node-b", "127.0.0.1",
                                 18081, ReplicaRole::FOLLOWER, 5}});

    ASSERT_TRUE(services.table.reconcile_all().ok());
    EXPECT_EQ(get_table_or_die(store).state.phase, TablePhase::CREATING);
}

// 检测 leader 丢失后路由被删除，心跳恢复新 leader 后路由重新发布。
TEST(TableServiceReconcileTest, RouteIsRepublishedAfterHeartbeatRecoversLeader) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_default_nodes(store);

    FakeNodeSelector selector(&store);
    ServiceHarness services(&store, &selector);
    place_default_table(services);
    reconcile_table_to_ready(services, store);

    RouteService route_service(&store);
    ShardRoute route;
    Status status = route_service.get_route(make_get_route_param(), &route);
    ASSERT_TRUE(status.ok()) << status.msg();
    ASSERT_EQ(route.replicas.size(), 2U);
    EXPECT_EQ(route.replicas[0].role, ReplicaRole::LEADER);

    std::vector<Replica> replicas = list_replicas_or_die(store);
    ASSERT_EQ(replicas.size(), 2U);
    for (Replica& replica : replicas) {
        replica.state.observed_raft_role = ReplicaRole::FOLLOWER;
        replica.state.term = 8;
        ASSERT_TRUE(store_test::put_replica(store, replica).ok());
    }

    ASSERT_TRUE(services.route.reconcile_all().ok());

    ShardRouteOr route_after_loss;
    ASSERT_TRUE(
        store_test::get_shard_route(store, ShardID{1001, 0}, route_after_loss)
            .ok());
    EXPECT_TRUE(route_after_loss.is_empty());
    status = route_service.get_route(make_get_route_param(), &route);
    EXPECT_EQ(status.code(), StatusCode::ROUTE_NOT_FOUND);

    report_replica_heartbeat(services, "node-a", 18080, 0, ReplicaRole::LEADER,
                             StorageReplicaStatus::READY, 11);
    report_replica_heartbeat(services, "node-b", 18081, 1, ReplicaRole::FOLLOWER,
                             StorageReplicaStatus::READY, 11);

    ASSERT_TRUE(services.route.reconcile_all().ok());

    status = route_service.get_route(make_get_route_param(), &route);
    ASSERT_TRUE(status.ok()) << status.msg();
    ASSERT_EQ(route.replicas.size(), 2U);
    EXPECT_EQ(route.replicas[0].replica_id.replica_seq, 0);
    EXPECT_EQ(route.replicas[0].role, ReplicaRole::LEADER);
    EXPECT_EQ(route.replicas[0].term, 11);
    EXPECT_EQ(route.replicas[0].ip, "127.0.0.1");
    EXPECT_EQ(route.replicas[0].port, 18080);
    EXPECT_EQ(route.replicas[1].replica_id.replica_seq, 1);
    EXPECT_EQ(route.replicas[1].role, ReplicaRole::FOLLOWER);

    ASSERT_TRUE(services.table.reconcile_all().ok());
    EXPECT_EQ(get_table_or_die(store).state.phase, TablePhase::READY);
}

// 检测删表流程：标记 ABSENT → 清理 ReplicaGroup → 删除路由 → Replica 物理删除 → Table 变 DELETED。
TEST(TableServiceReconcileTest, DropTableWaitsForReplicaGroupCleanup) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_default_nodes(store);

    FakeNodeSelector selector(&store);
    ServiceHarness services(&store, &selector);
    place_default_table(services);
    reconcile_table_to_ready(services, store);
    ShardRouteOr route_before;
    ASSERT_TRUE(
        store_test::get_shard_route(store, ShardID{1001, 0}, route_before)
            .ok());
    ASSERT_FALSE(route_before.is_empty());

    mark_table_absent(store);

    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    ASSERT_TRUE(services.route.reconcile_all().ok());
    ASSERT_TRUE(services.table.reconcile_all().ok());

    Table table = get_table_or_die(store);
    EXPECT_EQ(table.state.desired, TableDesired::ABSENT);
    EXPECT_EQ(table.state.phase, TablePhase::DELETING);

    std::vector<Replica> replicas = list_replicas_or_die(store);
    ASSERT_EQ(replicas.size(), 2U);
    for (const Replica& replica : replicas) {
        EXPECT_EQ(replica.state.desired, ReplicaDesired::ABSENT);
        EXPECT_EQ(replica.state.phase, ReplicaPhase::DELETING);
    }

    ReplicaGroupOr group;
    ASSERT_TRUE(
        store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
    ASSERT_FALSE(group.is_empty());
    EXPECT_EQ(group->mode, ReplicaGroupMode::RAFT_RECONFIG);
    EXPECT_EQ(group->target_replica_count, 0);
    EXPECT_TRUE(group->desired_members.empty());

    ShardRouteOr route;
    ASSERT_TRUE(
        store_test::get_shard_route(store, ShardID{1001, 0}, route).ok());
    EXPECT_TRUE(route.is_empty());

    for (Replica& replica : replicas) {
        replica.state.phase = ReplicaPhase::DELETED;
        ASSERT_TRUE(store_test::put_replica(store, replica).ok());
    }

    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    ASSERT_TRUE(services.route.reconcile_all().ok());
    ASSERT_TRUE(services.table.reconcile_all().ok());

    table = get_table_or_die(store);
    EXPECT_EQ(table.state.desired, TableDesired::ABSENT);
    EXPECT_EQ(table.state.phase, TablePhase::DELETED);
    EXPECT_TRUE(list_replicas_or_die(store).empty());
    ASSERT_TRUE(
        store_test::get_shard_route(store, ShardID{1001, 0}, route).ok());
    EXPECT_TRUE(route.is_empty());
    ASSERT_TRUE(
        store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
    EXPECT_TRUE(group.is_empty());
}

// 检测 FAILED 状态的 Table 不会被 reconcile 重试推进。
TEST(TableServiceReconcileTest, FailedDropTableWillNotRetry) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_default_nodes(store);

    FakeNodeSelector selector(&store);
    ServiceHarness services(&store, &selector);
    place_default_table(services);
    reconcile_table_to_ready(services, store);
    mark_table_absent(store);
    Table failed = get_table_or_die(store);
    failed.state.phase = TablePhase::FAILED;
    failed.state.last_error_msg = "manual failure";
    ASSERT_TRUE(store_test::put_table(store, failed).ok());
    ASSERT_EQ(get_table_or_die(store).state.phase, TablePhase::FAILED);

    ASSERT_TRUE(services.table.reconcile_all().ok());

    expect_table_phase_and_msg(store, TablePhase::FAILED, "manual failure");
}

}  // namespace adviskv::sdm
