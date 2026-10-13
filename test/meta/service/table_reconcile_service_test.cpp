#include <algorithm>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "common/model/model.h"
#include "common/status.h"
#include "meta/placement/node_selector.h"
#include "meta/service/ddl_service.h"
#include "meta/service/node_service.h"
#include "meta/service/replica_group_service.h"
#include "meta/service/route_service.h"
#include "meta/store/meta_store.h"
#include "test/meta/store/meta_store_test_helper.h"

namespace adviskv::meta {
namespace {

class FakeNodeSelector : public NodeSelector {
public:
    explicit FakeNodeSelector(MetaStore* store) : default_selector(store) {}

    Status select_table_nodes(const PlaceNodesParam& param, TablePlacementResult& res) const override {
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
    ServiceHarness(MetaStore* store, NodeSelector* selector)
            : table(store), node(store), replica_group(store, selector), route(store) {
        // 这些用例沿用固定 ID 构造心跳和路由，先推进 allocator，再通过 DdlService 建表。
        EXPECT_TRUE(store->write_with([](MetaStoreTxn& txn) {
                             for (DatabaseID id = 0; id < 11; ++id) {
                                 EXPECT_EQ(txn.allocate_db_id(), id);
                             }
                             for (TableID id = 0; id < 1001; ++id) {
                                 EXPECT_EQ(txn.allocate_table_id(), id);
                             }
                             return Status::OK();
                         }).ok());
        EXPECT_TRUE(table.create_db(CreateDBParam{"commerce", "zone-a"}, nullptr).ok());
    }

    DdlService table;
    NodeService node;
    ReplicaGroupService replica_group;
    RouteService route;
};

Node make_node(const NodeID& id, int32_t port) {
    return Node{id, NodeMeta{"pool-a", "dc-a"}, NodeState{NodeStatus::ONLINE, Endpoint{"127.0.0.1", port}, 1}};
}

CreateTableParam make_create_table_param() { return CreateTableParam{"commerce", "orders", 1, 2, "pool-a"}; }

void put_default_nodes(MetaStore& store) {
    ASSERT_TRUE(store_test::put_node(store, make_node("node-a", 18080)).ok());
    ASSERT_TRUE(store_test::put_node(store, make_node("node-b", 18081)).ok());
}

void mark_node_offline(MetaStore& store, const NodeID& node_id) {
    NodeOr node;
    ASSERT_TRUE(store_test::get_node(store, node_id, node).ok());
    ASSERT_FALSE(node.is_empty());
    node->state.status = NodeStatus::OFFLINE;
    ASSERT_TRUE(store_test::put_node(store, node.value()).ok());
}

void place_default_table(ServiceHarness& services) {
    ASSERT_TRUE(services.table.create_table(make_create_table_param(), nullptr).ok());
}

Table get_table_or_die(MetaStore& store) {
    TableOr table;
    EXPECT_TRUE(store_test::get_table(store, 1001, table).ok());
    EXPECT_FALSE(table.is_empty());
    return *table;
}

std::vector<Replica> list_replicas_or_die(MetaStore& store) {
    std::vector<Replica> replicas;
    EXPECT_TRUE(store_test::list_replicas_by_shard(store, ShardID{1001, 0}, replicas).ok());
    return replicas;
}

void update_replica_observed(MetaStore& store, const ReplicaID& replica_id,
                             const std::function<void(ReplicaObserved&)>& update) {
    ASSERT_TRUE(store.write_with([&](MetaStoreTxn& txn) -> Status {
        ReplicaObservedOr observed_or;
        RETURN_IF_INVALID_STATUS(txn.get_replica_observed(replica_id, observed_or))
        ReplicaObserved observed = observed_or.is_empty() ? ReplicaObserved{} : observed_or.value();
        update(observed);
        return txn.put_replica_observed(replica_id, observed);
    }).ok());
}

ReplicaObserved get_replica_observed_or_die(MetaStore& store, const ReplicaID& replica_id) {
    ReplicaObservedOr observed;
    EXPECT_TRUE(store.read_with([&](const MetaStoreTxn& txn) {
        return txn.get_replica_observed(replica_id, observed);
    }).ok());
    EXPECT_FALSE(observed.is_empty());
    return observed.value();
}

void make_replicas_ready(MetaStore& store) {
    std::vector<Replica> replicas = list_replicas_or_die(store);
    for (Replica& replica : replicas) {
        replica.state.phase = ReplicaPhase::READY;
        ASSERT_TRUE(store_test::put_replica(store, replica).ok());
        update_replica_observed(store, replica.replica_id, [&](ReplicaObserved& observed) {
            observed.storage_status = StorageReplicaStatus::READY;
            observed.raft_role = replica.replica_id.replica_seq == 0 ? ReplicaRole::LEADER : ReplicaRole::FOLLOWER;
            observed.member_type = RaftMemberType::VOTER;
            observed.endpoint = Endpoint{"127.0.0.1", 18080 + replica.replica_id.replica_seq};
            observed.term = 1;
        });
    }
}

void reconcile_table_to_ready(ServiceHarness& services, MetaStore& store) {
    ASSERT_TRUE(services.table.reconcile_all().ok());
    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    make_replicas_ready(store);
    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    ASSERT_TRUE(services.route.reconcile_all().ok());
    ASSERT_TRUE(services.table.reconcile_all().ok());
    EXPECT_EQ(get_table_or_die(store).state.phase, TablePhase::READY);
}

void mark_table_absent(DdlService& ddl) {
    ASSERT_TRUE(ddl.drop_table(DropTableByNameParam{"commerce", "orders"}, nullptr).ok());
}

void expect_table_phase_and_msg(MetaStore& store, TablePhase phase, const std::string& msg) {
    Table table = get_table_or_die(store);
    EXPECT_EQ(table.state.phase, phase);
    if (!msg.empty()) {
        EXPECT_NE(table.state.last_error_msg.find(msg), std::string::npos);
    }
}

void put_route(MetaStore& store, std::vector<RouteReplica> replicas) {
    ShardRoute route;
    route.shard_id = ShardID{1001, 0};
    route.replicas = std::move(replicas);
    ASSERT_TRUE(store_test::put_shard_route(store, route).ok());
}

GetTableRoutesParam make_get_table_routes_param() { return GetTableRoutesParam{"commerce", "orders", false}; }

void report_replica_heartbeat(ServiceHarness& services, const NodeID& node_id, int32_t port, ReplicaIndex replica_index,
                              ReplicaRole role, StorageReplicaStatus status, Term term) {
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
            {},
    });
    ASSERT_TRUE(services.node.heartbeat(param).ok());
}

}  // namespace

// 检测建表流程：创建 Table → reconcile 生成 Replica → 标记 READY → 路由生成 → Table 到达 READY。
TEST(TableReconcilerTest, CreateTableMaterializesReplicasAndReachesReady) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
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

// 检测 replica 创建时继承 table spec 中的 engine_type。
TEST(TableReconcilerTest, CreateTableMaterializesReplicaEngineType) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    put_default_nodes(store);

    FakeNodeSelector selector(&store);
    ServiceHarness services(&store, &selector);
    CreateTableParam param = make_create_table_param();
    param.engine_type = EngineType::MAP;
    ASSERT_TRUE(services.table.create_table(param, nullptr).ok());

    ASSERT_TRUE(services.table.reconcile_all().ok());
    ASSERT_TRUE(services.replica_group.reconcile_all().ok());

    std::vector<Replica> replicas = list_replicas_or_die(store);
    ASSERT_EQ(replicas.size(), 2U);
    for (const Replica& replica : replicas) {
        EXPECT_EQ(replica.spec.engine_type, EngineType::MAP);
    }
}

// 检测 scale-to-zero 建表可以收敛为 READY，但不会发布可写路由。
TEST(TableReconcilerTest, ZeroReplicaTableReachesReadyWithoutRoute) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    FakeNodeSelector selector(&store);
    ServiceHarness services(&store, &selector);

    CreateTableParam param = make_create_table_param();
    param.replica_count = 0;
    ASSERT_TRUE(services.table.create_table(param, nullptr).ok());

    ASSERT_TRUE(services.table.reconcile_all().ok());
    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    ASSERT_TRUE(services.route.reconcile_all().ok());
    ASSERT_TRUE(services.table.reconcile_all().ok());

    Table table = get_table_or_die(store);
    EXPECT_EQ(table.spec.target_replica_count, 0);
    EXPECT_EQ(table.state.phase, TablePhase::READY);
    EXPECT_TRUE(list_replicas_or_die(store).empty());

    ReplicaGroupOr group;
    ASSERT_TRUE(store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
    ASSERT_FALSE(group.is_empty());
    EXPECT_EQ(group->mode, ReplicaGroupMode::BOOTSTRAP);
    EXPECT_EQ(group->target_replica_count, 0);
    EXPECT_TRUE(group->desired_members.empty());

    ShardRouteOr route;
    ASSERT_TRUE(store_test::get_shard_route(store, ShardID{1001, 0}, route).ok());
    EXPECT_TRUE(route.is_empty());

    std::vector<ShardRoute> routes;
    EXPECT_EQ(services.route.get_table_routes(make_get_table_routes_param(), nullptr, &routes).code(),
              StatusCode::ROUTE_NOT_FOUND);
}

// 检测 scale-to-zero 表可以后续扩容并恢复可写路由。
TEST(TableReconcilerTest, ZeroReplicaTableCanExpandAndPublishRoute) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    put_default_nodes(store);

    FakeNodeSelector selector(&store);
    ServiceHarness services(&store, &selector);

    CreateTableParam param = make_create_table_param();
    param.replica_count = 0;
    ASSERT_TRUE(services.table.create_table(param, nullptr).ok());
    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    ASSERT_TRUE(services.route.reconcile_all().ok());
    ASSERT_TRUE(services.table.reconcile_all().ok());
    ASSERT_EQ(get_table_or_die(store).state.phase, TablePhase::READY);

    Status alter_status =
            services.table.alter_table_replica_count(AlterTableReplicaCountParam{"commerce", "orders", 2}, nullptr);
    ASSERT_TRUE(alter_status.ok()) << alter_status.to_string();
    EXPECT_EQ(get_table_or_die(store).state.phase, TablePhase::RESIZING);

    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    std::vector<Replica> replicas = list_replicas_or_die(store);
    ASSERT_EQ(replicas.size(), 2U);

    make_replicas_ready(store);
    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    ASSERT_TRUE(services.route.reconcile_all().ok());
    ASSERT_TRUE(services.table.reconcile_all().ok());

    Table table = get_table_or_die(store);
    EXPECT_EQ(table.spec.target_replica_count, 2);
    EXPECT_EQ(table.state.phase, TablePhase::READY);
    std::vector<ShardRoute> routes;
    ASSERT_TRUE(services.route.get_table_routes(make_get_table_routes_param(), nullptr, &routes).ok());
    ASSERT_EQ(routes.size(), 1U);
    EXPECT_EQ(routes[0].replicas.size(), 2U);
}

// 检测节点选择器失败时，Table 停留在 CREATING，下一轮选择器恢复后可以继续。
TEST(TableReconcilerTest, CreateTableSelectorErrorKeepsTableCreating) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
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
TEST(TableReconcilerTest, CreateTableNotEnoughNodesKeepsCreating) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
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
TEST(TableReconcilerTest, TableWaitsForReplicaGroupToMakeReplicasReady) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
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
    ASSERT_TRUE(store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
    ASSERT_FALSE(group.is_empty());
    EXPECT_EQ(group->mode, ReplicaGroupMode::BOOTSTRAP);
    expect_table_phase_and_msg(store, TablePhase::CREATING, "");

    ASSERT_TRUE(services.table.reconcile_all().ok());
    EXPECT_EQ(get_table_or_die(store).state.phase, TablePhase::CREATING);

    make_replicas_ready(store);
    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    ASSERT_TRUE(services.route.reconcile_all().ok());
    ASSERT_TRUE(services.table.reconcile_all().ok());
    ASSERT_TRUE(store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
    ASSERT_FALSE(group.is_empty());
    EXPECT_EQ(group->mode, ReplicaGroupMode::RAFT_RECONFIG);
    EXPECT_EQ(get_table_or_die(store).state.phase, TablePhase::READY);
}

// 检测 Table 可以在 group 还处于 BOOTSTRAP 模式时就变成 READY（group 模式切换在下一轮 reconcile 才发生）。
TEST(TableReconcilerTest, TableCanBecomeReadyWhileGroupIsBootstrapMode) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    put_default_nodes(store);

    FakeNodeSelector selector(&store);
    ServiceHarness services(&store, &selector);
    place_default_table(services);

    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    make_replicas_ready(store);
    ASSERT_TRUE(services.route.reconcile_all().ok());
    ASSERT_TRUE(services.table.reconcile_all().ok());

    ReplicaGroupOr group;
    ASSERT_TRUE(store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
    ASSERT_FALSE(group.is_empty());
    EXPECT_EQ(group->mode, ReplicaGroupMode::BOOTSTRAP);
    EXPECT_EQ(get_table_or_die(store).state.phase, TablePhase::READY);

    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    ASSERT_TRUE(store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
    ASSERT_FALSE(group.is_empty());
    EXPECT_EQ(group->mode, ReplicaGroupMode::RAFT_RECONFIG);
}

// 检测坏副本替换流程：LOST 副本触发补新副本 → 新副本 READY+NON_MEMBER → 心跳发 ADD_MEMBER → 变 VOTER → 心跳发
// REMOVE_MEMBER 删坏副本。
TEST(TableReconcilerTest, ReplicaGroupReplacesBadMemberAfterBackfill) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
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
    ASSERT_TRUE(store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
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

    ASSERT_TRUE(store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
    ASSERT_FALSE(group.is_empty());
    EXPECT_EQ(group->desired_members.size(), 3U);
    EXPECT_NE(std::find(group->desired_members.begin(), group->desired_members.end(), bad_id),
              group->desired_members.end());

    ReplicaOr bad_replica;
    ASSERT_TRUE(store_test::get_replica(store, bad_id, bad_replica).ok());
    ASSERT_FALSE(bad_replica.is_empty());
    EXPECT_EQ(bad_replica->state.phase, ReplicaPhase::LOST);

    ReplicaOr replacement_replica;
    ASSERT_TRUE(store_test::get_replica(store, replacement_id, replacement_replica).ok());
    ASSERT_FALSE(replacement_replica.is_empty());
    replacement_replica->state.phase = ReplicaPhase::READY;
    ASSERT_TRUE(store_test::put_replica(store, replacement_replica.value()).ok());
    update_replica_observed(store, replacement_id, [](ReplicaObserved& observed) {
        observed.storage_status = StorageReplicaStatus::READY;
        observed.raft_role = ReplicaRole::FOLLOWER;
        observed.member_type = RaftMemberType::NON_MEMBER;
        observed.term = 2;
    });

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
            {},
    });

    HeartBeatResult result;
    ASSERT_TRUE(services.replica_group.build_heartbeat_result(param, result).ok());
    auto add_it = std::find_if(result.expects.begin(), result.expects.end(), [&](const ExpectedReplica& expect) {
        return expect.type == ExpectedReplicaType::ADD_MEMBER && expect.replica_id == replacement_id;
    });
    EXPECT_NE(add_it, result.expects.end());
    auto remove_it = std::find_if(result.expects.begin(), result.expects.end(), [&](const ExpectedReplica& expect) {
        return expect.type == ExpectedReplicaType::REMOVE_MEMBER && expect.replica_id == bad_id;
    });
    EXPECT_EQ(remove_it, result.expects.end());

    update_replica_observed(store, replacement_id, [](ReplicaObserved& observed) {
        observed.member_type = RaftMemberType::VOTER;
    });

    result.expects.clear();
    ASSERT_TRUE(services.replica_group.build_heartbeat_result(param, result).ok());
    remove_it = std::find_if(result.expects.begin(), result.expects.end(), [&](const ExpectedReplica& expect) {
        return expect.type == ExpectedReplicaType::REMOVE_MEMBER && expect.replica_id == bad_id;
    });
    EXPECT_NE(remove_it, result.expects.end());

    param.replica_list[0].full_membership = {
            RaftMember{PeerMember{"node-a", leader_id, Endpoint{"127.0.0.1", 18080}}, RaftMemberType::VOTER},
            RaftMember{PeerMember{replacement_replica->spec.assign_node_id, replacement_id,
                                  get_replica_observed_or_die(store, replacement_id).endpoint},
                       RaftMemberType::VOTER},
    };
    ASSERT_TRUE(services.node.heartbeat(param).ok());

    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    ASSERT_TRUE(store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
    ASSERT_FALSE(group.is_empty());
    EXPECT_EQ(group->desired_members.size(), 2U);
    EXPECT_EQ(std::find(group->desired_members.begin(), group->desired_members.end(), bad_id),
              group->desired_members.end());
    EXPECT_NE(std::find(group->desired_members.begin(), group->desired_members.end(), replacement_id),
              group->desired_members.end());

    ASSERT_TRUE(store_test::get_replica(store, bad_id, bad_replica).ok());
    ASSERT_FALSE(bad_replica.is_empty());
    EXPECT_EQ(bad_replica->state.phase, ReplicaPhase::DELETING);
}

// 检测 3 副本中 1 个节点掉线后，可以补新副本，并通过 leader 上报的
// full membership 把 LOST 老副本从 desired_members 移除。
TEST(TableReconcilerTest, LostReplicaIsReplacedAfterLeaderReportsMembershipWithoutIt) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    put_default_nodes(store);
    ASSERT_TRUE(store_test::put_node(store, make_node("node-c", 18082)).ok());
    ASSERT_TRUE(store_test::put_node(store, make_node("node-d", 18083)).ok());

    FakeNodeSelector selector(&store);
    ServiceHarness services(&store, &selector);
    CreateTableParam param = make_create_table_param();
    param.replica_count = 3;
    ASSERT_TRUE(services.table.create_table(param, nullptr).ok());
    reconcile_table_to_ready(services, store);

    std::vector<Replica> original_replicas = list_replicas_or_die(store);
    ASSERT_EQ(original_replicas.size(), 3U);
    ReplicaID leader_id = original_replicas[0].replica_id;
    ReplicaID bad_id = original_replicas[1].replica_id;
    ReplicaID healthy_follower_id = original_replicas[2].replica_id;
    mark_node_offline(store, original_replicas[1].spec.assign_node_id);

    ASSERT_TRUE(services.replica_group.reconcile_all().ok());

    ReplicaOr bad_replica;
    ASSERT_TRUE(store_test::get_replica(store, bad_id, bad_replica).ok());
    ASSERT_FALSE(bad_replica.is_empty());
    EXPECT_EQ(bad_replica->state.phase, ReplicaPhase::LOST);

    ReplicaGroupOr group;
    ASSERT_TRUE(store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
    ASSERT_FALSE(group.is_empty());
    ASSERT_EQ(group->target_replica_count, 3);
    ASSERT_EQ(group->desired_members.size(), 4U);

    ReplicaID replacement_id;
    bool found_replacement = false;
    for (const ReplicaID& rid : group->desired_members) {
        if (rid != leader_id && rid != bad_id && rid != healthy_follower_id) {
            replacement_id = rid;
            found_replacement = true;
            break;
        }
    }
    ASSERT_TRUE(found_replacement);

    ReplicaOr replacement;
    ASSERT_TRUE(store_test::get_replica(store, replacement_id, replacement).ok());
    ASSERT_FALSE(replacement.is_empty());
    replacement->state.phase = ReplicaPhase::READY;
    ASSERT_TRUE(store_test::put_replica(store, replacement.value()).ok());
    update_replica_observed(store, replacement_id, [](ReplicaObserved& observed) {
        observed.storage_status = StorageReplicaStatus::READY;
        observed.raft_role = ReplicaRole::FOLLOWER;
        observed.member_type = RaftMemberType::NON_MEMBER;
        observed.endpoint = Endpoint{"127.0.0.1", 18083};
        observed.term = 2;
    });

    HeartBeatParam hb;
    hb.node_id = original_replicas[0].spec.assign_node_id;
    hb.ip = "127.0.0.1";
    hb.port = 18080;
    hb.resoure_pool_name = "pool-a";
    hb.dc = "dc-a";
    hb.last_heartbeat_ts = 5000;
    hb.replica_list.push_back(HeartBeatReplicaInfo{
            leader_id,
            ReplicaRole::LEADER,
            StorageReplicaStatus::READY,
            2,
            {},
    });

    HeartBeatResult result;
    ASSERT_TRUE(services.replica_group.build_heartbeat_result(hb, result).ok());
    auto add_it = std::find_if(result.expects.begin(), result.expects.end(), [&](const ExpectedReplica& expect) {
        return expect.type == ExpectedReplicaType::ADD_MEMBER && expect.replica_id == replacement_id;
    });
    ASSERT_NE(add_it, result.expects.end());

    update_replica_observed(store, replacement_id, [](ReplicaObserved& observed) {
        observed.member_type = RaftMemberType::VOTER;
    });

    result.expects.clear();
    ASSERT_TRUE(services.replica_group.build_heartbeat_result(hb, result).ok());
    auto remove_it = std::find_if(result.expects.begin(), result.expects.end(), [&](const ExpectedReplica& expect) {
        return expect.type == ExpectedReplicaType::REMOVE_MEMBER && expect.replica_id == bad_id;
    });
    ASSERT_NE(remove_it, result.expects.end());

    ReplicaOr healthy_follower;
    ASSERT_TRUE(store_test::get_replica(store, healthy_follower_id, healthy_follower).ok());
    ASSERT_FALSE(healthy_follower.is_empty());
    hb.replica_list[0].full_membership = {
            RaftMember{PeerMember{original_replicas[0].spec.assign_node_id, leader_id,
                                  get_replica_observed_or_die(store, leader_id).endpoint},
                       RaftMemberType::VOTER},
            RaftMember{PeerMember{healthy_follower->spec.assign_node_id, healthy_follower_id,
                                  get_replica_observed_or_die(store, healthy_follower_id).endpoint},
                       RaftMemberType::VOTER},
            RaftMember{
                    PeerMember{replacement->spec.assign_node_id, replacement_id,
                               get_replica_observed_or_die(store, replacement_id).endpoint},
                    RaftMemberType::VOTER},
    };
    ASSERT_TRUE(services.node.heartbeat(hb).ok());

    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    ASSERT_TRUE(store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
    ASSERT_FALSE(group.is_empty());
    EXPECT_EQ(group->desired_members.size(), 3U);
    EXPECT_EQ(std::find(group->desired_members.begin(), group->desired_members.end(), bad_id),
              group->desired_members.end());
    EXPECT_NE(std::find(group->desired_members.begin(), group->desired_members.end(), replacement_id),
              group->desired_members.end());

    ASSERT_TRUE(store_test::get_replica(store, bad_id, bad_replica).ok());
    ASSERT_FALSE(bad_replica.is_empty());
    EXPECT_EQ(bad_replica->state.phase, ReplicaPhase::DELETING);

    ASSERT_TRUE(services.route.reconcile_all().ok());
    ShardRouteOr route;
    ASSERT_TRUE(store_test::get_shard_route(store, ShardID{1001, 0}, route).ok());
    ASSERT_FALSE(route.is_empty());
    EXPECT_EQ(route->replicas.size(), 3U);
    auto route_has_replica = [&](const ReplicaID& rid) {
        return std::find_if(route->replicas.begin(), route->replicas.end(),
                            [&](const RouteReplica& r) { return r.replica_id == rid; }) != route->replicas.end();
    };
    EXPECT_TRUE(route_has_replica(leader_id));
    EXPECT_TRUE(route_has_replica(healthy_follower_id));
    EXPECT_TRUE(route_has_replica(replacement_id));
    EXPECT_FALSE(route_has_replica(bad_id));
}

// 检测 READY 表发生内部副本替换时仍保持 READY，并且外部 alter 仍可进入 RESIZING。
TEST(TableReconcilerTest, ReplacementKeepsTableReadyAndAllowsAlter) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    put_default_nodes(store);
    ASSERT_TRUE(store_test::put_node(store, make_node("node-c", 18082)).ok());
    ASSERT_TRUE(store_test::put_node(store, make_node("node-d", 18083)).ok());

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

    Status alter_status =
            services.table.alter_table_replica_count(AlterTableReplicaCountParam{"commerce", "orders", 3}, nullptr);
    ASSERT_TRUE(alter_status.ok()) << alter_status.to_string();
    EXPECT_EQ(get_table_or_die(store).state.phase, TablePhase::RESIZING);

    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    ReplicaGroupOr group;
    ASSERT_TRUE(store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
    ASSERT_FALSE(group.is_empty());
    EXPECT_EQ(group->target_replica_count, 3);
    EXPECT_EQ(group->desired_members.size(), 4U);
}

// 检测缩容流程：alter 减少副本数 → victim 变成 NON_MEMBER → reconciler 将其从 desired_members 移出并标 DELETING。
TEST(TableReconcilerTest, ReplicaGroupShrinksAfterObservedMemberRemoved) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    put_default_nodes(store);
    ASSERT_TRUE(store_test::put_node(store, make_node("node-c", 18082)).ok());

    FakeNodeSelector selector(&store);
    ServiceHarness services(&store, &selector);
    CreateTableParam param = make_create_table_param();
    param.replica_count = 3;
    ASSERT_TRUE(services.table.create_table(param, nullptr).ok());
    reconcile_table_to_ready(services, store);

    ASSERT_TRUE(services.table.alter_table_replica_count(AlterTableReplicaCountParam{"commerce", "orders", 2}, nullptr)
                        .ok());
    ASSERT_TRUE(services.replica_group.reconcile_all().ok());

    ReplicaGroupOr group;
    ASSERT_TRUE(store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
    ASSERT_FALSE(group.is_empty());
    ASSERT_EQ(group->desired_members.size(), 3U);
    EXPECT_EQ(group->target_replica_count, 2);
    ReplicaID leader_id = group->desired_members.front();
    ReplicaID victim_id = group->desired_members.back();

    ReplicaOr victim;
    ASSERT_TRUE(store_test::get_replica(store, victim_id, victim).ok());
    ASSERT_FALSE(victim.is_empty());
    update_replica_observed(store, victim_id, [](ReplicaObserved& observed) {
        observed.member_type = RaftMemberType::NON_MEMBER;
    });

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
            {},
    });

    HeartBeatResult result;
    ASSERT_TRUE(services.replica_group.build_heartbeat_result(hb, result).ok());
    auto add_victim_it = std::find_if(result.expects.begin(), result.expects.end(), [&](const ExpectedReplica& expect) {
        return expect.type == ExpectedReplicaType::ADD_MEMBER && expect.replica_id == victim_id;
    });
    EXPECT_EQ(add_victim_it, result.expects.end());

    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    ASSERT_TRUE(store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
    ASSERT_FALSE(group.is_empty());
    EXPECT_EQ(group->desired_members.size(), 2U);
    EXPECT_EQ(std::find(group->desired_members.begin(), group->desired_members.end(), victim_id),
              group->desired_members.end());

    ASSERT_TRUE(store_test::get_replica(store, victim_id, victim).ok());
    ASSERT_FALSE(victim.is_empty());
    EXPECT_EQ(victim->state.phase, ReplicaPhase::DELETING);
}

// 检测 READY 表可以缩到 0 副本，清理路由后重新进入 READY。
TEST(TableReconcilerTest, ReadyTableCanShrinkToZeroWithoutRoute) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    put_default_nodes(store);

    FakeNodeSelector selector(&store);
    ServiceHarness services(&store, &selector);
    place_default_table(services);
    reconcile_table_to_ready(services, store);

    Status alter_status =
            services.table.alter_table_replica_count(AlterTableReplicaCountParam{"commerce", "orders", 0}, nullptr);
    ASSERT_TRUE(alter_status.ok()) << alter_status.to_string();
    EXPECT_EQ(get_table_or_die(store).state.phase, TablePhase::RESIZING);

    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    ASSERT_TRUE(services.route.reconcile_all().ok());
    ASSERT_TRUE(services.table.reconcile_all().ok());
    EXPECT_EQ(get_table_or_die(store).state.phase, TablePhase::RESIZING);

    std::vector<Replica> replicas = list_replicas_or_die(store);
    ASSERT_EQ(replicas.size(), 2U);
    for (const Replica& replica : replicas) {
        EXPECT_EQ(replica.state.phase, ReplicaPhase::DELETING);
    }

    ShardRouteOr route;
    ASSERT_TRUE(store_test::get_shard_route(store, ShardID{1001, 0}, route).ok());
    EXPECT_TRUE(route.is_empty());

    for (Replica& replica : replicas) {
        replica.state.phase = ReplicaPhase::DELETED;
        ASSERT_TRUE(store_test::put_replica(store, replica).ok());
    }

    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    ASSERT_TRUE(services.route.reconcile_all().ok());
    ASSERT_TRUE(services.table.reconcile_all().ok());

    Table table = get_table_or_die(store);
    EXPECT_EQ(table.spec.target_replica_count, 0);
    EXPECT_EQ(table.state.phase, TablePhase::READY);
    EXPECT_TRUE(list_replicas_or_die(store).empty());

    ReplicaGroupOr group;
    ASSERT_TRUE(store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
    ASSERT_FALSE(group.is_empty());
    EXPECT_EQ(group->mode, ReplicaGroupMode::BOOTSTRAP);
    EXPECT_EQ(group->target_replica_count, 0);
    EXPECT_TRUE(group->desired_members.empty());
    std::vector<ShardRoute> routes;
    EXPECT_EQ(services.route.get_table_routes(make_get_table_routes_param(), nullptr, &routes).code(),
              StatusCode::ROUTE_NOT_FOUND);
}

// 检测 Replica 出现 ERROR 时，Table 继续等待
// ReplicaGroupMembershipReconciler 替换坏副本。
TEST(TableReconcilerTest, ReplicaErrorKeepsTableWaiting) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    put_default_nodes(store);

    FakeNodeSelector selector(&store);
    ServiceHarness services(&store, &selector);
    place_default_table(services);
    ASSERT_TRUE(services.table.reconcile_all().ok());
    ASSERT_TRUE(services.replica_group.reconcile_all().ok());

    std::vector<Replica> replicas = list_replicas_or_die(store);
    ASSERT_EQ(replicas.size(), 2U);
    replicas[0].state.phase = ReplicaPhase::ERROR;
    ASSERT_TRUE(store_test::put_replica(store, replicas[0]).ok());

    ASSERT_TRUE(services.table.reconcile_all().ok());

    expect_table_phase_and_msg(store, TablePhase::CREATING, "");
    EXPECT_TRUE(get_table_or_die(store).state.last_error_msg.empty());
}

// 检测路由中有多个 leader 时，Table 不会变成 READY。
TEST(TableReconcilerTest, TableDoesNotBecomeReadyWhenRouteHasMultipleLeaders) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    put_default_nodes(store);

    FakeNodeSelector selector(&store);
    ServiceHarness services(&store, &selector);
    place_default_table(services);

    ASSERT_TRUE(services.table.reconcile_all().ok());
    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    make_replicas_ready(store);
    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    ASSERT_TRUE(services.table.reconcile_all().ok());

    put_route(store, {RouteReplica{ReplicaID{1001, 0, 0}, Endpoint{"127.0.0.1", 18080}, ReplicaRole::LEADER},
                      RouteReplica{ReplicaID{1001, 0, 1}, Endpoint{"127.0.0.1", 18081}, ReplicaRole::LEADER}});

    ASSERT_TRUE(services.table.reconcile_all().ok());
    EXPECT_EQ(get_table_or_die(store).state.phase, TablePhase::CREATING);
}

// 检测 leader 的 endpoint 无效时，Table 不会变成 READY。
TEST(TableReconcilerTest, TableDoesNotBecomeReadyWhenLeaderEndpointInvalid) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    put_default_nodes(store);

    FakeNodeSelector selector(&store);
    ServiceHarness services(&store, &selector);
    place_default_table(services);

    ASSERT_TRUE(services.table.reconcile_all().ok());
    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    make_replicas_ready(store);
    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    ASSERT_TRUE(services.table.reconcile_all().ok());

    put_route(store, {RouteReplica{ReplicaID{1001, 0, 0}, Endpoint{"", 0}, ReplicaRole::LEADER},
                      RouteReplica{ReplicaID{1001, 0, 1}, Endpoint{"127.0.0.1", 18081}, ReplicaRole::FOLLOWER}});

    ASSERT_TRUE(services.table.reconcile_all().ok());
    EXPECT_EQ(get_table_or_die(store).state.phase, TablePhase::CREATING);
}

// 检测 leader 丢失后路由被删除，心跳恢复新 leader 后路由重新发布。
TEST(TableReconcilerTest, RouteIsRepublishedAfterHeartbeatRecoversLeader) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    put_default_nodes(store);

    FakeNodeSelector selector(&store);
    ServiceHarness services(&store, &selector);
    place_default_table(services);
    reconcile_table_to_ready(services, store);

    RouteService route_service(&store);
    std::vector<ShardRoute> routes;
    Status status = route_service.get_table_routes(make_get_table_routes_param(), nullptr, &routes);
    ASSERT_TRUE(status.ok()) << status.msg();
    ASSERT_EQ(routes.size(), 1U);
    ASSERT_EQ(routes[0].replicas.size(), 2U);
    EXPECT_EQ(routes[0].replicas[0].role, ReplicaRole::LEADER);

    std::vector<Replica> replicas = list_replicas_or_die(store);
    ASSERT_EQ(replicas.size(), 2U);
    for (const Replica& replica : replicas) {
        update_replica_observed(store, replica.replica_id, [](ReplicaObserved& observed) {
            observed.raft_role = ReplicaRole::FOLLOWER;
            observed.term = 8;
        });
    }

    ASSERT_TRUE(services.route.reconcile_all().ok());

    ShardRouteOr route_after_loss;
    ASSERT_TRUE(store_test::get_shard_route(store, ShardID{1001, 0}, route_after_loss).ok());
    EXPECT_TRUE(route_after_loss.is_empty());
    status = route_service.get_table_routes(make_get_table_routes_param(), nullptr, &routes);
    EXPECT_EQ(status.code(), StatusCode::ROUTE_NOT_FOUND);

    report_replica_heartbeat(services, "node-a", 18080, 0, ReplicaRole::LEADER, StorageReplicaStatus::READY, 11);
    report_replica_heartbeat(services, "node-b", 18081, 1, ReplicaRole::FOLLOWER, StorageReplicaStatus::READY, 11);

    ASSERT_TRUE(services.route.reconcile_all().ok());

    status = route_service.get_table_routes(make_get_table_routes_param(), nullptr, &routes);
    ASSERT_TRUE(status.ok()) << status.msg();
    ASSERT_EQ(routes.size(), 1U);
    ASSERT_EQ(routes[0].replicas.size(), 2U);
    EXPECT_EQ(routes[0].replicas[0].replica_id.replica_seq, 0);
    EXPECT_EQ(routes[0].replicas[0].role, ReplicaRole::LEADER);
    EXPECT_EQ(routes[0].replicas[0].endpoint, (Endpoint{"127.0.0.1", 18080}));
    EXPECT_EQ(routes[0].replicas[1].replica_id.replica_seq, 1);
    EXPECT_EQ(routes[0].replicas[1].role, ReplicaRole::FOLLOWER);

    ASSERT_TRUE(services.table.reconcile_all().ok());
    EXPECT_EQ(get_table_or_die(store).state.phase, TablePhase::READY);
}

// 检测删表流程：标记 ABSENT → 清理 ReplicaGroup → 删除路由 → Replica 物理删除 → Table 变 DELETED。
TEST(TableReconcilerTest, DropTableWaitsForReplicaGroupCleanup) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    put_default_nodes(store);

    FakeNodeSelector selector(&store);
    ServiceHarness services(&store, &selector);
    place_default_table(services);
    reconcile_table_to_ready(services, store);
    ShardRouteOr route_before;
    ASSERT_TRUE(store_test::get_shard_route(store, ShardID{1001, 0}, route_before).ok());
    ASSERT_FALSE(route_before.is_empty());

    mark_table_absent(services.table);

    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    ASSERT_TRUE(services.route.reconcile_all().ok());
    ASSERT_TRUE(services.table.reconcile_all().ok());

    Table table = get_table_or_die(store);
    EXPECT_EQ(table.state.phase, TablePhase::DELETING);

    std::vector<Replica> replicas = list_replicas_or_die(store);
    ASSERT_EQ(replicas.size(), 2U);
    for (const Replica& replica : replicas) {
        EXPECT_EQ(replica.state.phase, ReplicaPhase::DELETING);
    }

    ReplicaGroupOr group;
    ASSERT_TRUE(store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
    ASSERT_FALSE(group.is_empty());
    EXPECT_EQ(group->mode, ReplicaGroupMode::RAFT_RECONFIG);
    EXPECT_EQ(group->target_replica_count, 0);
    EXPECT_TRUE(group->desired_members.empty());

    ShardRouteOr route;
    ASSERT_TRUE(store_test::get_shard_route(store, ShardID{1001, 0}, route).ok());
    EXPECT_TRUE(route.is_empty());

    for (Replica& replica : replicas) {
        replica.state.phase = ReplicaPhase::DELETED;
        ASSERT_TRUE(store_test::put_replica(store, replica).ok());
    }

    ASSERT_TRUE(services.replica_group.reconcile_all().ok());
    ASSERT_TRUE(services.route.reconcile_all().ok());
    ASSERT_TRUE(services.table.reconcile_all().ok());

    table = get_table_or_die(store);
    EXPECT_EQ(table.state.phase, TablePhase::DELETED);
    EXPECT_TRUE(list_replicas_or_die(store).empty());
    ASSERT_TRUE(store_test::get_shard_route(store, ShardID{1001, 0}, route).ok());
    EXPECT_TRUE(route.is_empty());
    ASSERT_TRUE(store_test::get_replica_group(store, ShardID{1001, 0}, group).ok());
    EXPECT_TRUE(group.is_empty());
}


}  // namespace adviskv::meta
