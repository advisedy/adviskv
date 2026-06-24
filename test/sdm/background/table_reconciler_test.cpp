#include "sdm/background/table_reconciler.h"

#include <gtest/gtest.h>

#include <deque>
#include <optional>
#include <string>
#include <vector>

#include "common/model/storage_replica_status.h"
#include "common/status.h"
#include "sdm/background/routeupdate_check_task.h"
#include "sdm/model/sdm_store.h"
#include "sdm/selector/node_selector/node_selector.h"
#include "sdm/service/heartbeat_service.h"
#include "sdm/service/route_service.h"
#include "sdm/service/table_service.h"

namespace adviskv::sdm {
namespace {

class FakeStorageClient : public IStorageClient {
   public:
    Status create_replica(const CreateReplicaParam& param) override {
        created.push_back(param);
        if (!create_statuses.empty()) {
            Status status = create_statuses.front();
            create_statuses.pop_front();
            return status;
        }
        return Status::OK();
    }

    Status delete_replica(const DeleteReplicaParam& param) override {
        deleted.push_back(param);
        if (!delete_statuses.empty()) {
            Status status = delete_statuses.front();
            delete_statuses.pop_front();
            return status;
        }
        return Status::OK();
    }

    Status get_replica_info(const GetReplicaInfoParam& param,
                            StorageReplicaInfo& out, bool& exists) override {
        got_infos.push_back(param);
        if (!get_info_statuses.empty()) {
            Status status = get_info_statuses.front();
            get_info_statuses.pop_front();
            return status;
        }
        for (const StorageReplicaInfo& info : replica_infos) {
            if (info.replica_id == param.replica_id) {
                out = info;
                exists = true;
                return Status::OK();
            }
        }
        exists = false;
        return Status::OK();
    }

    std::deque<Status> create_statuses;
    std::deque<Status> delete_statuses;
    std::deque<Status> get_info_statuses;
    std::vector<CreateReplicaParam> created;
    std::vector<DeleteReplicaParam> deleted;
    std::vector<GetReplicaInfoParam> got_infos;
    std::vector<StorageReplicaInfo> replica_infos;
};

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

class TestTableReconciler : public TableReconciler {
   public:
    using TableReconciler::TableReconciler;
};

Node make_node(const NodeID& id, int32_t port) {
    return Node{id,
                NodeSpec{"pool-a", "dc-a", NodeStatus::ONLINE},
                NodeState{Endpoint{"127.0.0.1", port}, 1},
                NodeDerived{}};
}

PlaceTableParam make_place_table_param() {
    return PlaceTableParam{11, 1001, "commerce", "orders", 2, 1, "pool-a",
                           "create-1001"};
}

void put_default_nodes(SdmStore& store) {
    ASSERT_TRUE(store.put_node(make_node("node-a", 18080)).ok());
    ASSERT_TRUE(store.put_node(make_node("node-b", 18081)).ok());
}

void place_default_table(SdmStore& store) {
    TableService table_service(&store);
    ASSERT_TRUE(table_service.place_table(make_place_table_param()).ok());
}

Table get_table_or_die(SdmStore& store) {
    TableOr table;
    EXPECT_TRUE(store.get_table(1001, table).ok());
    EXPECT_FALSE(table.is_empty());
    return *table;
}

std::vector<Replica> list_replicas_or_die(SdmStore& store) {
    std::vector<Replica> replicas;
    EXPECT_TRUE(store.list_replicas_by_shard(ShardID{1001, 0}, replicas).ok());
    return replicas;
}

// 这个是设置storage_client在被调用到get_replica_info的时候会返回成功的数据
void make_table_ready_in_storage(SdmStore& store, FakeStorageClient& storage) {
    std::vector<Replica> replicas = list_replicas_or_die(store);
    for (const Replica& replica : replicas) {
        storage.replica_infos.push_back(StorageReplicaInfo{
            replica.replica_id,
            replica.replica_id.replica_seq == 0 ? ReplicaRole::LEADER
                                                  : ReplicaRole::FOLLOWER,
            StorageReplicaStatus::READY,
            replica.state.observed_endpoint,
            1,
        });
    }
}

void reconcile_table_to_ready(SdmStore& store, FakeStorageClient& storage,
                              TestTableReconciler& reconciler) {
    ASSERT_TRUE(reconciler.reconcile_once().ok());
    make_table_ready_in_storage(store, storage);
    RouteUpdateCheckTask route_task(&store);
    ASSERT_TRUE(reconciler.reconcile_once().ok());
    ASSERT_TRUE(route_task.update_once().ok());
    ASSERT_TRUE(reconciler.reconcile_once().ok());
    EXPECT_EQ(get_table_or_die(store).state.phase, TablePhase::READY);
}

void mark_table_absent(SdmStore& store) {
    Table table = get_table_or_die(store);
    table.state.desired = TableDesired::ABSENT;
    table.state.phase = TablePhase::DELETING;
    table.spec.operation_id = "drop-1001";
    table.state.last_error_msg.clear();
    ASSERT_TRUE(store.put_table(table).ok());
}

void expect_table_phase_and_msg(SdmStore& store, TablePhase phase,
                                const std::string& msg) {
    Table table = get_table_or_die(store);
    EXPECT_EQ(table.state.phase, phase);
    EXPECT_NE(table.state.last_error_msg.find(msg), std::string::npos);
}

void put_route(SdmStore& store, std::vector<RouteEntry> replicas) {
    ShardRoute route;
    route.shard_id = ShardID{1001, 0};
    route.replicas = std::move(replicas);
    ASSERT_TRUE(store.put_shard_route(route).ok());
}

GetRouteParam make_get_route_param(const Key& key = "user-123") {
    return GetRouteParam{"commerce", "orders", key};
}

void report_replica_heartbeat(SdmStore& store, const NodeID& node_id,
                              int32_t port, ReplicaIndex replica_index,
                              ReplicaRole role, StorageReplicaStatus status,
                              Term term) {
    HeartBeatService heartbeat_service(&store);
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
    });
    ASSERT_TRUE(heartbeat_service.heartbeat(param).ok());
}

}  // namespace

// 创建table的正常完整流程可以走完一遍
TEST(TableReconcilerTest, CreateTableMaterializesReplicasAndReachesReady) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_default_nodes(store);
    place_default_table(store);

    FakeStorageClient storage_client;
    FakeNodeSelector selector(&store);
    TestTableReconciler reconciler(&store, &storage_client, &selector);

    // 走这个第一波，然后把那个Replica的这个状态就会更新到这个CREATING
    ASSERT_TRUE(reconciler.reconcile_once().ok());

    EXPECT_EQ(storage_client.created.size(), 2U);
    make_table_ready_in_storage(store, storage_client);

    RouteUpdateCheckTask route_task(&store);

    // 然后第二波这个Replica就会更新成了这个Ready
    ASSERT_TRUE(reconciler.reconcile_once().ok());

    // Replica更新OK之后，这个RouteChecker就会把这个Route给更新出来。
    ASSERT_TRUE(route_task.update_once().ok());

    // 之后这个Table就可以去创建出来了。
    ASSERT_TRUE(reconciler.reconcile_once().ok());

    EXPECT_EQ(get_table_or_die(store).state.phase, TablePhase::READY);
}

// 创建table的时候node选择失败，table应该会继续维持CREATING重试
TEST(TableReconcilerTest, CreateTableSelectorErrorMarksTableFailed) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_default_nodes(store);
    place_default_table(store);

    FakeStorageClient storage_client;
    FakeNodeSelector selector(&store);
    selector.statuses.push_back(Status::ERROR("select node failed"));
    TestTableReconciler reconciler(&store, &storage_client, &selector);

    ASSERT_TRUE(reconciler.reconcile_once().ok());

    expect_table_phase_and_msg(store, TablePhase::FAILED, "select node failed");

    EXPECT_TRUE(storage_client.created.empty());  // 因为还没有调用到他那一块。
}

// 创建table的时候资源池节点不足，table还是CREATING并记录错误等待重试
TEST(TableReconcilerTest, CreateTableNotEnoughNodesKeepsCreating) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store.put_node(make_node("node-a", 18080)).ok());
    place_default_table(store);

    FakeStorageClient storage_client;
    FakeNodeSelector selector(&store);
    TestTableReconciler reconciler(&store, &storage_client, &selector);

    ASSERT_TRUE(reconciler.reconcile_once().ok());

    expect_table_phase_and_msg(store, TablePhase::CREATING, "not enough nodes");
    EXPECT_TRUE(storage_client.created.empty());
}

// storage 创建replica返回可重试错误，table还是CREATING并记录错误等待重试
// 然后第二次replica可以创建了
TEST(TableReconcilerTest, CreateReplicaRetryableErrorKeepsCreating) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_default_nodes(store);
    place_default_table(store);

    FakeStorageClient storage_client;
    storage_client.create_statuses.push_back(
        Status::RPC_ERROR("storage rpc failed"));
    FakeNodeSelector selector(&store);
    TestTableReconciler reconciler(&store, &storage_client, &selector);

    ASSERT_TRUE(reconciler.reconcile_once().ok());

    expect_table_phase_and_msg(store, TablePhase::CREATING,
                               "storage rpc failed");
    {
        std::vector<Replica> replicas = list_replicas_or_die(store);
        ASSERT_EQ(replicas.size(), 2U);
        EXPECT_EQ(replicas[0].state.phase, ReplicaPhase::CREATING);
        EXPECT_NE(replicas[0].state.last_error_msg.find("storage rpc failed"),
                  std::string::npos);
        EXPECT_EQ(replicas[1].state.phase, ReplicaPhase::PENDING);
    }
    {
        storage_client.create_statuses.push_back(Status::OK());
        storage_client.create_statuses.push_back(Status::OK());
        ASSERT_TRUE(reconciler.reconcile_once().ok());
        std::vector<Replica> replicas = list_replicas_or_die(store);
        ASSERT_EQ(replicas.size(), 2U);
        EXPECT_EQ(replicas[0].state.phase, ReplicaPhase::CREATING);  //
        EXPECT_EQ(replicas[1].state.phase, ReplicaPhase::CREATING);
    }
    {
        make_table_ready_in_storage(store, storage_client);
        ASSERT_TRUE(reconciler.reconcile_once().ok());

        std::vector<Replica> replicas = list_replicas_or_die(store);
        ASSERT_EQ(replicas.size(), 2U);
        EXPECT_EQ(replicas[0].state.phase, ReplicaPhase::READY);
        EXPECT_EQ(replicas[1].state.phase, ReplicaPhase::READY);
    }
}

// 创建replica返回不可重试错误，table进入FAILED，replica进入ERROR
TEST(TableReconcilerTest, CreateReplicaFatalErrorMarksFailed) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_default_nodes(store);
    place_default_table(store);

    FakeStorageClient storage_client;
    storage_client.create_statuses.push_back(
        Status::INVALID_ARGUMENT("bad create param"));
    FakeNodeSelector selector(&store);
    TestTableReconciler reconciler(&store, &storage_client, &selector);

    ASSERT_TRUE(reconciler.reconcile_once().ok());

    expect_table_phase_and_msg(store, TablePhase::FAILED, "bad create param");
    std::vector<Replica> replicas = list_replicas_or_die(store);
    ASSERT_EQ(replicas.size(), 2U);
    EXPECT_EQ(replicas[0].state.phase, ReplicaPhase::ERROR);
    EXPECT_NE(replicas[0].state.last_error_msg.find("bad create param"),
              std::string::npos);
}

// 拉取replica信息返回可重试错误，table还是CREATING并记录错误等待重试
TEST(TableReconcilerTest, GetReplicaInfoRetryableErrorKeepsCreating) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_default_nodes(store);
    place_default_table(store);

    FakeStorageClient storage_client;
    FakeNodeSelector selector(&store);
    TestTableReconciler reconciler(&store, &storage_client, &selector);
    ASSERT_TRUE(reconciler.reconcile_once().ok());
    storage_client.get_info_statuses.push_back(
        Status::RPC_ERROR("get info rpc failed"));

    ASSERT_TRUE(reconciler.reconcile_once().ok());

    expect_table_phase_and_msg(store, TablePhase::CREATING,
                               "get info rpc failed");
    std::vector<Replica> replicas = list_replicas_or_die(store);
    ASSERT_EQ(replicas.size(), 2U);
    EXPECT_EQ(replicas[0].state.phase, ReplicaPhase::CREATING);
}

// 拉取replica信息返回不可重试错误，table进入FAILED，replica进入ERROR
TEST(TableReconcilerTest, GetReplicaInfoFatalErrorMarksFailed) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_default_nodes(store);
    place_default_table(store);

    FakeStorageClient storage_client;
    FakeNodeSelector selector(&store);
    TestTableReconciler reconciler(&store, &storage_client, &selector);
    ASSERT_TRUE(reconciler.reconcile_once().ok());
    storage_client.get_info_statuses.push_back(
        Status::INVALID_ARGUMENT("bad get info param"));

    ASSERT_TRUE(reconciler.reconcile_once().ok());

    expect_table_phase_and_msg(store, TablePhase::FAILED, "bad get info param");
    std::vector<Replica> replicas = list_replicas_or_die(store);
    ASSERT_EQ(replicas.size(), 2U);
    EXPECT_EQ(replicas[0].state.phase, ReplicaPhase::ERROR);
}

// 即使 replica 都 READY 了，只要 route 里存在多个 leader，table 也不能进入 READY。
TEST(TableReconcilerTest, TableDoesNotBecomeReadyWhenRouteHasMultipleLeaders) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_default_nodes(store);
    place_default_table(store);

    FakeStorageClient storage_client;
    FakeNodeSelector selector(&store);
    TestTableReconciler reconciler(&store, &storage_client, &selector);

    ASSERT_TRUE(reconciler.reconcile_once().ok());
    make_table_ready_in_storage(store, storage_client);
    ASSERT_TRUE(reconciler.reconcile_once().ok());

    put_route(store,
              {RouteEntry{ReplicaID{1001, 0, 0}, "node-a", "127.0.0.1", 18080,
                          ReplicaRole::LEADER, 5},
               RouteEntry{ReplicaID{1001, 0, 1}, "node-b", "127.0.0.1", 18081,
                          ReplicaRole::LEADER, 4}});

    ASSERT_TRUE(reconciler.reconcile_once().ok());
    EXPECT_EQ(get_table_or_die(store).state.phase, TablePhase::CREATING);
}

// 如果唯一 leader 的 endpoint 非法，那么这张表也不能对外视为 READY。
TEST(TableReconcilerTest, TableDoesNotBecomeReadyWhenLeaderEndpointInvalid) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_default_nodes(store);
    place_default_table(store);

    FakeStorageClient storage_client;
    FakeNodeSelector selector(&store);
    TestTableReconciler reconciler(&store, &storage_client, &selector);

    ASSERT_TRUE(reconciler.reconcile_once().ok());
    make_table_ready_in_storage(store, storage_client);
    ASSERT_TRUE(reconciler.reconcile_once().ok());

    put_route(store,
              {RouteEntry{ReplicaID{1001, 0, 0}, "node-a", "", 0,
                          ReplicaRole::LEADER, 5},
               RouteEntry{ReplicaID{1001, 0, 1}, "node-b", "127.0.0.1", 18081,
                          ReplicaRole::FOLLOWER, 5}});

    ASSERT_TRUE(reconciler.reconcile_once().ok());
    EXPECT_EQ(get_table_or_die(store).state.phase, TablePhase::CREATING);
}

// route 暂时因为无 leader 被撤掉之后，heartbeat 恢复 observed leader，
// RouteUpdateCheckTask 应该重新发布 route，GetRoute 也应该恢复成功。
TEST(TableReconcilerTest, RouteIsRepublishedAfterHeartbeatRecoversLeader) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_default_nodes(store);
    place_default_table(store);

    FakeStorageClient storage_client;
    FakeNodeSelector selector(&store);
    TestTableReconciler reconciler(&store, &storage_client, &selector);
    reconcile_table_to_ready(store, storage_client, reconciler);

    RouteService route_service(&store);
    RouteUpdateCheckTask route_task(&store);
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
        ASSERT_TRUE(store.put_replica(replica).ok());
    }

    ASSERT_TRUE(route_task.update_once().ok());

    ShardRouteOr route_after_loss;
    ASSERT_TRUE(store.get_shard_route(ShardID{1001, 0}, route_after_loss).ok());
    EXPECT_TRUE(route_after_loss.is_empty());
    status = route_service.get_route(make_get_route_param(), &route);
    EXPECT_EQ(status.code(), StatusCode::ROUTE_NOT_FOUND);

    report_replica_heartbeat(store, "node-a", 18080, 0, ReplicaRole::LEADER,
                             StorageReplicaStatus::READY, 11);
    report_replica_heartbeat(store, "node-b", 18081, 1,
                             ReplicaRole::FOLLOWER, StorageReplicaStatus::READY,
                             11);

    ASSERT_TRUE(route_task.update_once().ok());

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

    ASSERT_TRUE(reconciler.reconcile_once().ok());
    EXPECT_EQ(get_table_or_die(store).state.phase, TablePhase::READY);
}

// 删除table的正常完整流程可以走完一遍
TEST(TableReconcilerTest, DropTableDeletesRoutesStorageAndReplicaMetadata) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_default_nodes(store);
    place_default_table(store);

    FakeStorageClient storage_client;
    FakeNodeSelector selector(&store);
    TestTableReconciler reconciler(&store, &storage_client, &selector);
    reconcile_table_to_ready(store, storage_client, reconciler);
    ShardRouteOr route_before;
    ASSERT_TRUE(store.get_shard_route(ShardID{1001, 0}, route_before).ok());
    ASSERT_FALSE(route_before.is_empty());

    mark_table_absent(store);

    ASSERT_TRUE(reconciler.reconcile_once().ok());

    Table table = get_table_or_die(store);
    EXPECT_EQ(table.state.desired, TableDesired::ABSENT);
    EXPECT_EQ(table.state.phase, TablePhase::DELETED);
    EXPECT_EQ(storage_client.deleted.size(), 2U);
    EXPECT_TRUE(list_replicas_or_die(store).empty());
    ShardRouteOr route;
    ASSERT_TRUE(store.get_shard_route(ShardID{1001, 0}, route).ok());
    EXPECT_TRUE(route.is_empty());
}

// 删除replica返回可重试错误，table还是DELETING并记录错误等待重试
TEST(TableReconcilerTest, DeleteReplicaRetryableErrorKeepsDeleting) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_default_nodes(store);
    place_default_table(store);

    FakeStorageClient storage_client;
    FakeNodeSelector selector(&store);
    TestTableReconciler reconciler(&store, &storage_client, &selector);
    reconcile_table_to_ready(store, storage_client, reconciler);
    mark_table_absent(store);
    storage_client.delete_statuses.push_back(
        Status::RPC_ERROR("delete rpc failed"));

    ASSERT_TRUE(reconciler.reconcile_once().ok());

    expect_table_phase_and_msg(store, TablePhase::DELETING,
                               "delete rpc failed");
    std::vector<Replica> replicas = list_replicas_or_die(store);
    ASSERT_EQ(replicas.size(), 2U);
    EXPECT_EQ(replicas[0].state.phase, ReplicaPhase::DELETING);
    EXPECT_NE(replicas[0].state.last_error_msg.find("delete rpc failed"),
              std::string::npos);
}

// 删除replica返回不可重试错误，table进入FAILED，replica进入ERROR
TEST(TableReconcilerTest, DeleteReplicaFatalErrorMarksFailed) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_default_nodes(store);
    place_default_table(store);

    FakeStorageClient storage_client;
    FakeNodeSelector selector(&store);
    TestTableReconciler reconciler(&store, &storage_client, &selector);
    reconcile_table_to_ready(store, storage_client, reconciler);
    mark_table_absent(store);
    storage_client.delete_statuses.push_back(
        Status::INVALID_ARGUMENT("bad delete param"));

    ASSERT_TRUE(reconciler.reconcile_once().ok());

    expect_table_phase_and_msg(store, TablePhase::FAILED, "bad delete param");
    std::vector<Replica> replicas = list_replicas_or_die(store);
    ASSERT_EQ(replicas.size(), 2U);
    EXPECT_EQ(replicas[0].state.phase, ReplicaPhase::ERROR);
}

// 删除table进入FAILED后，下一轮不会继续重试删除
TEST(TableReconcilerTest, FailedDropTableWillNotRetry) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_default_nodes(store);
    place_default_table(store);

    FakeStorageClient storage_client;
    FakeNodeSelector selector(&store);
    TestTableReconciler reconciler(&store, &storage_client, &selector);
    reconcile_table_to_ready(store, storage_client, reconciler);
    mark_table_absent(store);
    storage_client.delete_statuses.push_back(
        Status::INVALID_ARGUMENT("bad delete param"));
    ASSERT_TRUE(reconciler.reconcile_once().ok());
    ASSERT_EQ(get_table_or_die(store).state.phase, TablePhase::FAILED);
    size_t delete_count = storage_client.deleted.size();

    ASSERT_TRUE(reconciler.reconcile_once().ok());

    EXPECT_EQ(storage_client.deleted.size(), delete_count);
    expect_table_phase_and_msg(store, TablePhase::FAILED, "bad delete param");
}

}  // namespace adviskv::sdm