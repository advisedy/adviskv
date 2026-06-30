#include "sdm/service/table_service.h"

#include <fmt/format.h>

#include <gtest/gtest.h>
#include <memory>

#include "common/status.h"
#include "sdm/model/sdm_store.h"
#include "sdm/sdm_store_test_helper.h"

namespace adviskv::sdm {
namespace {

PlaceTableParam make_place_table_param() {
    return PlaceTableParam{11, 1001, "commerce", "orders",
                           2,  3,    "pool-a",   "create-table-1001"};
}

DropTableParam make_drop_table_param() {
    return DropTableParam{1001, "drop-table-1001"};
}

AlterReplicaCountParam make_alter_replica_count_param(
    int32_t replica_count = 5) {
    return AlterReplicaCountParam{1001, replica_count,
                                       "alter-replica-count-1001"};
}

void put_ready_table(SdmStore& store) {
    TableState state{};
    state.desired = TableDesired::PRESENT;
    state.phase = TablePhase::READY;
    Table table{1001,
                TableSpec{"orders", 11, "commerce", 3, 2, "pool-a",
                          "create-table-1001"},
                state};
    ASSERT_TRUE(store_test::put_table(store, table).ok());

    for (ShardIndex shard_index = 0; shard_index < table.spec.shard_count;
         ++shard_index) {
        ShardID shard_id{table.table_id, shard_index};
        ReplicaGroup group;
        group.shard_id = shard_id;
        group.mode = ReplicaGroupMode::RAFT_RECONFIG;
        group.target_replica_count = table.spec.replica_count;
        group.seq_allocator = IDAllocator<ReplicaSeq>(table.spec.replica_count);

        ShardRoute route;
        route.shard_id = shard_id;

        for (ReplicaSeq seq = 0; seq < table.spec.replica_count; ++seq) {
            ReplicaID rid{table.table_id, shard_index, seq};
            group.desired_members.push_back(rid);

            Endpoint endpoint{"127.0.0.1", 18080 + shard_index * 10 + seq};
            Replica replica;
            replica.replica_id = rid;
            replica.spec.dc = "dc-a";
            replica.spec.assign_node_id =
                fmt::format("node-{}-{}", shard_index, seq);
            replica.spec.engine_type = EngineType::MAP;
            replica.state.desired = ReplicaDesired::PRESENT;
            replica.state.phase = ReplicaPhase::READY;
            replica.state.observed_raft_role =
                seq == 0 ? ReplicaRole::LEADER : ReplicaRole::FOLLOWER;
            replica.state.observed_member_type = RaftMemberType::VOTER;
            replica.state.observed_endpoint = endpoint;
            replica.state.term = 1;
            replica.state.update_ts = 1;
            ASSERT_TRUE(store_test::put_replica(store, replica).ok());

            route.replicas.push_back(RouteEntry{
                rid,
                replica.spec.assign_node_id,
                endpoint.ip,
                endpoint.port,
                replica.state.observed_raft_role,
                replica.state.term,
            });
        }

        ASSERT_TRUE(store_test::put_replica_group(store, group).ok());
        ASSERT_TRUE(store_test::put_shard_route(store, route).ok());
    }
}

}  // namespace

// 检测 place_table 会把建表参数转换为 CREATING 的 Table desired state。
TEST(TableServiceTest, PlaceTableConvertsParamToDesiredTableState) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    TableService service(&store);
    PlaceTableParam param = make_place_table_param();

    Status status = service.place_table(param);

    ASSERT_TRUE(status.ok()) << status.msg();
    TableOr stored;
    ASSERT_TRUE(store_test::get_table(store, param.table_id, stored).ok());
    ASSERT_FALSE(stored.is_empty());
    EXPECT_EQ(stored->table_id, param.table_id);
    EXPECT_EQ(stored->spec.table_name, param.table_name);
    EXPECT_EQ(stored->spec.db_name, param.db_name);
    EXPECT_EQ(stored->spec.db_id, param.db_id);
    EXPECT_EQ(stored->spec.replica_count, param.replica_count);
    EXPECT_EQ(stored->spec.shard_count, param.shard_count);
    EXPECT_EQ(stored->spec.resource_pool, param.resource_pool);
    EXPECT_EQ(stored->spec.operation_id, param.operation_id);
    EXPECT_EQ(stored->state.desired, TableDesired::PRESENT);
    EXPECT_EQ(stored->state.phase, TablePhase::CREATING);
}

// 检测相同 operation_id 的 place_table 重试会被当成幂等成功。
TEST(TableServiceTest, PlaceTableTreatsSameOperationIdRetryAsIdempotent) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    TableService service(&store);
    PlaceTableParam param = make_place_table_param();
    ASSERT_TRUE(service.place_table(param).ok());

    Status status = service.place_table(param);

    EXPECT_TRUE(status.ok()) << status.msg();
}

// 检测相同 table_id 但不同 operation_id 的建表请求会返回冲突。
TEST(TableServiceTest, PlaceTableRejectsConflictingOperationId) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    TableService service(&store);
    PlaceTableParam param = make_place_table_param();
    ASSERT_TRUE(service.place_table(param).ok());

    PlaceTableParam conflict = param;
    conflict.operation_id = "create-table-1001-other";
    Status status = service.place_table(conflict);

    EXPECT_EQ(status.code(), StatusCode::ALREADY_EXIST);
}

// 检测非法建表参数不会写入 SdmStore。
TEST(TableServiceTest, PlaceTableRejectsInvalidParamBeforePersist) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    TableService service(&store);
    PlaceTableParam param = make_place_table_param();
    param.table_name.clear();

    Status status = service.place_table(param);

    EXPECT_EQ(status.code(), StatusCode::INVALID_ARGUMENT);
    TableOr stored;
    ASSERT_TRUE(store_test::get_table(store, param.table_id, stored).ok());
    EXPECT_TRUE(stored.is_empty());
}

// 检测 READY 表执行 drop_table 后会被标记为 ABSENT/DELETING。
TEST(TableServiceTest, DropReadyTableMarksDesiredAbsent) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_ready_table(store);
    TableService service(&store);

    Status status = service.drop_table(make_drop_table_param());

    ASSERT_TRUE(status.ok()) << status.msg();
    TableOr stored;
    ASSERT_TRUE(store_test::get_table(store, 1001, stored).ok());
    ASSERT_FALSE(stored.is_empty());
    EXPECT_EQ(stored->state.desired, TableDesired::ABSENT);
    EXPECT_EQ(stored->state.phase, TablePhase::DELETING);
    EXPECT_EQ(stored->spec.operation_id, "drop-table-1001");
    EXPECT_TRUE(stored->state.last_error_msg.empty());
}

// 检测 drop_table 是 ensure-absent 语义：不存在、同 operation_id 重试、
// 不同 operation_id 命中已 ABSENT 的表，都保持幂等成功。
TEST(TableServiceTest, DropTableHandlesAlreadyAbsentAsIdempotent) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    TableService service(&store);

    EXPECT_TRUE(service.drop_table(make_drop_table_param()).ok());

    put_ready_table(store);
    ASSERT_TRUE(service.drop_table(make_drop_table_param()).ok());
    Status retry = service.drop_table(make_drop_table_param());

    EXPECT_TRUE(retry.ok()) << retry.msg();

    Status another_drop = service.drop_table(
        DropTableParam{1001, "drop-table-1001-another"});
    EXPECT_TRUE(another_drop.ok()) << another_drop.msg();
}

// 检测 drop_table 对非 READY 的 PRESENT 表会返回冲突。
TEST(TableServiceTest, DropTableRejectsInvalidPresentState) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    TableService service(&store);
    ASSERT_TRUE(service.place_table(make_place_table_param()).ok());

    Status not_ready = service.drop_table(make_drop_table_param());
    EXPECT_EQ(not_ready.code(), StatusCode::ALREADY_EXIST);
}

// 检测 READY 表执行 alter replica_count 后会更新副本数并进入 RESIZING。
TEST(TableServiceTest, AlterReadyTableUpdatesReplicaCountAndMarksResizing) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_ready_table(store);
    TableService service(&store);

    Status status =
        service.alter_table_replica_count(make_alter_replica_count_param(5));

    ASSERT_TRUE(status.ok()) << status.to_string();
    TableOr stored;
    ASSERT_TRUE(store_test::get_table(store, 1001, stored).ok());
    ASSERT_FALSE(stored.is_empty());
    EXPECT_EQ(stored->spec.replica_count, 5);
    EXPECT_EQ(stored->spec.operation_id, "alter-replica-count-1001");
    EXPECT_EQ(stored->state.desired, TableDesired::PRESENT);
    EXPECT_EQ(stored->state.phase, TablePhase::RESIZING);
    EXPECT_TRUE(stored->state.last_error_msg.empty());
}

// 检测相同 operation_id 的 alter replica_count 重试会被当成幂等成功。
TEST(TableServiceTest, AlterReplicaCountRetryIsIdempotent) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_ready_table(store);
    TableService service(&store);
    AlterReplicaCountParam param = make_alter_replica_count_param(5);
    ASSERT_TRUE(service.alter_table_replica_count(param).ok());

    Status status = service.alter_table_replica_count(param);

    EXPECT_TRUE(status.ok()) << status.to_string();
}

// 检测 RESIZING 表在 shard 尚未 ready 前会继续保持 RESIZING。
TEST(TableServiceTest, ResizingTableKeepsResizingUntilShardsReady) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    put_ready_table(store);
    TableService service(&store);
    ASSERT_TRUE(
        service.alter_table_replica_count(make_alter_replica_count_param(5))
            .ok());

    Status status = service.reconcile_all();

    ASSERT_TRUE(status.ok()) << status.to_string();
    TableOr stored;
    ASSERT_TRUE(store_test::get_table(store, 1001, stored).ok());
    ASSERT_FALSE(stored.is_empty());
    EXPECT_EQ(stored->state.phase, TablePhase::RESIZING);
}

// 检测非 READY 表执行 alter replica_count 会返回错误。
TEST(TableServiceTest, AlterReplicaCountRejectsNonReadyTable) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    TableService service(&store);
    ASSERT_TRUE(service.place_table(make_place_table_param()).ok());

    Status status =
        service.alter_table_replica_count(make_alter_replica_count_param(5));

    EXPECT_EQ(status.code(), StatusCode::ERROR);
}

// 检测 get_table_status 可以查询表状态，并校验 operation_id。
TEST(TableServiceTest, GetTableStatusReturnsStoredTableAndChecksOperationId) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    TableService service(&store);
    PlaceTableParam param = make_place_table_param();
    ASSERT_TRUE(service.place_table(param).ok());

    Table table;
    Status status = service.get_table_status(
        GetTableStatusParam{param.operation_id, param.table_id}, &table);

    ASSERT_TRUE(status.ok()) << status.msg();
    EXPECT_EQ(table.table_id, param.table_id);
    EXPECT_EQ(table.spec.operation_id, param.operation_id);

    Status mismatch = service.get_table_status(
        GetTableStatusParam{"other-op", param.table_id}, nullptr);
    EXPECT_EQ(mismatch.code(), StatusCode::INVALID_ARGUMENT);

    Status missing =
        service.get_table_status(GetTableStatusParam{"", 9999}, nullptr);
    EXPECT_EQ(missing.code(), StatusCode::TABLE_NOT_FOUND);
}

}  // namespace adviskv::sdm
