#include "sdm/service/table_service.h"

#include <gtest/gtest.h>

#include "common/status.h"
#include "sdm/model/sdm_store.h"

namespace adviskv::sdm {
namespace {

PlaceTableParam make_place_table_param() {
    return PlaceTableParam{11, 1001, "commerce", "orders", 2, 3, "pool-a",
                           "create-table-1001"};
}

DropTableParam make_drop_table_param() {
    return DropTableParam{1001, "drop-table-1001"};
}

void put_ready_table(SdmStore& store) {
    TableState state{};
    state.desired = TableDesired::PRESENT;
    state.phase = TablePhase::READY;
    Table table{1001,
                TableSpec{"orders", 11, "commerce", 3, 2, "pool-a",
                          "create-table-1001"},
                state};
    ASSERT_TRUE(store.put_table(table).ok());
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
    ASSERT_TRUE(store.get_table(param.table_id, stored).ok());
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
    ASSERT_TRUE(store.get_table(param.table_id, stored).ok());
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
    ASSERT_TRUE(store.get_table(1001, stored).ok());
    ASSERT_FALSE(stored.is_empty());
    EXPECT_EQ(stored->state.desired, TableDesired::ABSENT);
    EXPECT_EQ(stored->state.phase, TablePhase::DELETING);
    EXPECT_EQ(stored->spec.operation_id, "drop-table-1001");
    EXPECT_TRUE(stored->state.last_error_msg.empty());
}

// 检测 drop_table 对不存在的表和相同 operation_id 的重试保持幂等。
TEST(TableServiceTest, DropTableHandlesMissingAndSameOperationRetry) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    TableService service(&store);

    EXPECT_TRUE(service.drop_table(make_drop_table_param()).ok());

    put_ready_table(store);
    ASSERT_TRUE(service.drop_table(make_drop_table_param()).ok());
    Status retry = service.drop_table(make_drop_table_param());

    EXPECT_TRUE(retry.ok()) << retry.msg();
}

// 检测 drop_table 对非 READY 表或者不同 drop operation_id 会返回冲突。
TEST(TableServiceTest, DropTableRejectsInvalidStateAndConflictingOperation) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    TableService service(&store);
    ASSERT_TRUE(service.place_table(make_place_table_param()).ok());

    Status not_ready = service.drop_table(make_drop_table_param());
    EXPECT_EQ(not_ready.code(), StatusCode::ALREADY_EXIST);

    TableState state{};
    state.desired = TableDesired::ABSENT;
    state.phase = TablePhase::DELETING;
    Table table{1002,
                TableSpec{"payments", 11, "commerce", 1, 1, "pool-a",
                          "drop-old"},
                state};
    ASSERT_TRUE(store.put_table(table).ok());

    Status conflict = service.drop_table(
        DropTableParam{1002, "drop-new"});
    EXPECT_EQ(conflict.code(), StatusCode::ALREADY_EXIST);
}

// 检测 get_table_status 可以查询表状态，并校验 operation_id。
TEST(TableServiceTest, GetTableStatusReturnsStoredTableAndChecksOperationId) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    TableService service(&store);
    PlaceTableParam param = make_place_table_param();
    ASSERT_TRUE(service.place_table(param).ok());

    Table table;
    Status status = service.get_table_status(
        GetTableStatusParam{param.operation_id, param.table_id},
        &table);

    ASSERT_TRUE(status.ok()) << status.msg();
    EXPECT_EQ(table.table_id, param.table_id);
    EXPECT_EQ(table.spec.operation_id, param.operation_id);

    Status mismatch = service.get_table_status(
        GetTableStatusParam{"other-op", param.table_id},
        nullptr);
    EXPECT_EQ(mismatch.code(), StatusCode::INVALID_ARGUMENT);

    Status missing = service.get_table_status(
        GetTableStatusParam{"", 9999}, nullptr);
    EXPECT_EQ(missing.code(), StatusCode::TABLE_NOT_FOUND);
}

}  // namespace adviskv::sdm