#include "meta/service/ddl_service.h"

#include <filesystem>
#include <string>

#include <gtest/gtest.h>
#include <unistd.h>

#include "common/status.h"
#include "meta/catalog/catalog_manager.h"
#include "meta/persist/meta_persist_engine.h"
#include "meta/proto/table_state_proto.h"
#include "meta/service/sdm_client.h"

namespace adviskv::meta {
namespace {

class FakeSdmClient : public ISdmClient {
public:
    Status call_place_table(const TableMeta& table_meta) override {
        ++place_table_calls;
        last_place_table = table_meta;
        return place_table_status;
    }

    Status call_drop_table(const TableMeta& table_meta) override {
        ++drop_table_calls;
        last_drop_table = table_meta;
        return drop_table_status;
    }

    Status call_alter_table_replica_count(const TableMeta& table_meta) override {
        ++alter_table_calls;
        last_alter_table = table_meta;
        return alter_table_status;
    }

    Status get_table_status(const TableMeta& table_meta, SdmTableStatus* table_status) override {
        ++get_table_status_calls;
        last_get_table = table_meta;
        if (table_status) {
            *table_status = table_status_result;
        }
        return get_table_status_status;
    }

    Status place_table_status{Status::OK()};
    Status drop_table_status{Status::OK()};
    Status alter_table_status{Status::OK()};
    Status get_table_status_status{Status::OK()};
    SdmTableStatus table_status_result;

    int place_table_calls{0};
    int drop_table_calls{0};
    int alter_table_calls{0};
    int get_table_status_calls{0};

    TableMeta last_place_table;
    TableMeta last_drop_table;
    TableMeta last_alter_table;
    TableMeta last_get_table;
};

class DdlServiceTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = std::filesystem::temp_directory_path() / ("ddl_service_test_" + std::to_string(::getpid()));
        std::filesystem::create_directories(test_dir_);
    }

    void TearDown() override {
        std::filesystem::remove_all(test_dir_);
    }

    std::filesystem::path make_sub_dir(const std::string& name) {
        auto dir = test_dir_ / name;
        std::filesystem::create_directories(dir);
        return dir;
    }

    struct Fixture {
        MetaPersistEngine engine;
        CatalogManager catalog;

        explicit Fixture(const std::filesystem::path& dir) : engine(dir.string()), catalog(&engine) {
            EXPECT_TRUE(engine.init().ok());
            EXPECT_TRUE(catalog.init().ok());
        }
    };

    CreateDBParam create_db_param() {
        return CreateDBParam{db_name_, zone_};
    }

    CreateTableParam create_table_param() {
        return CreateTableParam{db_name_, table_name_, 4, 3, resource_pool_};
    }

    DropTableParam drop_table_param() {
        return DropTableParam{db_name_, table_name_};
    }

    AlterTableReplicaCountParam alter_replica_count_param(int32_t replica_count = 5) {
        return AlterTableReplicaCountParam{db_name_, table_name_, replica_count};
    }

    DropDBParam drop_db_param() {
        return DropDBParam{db_name_};
    }

    GetTableParam get_table_by_name_param() {
        return GetTableParam{db_name_, table_name_, false, -1};
    }

    void create_db_or_die(DdlService& service) {
        DBMeta db;
        ASSERT_TRUE(service.create_db(create_db_param(), &db).ok());
    }

    std::filesystem::path test_dir_;
    const std::string db_name_{"commerce"};
    const std::string zone_{"zone-a"};
    const std::string table_name_{"orders"};
    const std::string resource_pool_{"pool-a"};
};

// DB 是 Meta 侧 namespace，创建 DB 不依赖 SDM。
TEST_F(DdlServiceTest, CreateDbWithoutSdmClientPersistsCatalog) {
    Fixture fixture{make_sub_dir("create_db_without_sdm")};
    DdlService service{&fixture.catalog, nullptr};

    DBMeta db;
    Status status = service.create_db(create_db_param(), &db);

    ASSERT_TRUE(status.ok()) << status.to_string();
    DBMeta stored;
    ASSERT_TRUE(fixture.catalog.get_db(db_name_, &stored).ok());
    EXPECT_EQ(stored, db);
}

// 测试创建db一切正常，然后查询db是否存在。
TEST_F(DdlServiceTest, CreateDbSuccessPersistsCatalog) {
    Fixture fixture{make_sub_dir("create_db_success")};
    FakeSdmClient client;
    DdlService service{&fixture.catalog, &client};

    DBMeta db;
    ASSERT_TRUE(service.create_db(create_db_param(), &db).ok());

    EXPECT_EQ(client.place_table_calls, 0);
    EXPECT_EQ(client.drop_table_calls, 0);
    EXPECT_EQ(client.get_table_status_calls, 0);

    DBMeta stored;
    ASSERT_TRUE(fixture.catalog.get_db(db_name_, &stored).ok());
    EXPECT_EQ(stored, db);
}

// 测试 DDL 层删除仍包含活跃表的 DB 时返回非法参数，并保留 DB 元信息。
TEST_F(DdlServiceTest, DropDbRejectsDbWithActiveTable) {
    Fixture fixture{make_sub_dir("drop_db_with_active_table")};
    FakeSdmClient client;
    DdlService service{&fixture.catalog, &client};
    ASSERT_NO_FATAL_FAILURE(create_db_or_die(service));
    TableMeta table;
    ASSERT_TRUE(service.create_table(create_table_param(), &table).ok());

    DBMeta dropped;
    Status status = service.drop_db(drop_db_param(), &dropped);

    EXPECT_EQ(status.code(), StatusCode::INVALID_ARGUMENT);
    DBMeta stored;
    ASSERT_TRUE(fixture.catalog.get_db(db_name_, &stored).ok());
    EXPECT_EQ(stored.db_name, db_name_);
}

// 测试 DDL 层允许删除只剩 DELETED 历史表的 DB，并确认 DB 名称索引被清理。
TEST_F(DdlServiceTest, DropDbAllowsOnlyDeletedTables) {
    Fixture fixture{make_sub_dir("drop_db_only_deleted_tables")};
    FakeSdmClient client;
    DdlService service{&fixture.catalog, &client};
    ASSERT_NO_FATAL_FAILURE(create_db_or_die(service));
    TableMeta table;
    ASSERT_TRUE(service.create_table(create_table_param(), &table).ok());
    ASSERT_TRUE(fixture.catalog.update_table_state(table.table_id, TableState::DELETED).ok());

    DBMeta dropped;
    Status status = service.drop_db(drop_db_param(), &dropped);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(dropped.db_name, db_name_);
    DBMeta stored;
    EXPECT_EQ(fixture.catalog.get_db(db_name_, &stored).code(), StatusCode::DB_NOT_FOUND);
}

// 测试 Meta 建表允许 replica_count = 0，并把 scale-to-zero 语义交给 SDM。
TEST_F(DdlServiceTest, CreateTableAllowsZeroReplicaCount) {
    Fixture fixture{make_sub_dir("create_table_zero_replica")};
    FakeSdmClient client;
    DdlService service{&fixture.catalog, &client};
    ASSERT_NO_FATAL_FAILURE(create_db_or_die(service));

    CreateTableParam param = create_table_param();
    param.replica_count = 0;
    param.engine_type = EngineType::ROCKSDB;
    TableMeta created;
    Status status = service.create_table(param, &created);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(client.place_table_calls, 1);
    EXPECT_EQ(client.last_place_table.replica_count, 0);
    EXPECT_EQ(client.last_place_table.engine_type, EngineType::ROCKSDB);
    EXPECT_EQ(created.state, TableState::ADDING);
    EXPECT_EQ(created.replica_count, 0);
    EXPECT_EQ(created.engine_type, EngineType::ROCKSDB);
    TableMeta stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_name(db_name_, table_name_, &stored).ok());
    EXPECT_EQ(stored.replica_count, 0);
    EXPECT_EQ(stored.engine_type, EngineType::ROCKSDB);
    EXPECT_EQ(stored.state, TableState::ADDING);
}

// 测试创建table之后，没有sdm_client，失败了，table的状态还是ADDING，检测err_msg。
TEST_F(DdlServiceTest, CreateTableWithoutSdmClientKeepsAddingWithError) {
    Fixture fixture{make_sub_dir("create_table_without_sdm")};
    ASSERT_TRUE(fixture.catalog.create_db(CreateDBParam{db_name_, zone_}, nullptr).ok());

    DdlService db_service{&fixture.catalog, nullptr};
    TableMeta created;
    ASSERT_TRUE(db_service.create_table(create_table_param(), &created).ok());

    EXPECT_EQ(created.state, TableState::ADDING);
    EXPECT_EQ(created.last_error_msg, "sdm_client is nullptr");

    TableMeta stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_name(db_name_, table_name_, &stored).ok());
    EXPECT_EQ(stored.state, TableState::ADDING);
    EXPECT_EQ(stored.last_error_msg, "sdm_client is nullptr");
}

// 测试创建table之后，sdm_client_->call_place_table返回失败了，table的状态还是ADDING，检测err_msg。
TEST_F(DdlServiceTest, CreateTableSdmFailureKeepsAddingWithError) {
    Fixture fixture{make_sub_dir("create_table_sdm_failure")};
    FakeSdmClient client;
    DdlService service{&fixture.catalog, &client};
    ASSERT_NO_FATAL_FAILURE(create_db_or_die(service));

    client.place_table_status = Status::ERROR("place table failed");
    TableMeta created;
    ASSERT_TRUE(service.create_table(create_table_param(), &created).ok());

    EXPECT_EQ(client.place_table_calls, 1);
    EXPECT_EQ(created.state, TableState::ADDING);
    EXPECT_NE(created.last_error_msg.find("place table failed"), std::string::npos);

    TableMeta stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_name(db_name_, table_name_, &stored).ok());
    EXPECT_EQ(stored.state, TableState::ADDING);
    EXPECT_NE(stored.last_error_msg.find("place table failed"), std::string::npos);
}

// 测试创建table成功了，get_table的两种方法是没有问题的。
TEST_F(DdlServiceTest, CreateTableSuccessSupportsGetByNameAndId) {
    Fixture fixture{make_sub_dir("create_table_success_get")};
    FakeSdmClient client;
    DdlService service{&fixture.catalog, &client};
    ASSERT_NO_FATAL_FAILURE(create_db_or_die(service));

    TableMeta created;
    ASSERT_TRUE(service.create_table(create_table_param(), &created).ok());

    EXPECT_EQ(client.place_table_calls, 1);
    EXPECT_EQ(created.state, TableState::ADDING);
    EXPECT_TRUE(created.last_error_msg.empty());

    TableMeta by_name;
    ASSERT_TRUE(service.get_table(get_table_by_name_param(), &by_name).ok());
    EXPECT_EQ(by_name.table_id, created.table_id);
    EXPECT_EQ(by_name.table_name, table_name_);

    TableMeta by_id;
    ASSERT_TRUE(service.get_table(GetTableParam{"", "", true, created.table_id}, &by_id).ok());
    EXPECT_EQ(by_id, by_name);
}

// 测试 alter replica_count 成功时会调用 SDM，并把 Meta 表状态标记为 ALTERING。
TEST_F(DdlServiceTest, AlterReplicaCountSuccessMarksAlteringAndCallsSdm) {
    Fixture fixture{make_sub_dir("alter_replica_count_success")};
    FakeSdmClient client;
    DdlService service{&fixture.catalog, &client};
    ASSERT_NO_FATAL_FAILURE(create_db_or_die(service));

    TableMeta table;
    ASSERT_TRUE(service.create_table(create_table_param(), &table).ok());
    ASSERT_TRUE(fixture.catalog.update_table_state(table.table_id, TableState::NORMAL).ok());

    TableMeta altered;
    ASSERT_TRUE(service.alter_table_replica_count(alter_replica_count_param(5), &altered).ok());

    EXPECT_EQ(client.alter_table_calls, 1);
    EXPECT_EQ(client.last_alter_table.table_id, table.table_id);
    EXPECT_EQ(client.last_alter_table.replica_count, 5);
    EXPECT_EQ(altered.state, TableState::ALTERING);
    EXPECT_EQ(altered.replica_count, 5);
    EXPECT_FALSE(altered.operation_id.empty());
    EXPECT_TRUE(altered.last_error_msg.empty());
}

// 测试 alter replica_count 允许缩到 0，并把 scale-to-zero 语义交给 SDM。
TEST_F(DdlServiceTest, AlterReplicaCountAllowsZeroReplicaCount) {
    Fixture fixture{make_sub_dir("alter_replica_count_zero")};
    FakeSdmClient client;
    DdlService service{&fixture.catalog, &client};
    ASSERT_NO_FATAL_FAILURE(create_db_or_die(service));

    TableMeta table;
    ASSERT_TRUE(service.create_table(create_table_param(), &table).ok());
    ASSERT_TRUE(fixture.catalog.update_table_state(table.table_id, TableState::NORMAL).ok());

    TableMeta altered;
    ASSERT_TRUE(service.alter_table_replica_count(alter_replica_count_param(0), &altered).ok());

    EXPECT_EQ(client.alter_table_calls, 1);
    EXPECT_EQ(client.last_alter_table.table_id, table.table_id);
    EXPECT_EQ(client.last_alter_table.replica_count, 0);
    EXPECT_EQ(altered.state, TableState::ALTERING);
    EXPECT_EQ(altered.replica_count, 0);
}

// 测试 alter replica_count 缺少 SDM client 时，表保持 ALTERING 并记录错误信息。
TEST_F(DdlServiceTest, AlterReplicaCountWithoutSdmKeepsAlteringWithError) {
    Fixture fixture{make_sub_dir("alter_replica_count_without_sdm")};
    FakeSdmClient client;
    DdlService create_service{&fixture.catalog, &client};
    ASSERT_NO_FATAL_FAILURE(create_db_or_die(create_service));

    TableMeta table;
    ASSERT_TRUE(create_service.create_table(create_table_param(), &table).ok());
    ASSERT_TRUE(fixture.catalog.update_table_state(table.table_id, TableState::NORMAL).ok());

    DdlService alter_service{&fixture.catalog, nullptr};
    TableMeta altered;
    ASSERT_TRUE(alter_service.alter_table_replica_count(alter_replica_count_param(5), &altered).ok());

    EXPECT_EQ(altered.state, TableState::ALTERING);
    EXPECT_EQ(altered.replica_count, 5);
    EXPECT_EQ(altered.last_error_msg, "sdm_client is nullptr");
}

// 测试创建table成功后，drop_table遇到table.state == TableState::DELETED的情况。
TEST_F(DdlServiceTest, DropDeletedTableReturnsTableNotFound) {
    Fixture fixture{make_sub_dir("drop_deleted")};
    FakeSdmClient client;
    DdlService service{&fixture.catalog, &client};
    ASSERT_NO_FATAL_FAILURE(create_db_or_die(service));

    TableMeta table;
    ASSERT_TRUE(service.create_table(create_table_param(), &table).ok());
    ASSERT_TRUE(fixture.catalog.update_table_state(table.table_id, TableState::DELETED).ok());

    TableMeta dropped;
    Status status = service.drop_table(drop_table_param(), &dropped);

    EXPECT_EQ(status.code(), StatusCode::TABLE_NOT_FOUND);
    EXPECT_EQ(client.drop_table_calls, 0);
}

// 测试 V1 普通 drop_table 只允许删除已经 NORMAL 的 table，创建中的 table 会被拒绝。
TEST_F(DdlServiceTest, DropAddingTableIsRejectedWithoutCallingSdm) {
    Fixture fixture{make_sub_dir("drop_adding_rejected")};
    FakeSdmClient client;
    DdlService service{&fixture.catalog, &client};
    ASSERT_NO_FATAL_FAILURE(create_db_or_die(service));

    TableMeta table;
    ASSERT_TRUE(service.create_table(create_table_param(), &table).ok());
    ASSERT_EQ(table.state, TableState::ADDING);

    TableMeta dropped;
    Status status = service.drop_table(drop_table_param(), &dropped);

    EXPECT_EQ(status.code(), StatusCode::INVALID_ARGUMENT);
    EXPECT_EQ(client.drop_table_calls, 0);

    TableMeta stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(table.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::ADDING);
    EXPECT_EQ(stored.operation_id, table.operation_id);
}

// 测试创建table成功后，drop_table遇到table.state ==
// TableState::DROPPING的情况。
TEST_F(DdlServiceTest, DropDroppingTableIsIdempotent) {
    Fixture fixture{make_sub_dir("drop_dropping")};
    FakeSdmClient client;
    DdlService service{&fixture.catalog, &client};
    ASSERT_NO_FATAL_FAILURE(create_db_or_die(service));

    TableMeta table;
    ASSERT_TRUE(service.create_table(create_table_param(), &table).ok());
    ASSERT_TRUE(fixture.catalog.update_table_state(table.table_id, TableState::NORMAL).ok());
    ASSERT_TRUE(fixture.catalog.delete_table(table.table_id, &table).ok());
    ASSERT_EQ(table.state, TableState::DROPPING);

    TableMeta dropped;
    ASSERT_TRUE(service.drop_table(drop_table_param(), &dropped).ok());

    EXPECT_EQ(dropped.table_id, table.table_id);
    EXPECT_EQ(dropped.state, TableState::DROPPING);
    EXPECT_EQ(client.drop_table_calls, 0);
}

// 测试创建table成功后，drop_table遇到没有sdm_client的情况，状态会是DROPPING，检测err_msg。
TEST_F(DdlServiceTest, DropTableWithoutSdmClientKeepsDroppingWithError) {
    Fixture fixture{make_sub_dir("drop_without_sdm")};
    FakeSdmClient client;
    DdlService create_service{&fixture.catalog, &client};
    ASSERT_NO_FATAL_FAILURE(create_db_or_die(create_service));

    TableMeta table;
    ASSERT_TRUE(create_service.create_table(create_table_param(), &table).ok());
    ASSERT_TRUE(fixture.catalog.update_table_state(table.table_id, TableState::NORMAL).ok());

    DdlService drop_service{&fixture.catalog, nullptr};
    TableMeta dropped;
    ASSERT_TRUE(drop_service.drop_table(drop_table_param(), &dropped).ok());

    EXPECT_EQ(dropped.state, TableState::DROPPING);
    EXPECT_EQ(dropped.last_error_msg, "sdm_client is nullptr");

    TableMeta stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(dropped.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::DROPPING);
    EXPECT_EQ(stored.last_error_msg, "sdm_client is nullptr");
}

// 测试创建table成功后，drop_table遇到sdm_client_->call_drop_table返回失败的情况，检测err_msg。
TEST_F(DdlServiceTest, DropTableSdmFailureKeepsDroppingWithError) {
    Fixture fixture{make_sub_dir("drop_sdm_failure")};
    FakeSdmClient client;
    DdlService service{&fixture.catalog, &client};
    ASSERT_NO_FATAL_FAILURE(create_db_or_die(service));

    TableMeta table;
    ASSERT_TRUE(service.create_table(create_table_param(), &table).ok());
    ASSERT_TRUE(fixture.catalog.update_table_state(table.table_id, TableState::NORMAL).ok());

    client.drop_table_status = Status::ERROR("drop table failed");
    TableMeta dropped;
    ASSERT_TRUE(service.drop_table(drop_table_param(), &dropped).ok());

    EXPECT_EQ(client.drop_table_calls, 1);
    EXPECT_EQ(dropped.state, TableState::DROPPING);
    EXPECT_NE(dropped.last_error_msg.find("drop table failed"), std::string::npos);

    TableMeta stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(dropped.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::DROPPING);
    EXPECT_NE(stored.last_error_msg.find("drop table failed"), std::string::npos);
}

// 测试创建table成功后，drop_table一切正常，会调用sdm_client并且table状态变成DROPPING。
TEST_F(DdlServiceTest, DropTableSuccessMarksDroppingAndCallsSdm) {
    Fixture fixture{make_sub_dir("drop_success")};
    FakeSdmClient client;
    DdlService service{&fixture.catalog, &client};
    ASSERT_NO_FATAL_FAILURE(create_db_or_die(service));

    TableMeta table;
    ASSERT_TRUE(service.create_table(create_table_param(), &table).ok());
    ASSERT_TRUE(fixture.catalog.update_table_state(table.table_id, TableState::NORMAL).ok());

    TableMeta dropped;
    ASSERT_TRUE(service.drop_table(drop_table_param(), &dropped).ok());

    EXPECT_EQ(client.drop_table_calls, 1);
    EXPECT_EQ(client.last_drop_table.table_id, table.table_id);
    EXPECT_EQ(dropped.state, TableState::DROPPING);
    EXPECT_TRUE(dropped.last_error_msg.empty());
}

// 测试创建table成功了，然后删除table成功了，然后再创建一个同名的table后，get_table用name和id分别找到当下的table和之前的table。
TEST_F(DdlServiceTest, RecreateSameNameAfterDeleteKeepsOldIdLookup) {
    Fixture fixture{make_sub_dir("recreate_same_name")};
    FakeSdmClient client;
    DdlService service{&fixture.catalog, &client};
    ASSERT_NO_FATAL_FAILURE(create_db_or_die(service));

    TableMeta old_table;
    ASSERT_TRUE(service.create_table(create_table_param(), &old_table).ok());
    ASSERT_TRUE(fixture.catalog.update_table_state(old_table.table_id, TableState::DELETED).ok());

    TableMeta new_table;
    ASSERT_TRUE(service.create_table(create_table_param(), &new_table).ok());
    ASSERT_NE(new_table.table_id, old_table.table_id);

    TableMeta by_name;
    ASSERT_TRUE(service.get_table(get_table_by_name_param(), &by_name).ok());
    EXPECT_EQ(by_name.table_id, new_table.table_id);

    TableMeta old_by_id;
    ASSERT_TRUE(service.get_table(GetTableParam{"", "", true, old_table.table_id}, &old_by_id).ok());
    EXPECT_EQ(old_by_id.table_id, old_table.table_id);
    EXPECT_EQ(old_by_id.state, TableState::DELETED);
}

}  // namespace

}  // namespace adviskv::meta
