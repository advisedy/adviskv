#include "meta/service/ddl_service.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <string>

#include "common/status.h"
#include "meta/catalog/catalog_manager.h"
#include "meta/persist/meta_persist_engine.h"
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

    Status call_place_db(const DBMeta& db_meta) override {
        ++place_db_calls;
        last_place_db = db_meta;
        return place_db_status;
    }

    Status get_table_status(const TableMeta& table_meta,
                            SdmTableStatus* table_status) override {
        ++get_table_status_calls;
        last_get_table = table_meta;
        if (table_status) {
            *table_status = table_status_result;
        }
        return get_table_status_status;
    }

    Status place_table_status{Status::OK()};
    Status drop_table_status{Status::OK()};
    Status place_db_status{Status::OK()};
    Status get_table_status_status{Status::OK()};
    SdmTableStatus table_status_result;

    int place_table_calls{0};
    int drop_table_calls{0};
    int place_db_calls{0};
    int get_table_status_calls{0};

    TableMeta last_place_table;
    TableMeta last_drop_table;
    TableMeta last_get_table;
    DBMeta last_place_db;
};

class DdlServiceTest : public ::testing::Test {
   protected:
    void SetUp() override {
        test_dir_ = std::filesystem::temp_directory_path() /
                    ("ddl_service_test_" + std::to_string(::getpid()));
        std::filesystem::create_directories(test_dir_);
    }

    void TearDown() override { std::filesystem::remove_all(test_dir_); }

    std::filesystem::path make_sub_dir(const std::string& name) {
        auto dir = test_dir_ / name;
        std::filesystem::create_directories(dir);
        return dir;
    }

    struct Fixture {
        MetaPersistEngine engine;
        CatalogManager catalog;

        explicit Fixture(const std::filesystem::path& dir)
            : engine(dir.string()), catalog(&engine) {
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

// 测试创建db之后，没有sdm_client，导致db被删除，检验返回码，然后查询db是否存在。
TEST_F(DdlServiceTest, CreateDbWithoutSdmClientRollsBackCatalog) {
    Fixture fixture{make_sub_dir("create_db_without_sdm")};
    DdlService service{&fixture.catalog, nullptr};

    DBMeta db;
    Status status = service.create_db(create_db_param(), &db);

    EXPECT_EQ(status.code(), StatusCode::ERROR);
    DBMeta stored;
    EXPECT_EQ(fixture.catalog.get_db(db_name_, &stored).code(),
              StatusCode::DB_NOT_FOUND);
}

// 测试创建db之后，sdm_client那边返回fail，导致db被删除，检验返回码，然后查询db是否存在。
TEST_F(DdlServiceTest, CreateDbSdmFailureRollsBackCatalog) {
    Fixture fixture{make_sub_dir("create_db_sdm_failure")};
    FakeSdmClient client;
    client.place_db_status = Status::ERROR("place db failed");
    DdlService service{&fixture.catalog, &client};

    DBMeta db;
    Status status = service.create_db(create_db_param(), &db);

    EXPECT_EQ(status.code(), StatusCode::ERROR);
    EXPECT_EQ(client.place_db_calls, 1);
    DBMeta stored;
    EXPECT_EQ(fixture.catalog.get_db(db_name_, &stored).code(),
              StatusCode::DB_NOT_FOUND);
}

// 测试创建db一切正常，然后查询db是否存在。
TEST_F(DdlServiceTest, CreateDbSuccessPersistsCatalog) {
    Fixture fixture{make_sub_dir("create_db_success")};
    FakeSdmClient client;
    DdlService service{&fixture.catalog, &client};

    DBMeta db;
    ASSERT_TRUE(service.create_db(create_db_param(), &db).ok());

    EXPECT_EQ(client.place_db_calls, 1);
    EXPECT_EQ(client.last_place_db.db_name, db_name_);

    DBMeta stored;
    ASSERT_TRUE(fixture.catalog.get_db(db_name_, &stored).ok());
    EXPECT_EQ(stored, db);
}

// 测试创建table之后，没有sdm_client，失败了，table的状态还是ADDING，检测err_msg。
TEST_F(DdlServiceTest, CreateTableWithoutSdmClientKeepsAddingWithError) {
    Fixture fixture{make_sub_dir("create_table_without_sdm")};
    ASSERT_TRUE(
        fixture.catalog.create_db(CreateDBMetaParam{db_name_, zone_}, nullptr)
            .ok());

    DdlService db_service{&fixture.catalog, nullptr};
    TableMeta created;
    ASSERT_TRUE(db_service.create_table(create_table_param(), &created).ok());

    EXPECT_EQ(created.state, TableState::ADDING);
    EXPECT_EQ(created.last_error_msg, "sdm_client is nullptr");

    TableMeta stored;
    ASSERT_TRUE(
        fixture.catalog.get_table_by_name(db_name_, table_name_, &stored).ok());
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
    EXPECT_NE(created.last_error_msg.find("place table failed"),
              std::string::npos);

    TableMeta stored;
    ASSERT_TRUE(
        fixture.catalog.get_table_by_name(db_name_, table_name_, &stored).ok());
    EXPECT_EQ(stored.state, TableState::ADDING);
    EXPECT_NE(stored.last_error_msg.find("place table failed"),
              std::string::npos);
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
    ASSERT_TRUE(
        service.get_table(GetTableParam{"", "", true, created.table_id}, &by_id)
            .ok());
    EXPECT_EQ(by_id, by_name);
}

// 测试创建table成功后，drop_table遇到table.state == TableState::DELETED的情况。
TEST_F(DdlServiceTest, DropDeletedTableReturnsTableNotFound) {
    Fixture fixture{make_sub_dir("drop_deleted")};
    FakeSdmClient client;
    DdlService service{&fixture.catalog, &client};
    ASSERT_NO_FATAL_FAILURE(create_db_or_die(service));

    TableMeta table;
    ASSERT_TRUE(service.create_table(create_table_param(), &table).ok());
    ASSERT_TRUE(
        fixture.catalog.update_table_state(table.table_id, TableState::DELETED)
            .ok());

    TableMeta dropped;
    Status status = service.drop_table(drop_table_param(), &dropped);

    EXPECT_EQ(status.code(), StatusCode::TABLE_NOT_FOUND);
    EXPECT_EQ(client.drop_table_calls, 0);
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

    DdlService drop_service{&fixture.catalog, nullptr};
    TableMeta dropped;
    ASSERT_TRUE(drop_service.drop_table(drop_table_param(), &dropped).ok());

    EXPECT_EQ(dropped.state, TableState::DROPPING);
    EXPECT_EQ(dropped.last_error_msg, "sdm_client is nullptr");

    TableMeta stored;
    ASSERT_TRUE(
        fixture.catalog.get_table_by_id(dropped.table_id, &stored).ok());
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

    client.drop_table_status = Status::ERROR("drop table failed");
    TableMeta dropped;
    ASSERT_TRUE(service.drop_table(drop_table_param(), &dropped).ok());

    EXPECT_EQ(client.drop_table_calls, 1);
    EXPECT_EQ(dropped.state, TableState::DROPPING);
    EXPECT_NE(dropped.last_error_msg.find("drop table failed"),
              std::string::npos);

    TableMeta stored;
    ASSERT_TRUE(
        fixture.catalog.get_table_by_id(dropped.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::DROPPING);
    EXPECT_NE(stored.last_error_msg.find("drop table failed"),
              std::string::npos);
}

// 测试创建table成功后，drop_table一切正常，会调用sdm_client并且table状态变成DROPPING。
TEST_F(DdlServiceTest, DropTableSuccessMarksDroppingAndCallsSdm) {
    Fixture fixture{make_sub_dir("drop_success")};
    FakeSdmClient client;
    DdlService service{&fixture.catalog, &client};
    ASSERT_NO_FATAL_FAILURE(create_db_or_die(service));

    TableMeta table;
    ASSERT_TRUE(service.create_table(create_table_param(), &table).ok());

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
    ASSERT_TRUE(fixture.catalog
                    .update_table_state(old_table.table_id, TableState::DELETED)
                    .ok());

    TableMeta new_table;
    ASSERT_TRUE(service.create_table(create_table_param(), &new_table).ok());
    ASSERT_NE(new_table.table_id, old_table.table_id);

    TableMeta by_name;
    ASSERT_TRUE(service.get_table(get_table_by_name_param(), &by_name).ok());
    EXPECT_EQ(by_name.table_id, new_table.table_id);

    TableMeta old_by_id;
    ASSERT_TRUE(service
                    .get_table(
                        GetTableParam{"", "", true, old_table.table_id},
                        &old_by_id)
                    .ok());
    EXPECT_EQ(old_by_id.table_id, old_table.table_id);
    EXPECT_EQ(old_by_id.state, TableState::DELETED);
}

}  // namespace

}  // namespace adviskv::meta