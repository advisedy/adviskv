#include "meta/background/table_ddl_reconciler.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <string>

#include "common/status.h"
#include "meta/catalog/catalog_manager.h"
#include "meta/persist/meta_persist_engine.h"
#include "meta/service/sdm_client.h"
#include "sdm/model/store.h"

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

class TestableTableDdlReconciler : public TableDdlReconciler {
   public:
    using TableDdlReconciler::TableDdlReconciler;

    void run_once_for_test() { run(); }
};

class TableDdlReconcilerTest : public ::testing::Test {
   protected:
    void SetUp() override {
        test_dir_ = std::filesystem::temp_directory_path() /
                    ("table_ddl_reconciler_test_" + std::to_string(::getpid()));
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

    TableMeta create_table(CatalogManager& catalog, TableState state) {
        DBMeta db;
        EXPECT_TRUE(catalog.create_db({.db_name = db_name_, .zone = zone_}, &db)
                        .ok());

        TableMeta table;
        EXPECT_TRUE(catalog
                        .create_table({.db_name = db_name_,
                                       .table_name = table_name_,
                                       .shard_count = 4,
                                       .replica_count = 3,
                                       .resource_pool = resource_pool_},
                                      &table)
                        .ok());
        if (table.state != state) {
            EXPECT_TRUE(catalog.update_table_state(table.table_id, state).ok());
            EXPECT_TRUE(catalog.get_table_by_id(table.table_id, &table).ok());
        }
        return table;
    }

    SdmTableStatus sdm_status(sdm::TablePhase phase,
                              const std::string& error_msg = "") {
        return SdmTableStatus{.table_id = 0,
                              .desired = 0,
                              .phase = static_cast<int32_t>(phase),
                              .last_error_msg = error_msg,
                              .operation_id = ""};
    }

    std::filesystem::path test_dir_;
    const std::string db_name_{"commerce"};
    const std::string zone_{"zone-a"};
    const std::string table_name_{"orders"};
    const std::string resource_pool_{"pool-a"};
};

// 测试ADDING的table，sdm那边查不到table的时候，会重新提交place_table。
TEST_F(TableDdlReconcilerTest, AddingTableMissingInSdmResubmitsPlaceTable) {
    Fixture fixture{make_sub_dir("adding_missing")};
    TableMeta table = create_table(fixture.catalog, TableState::ADDING);
    FakeSdmClient client;
    client.get_table_status_status = Status::TABLE_NOT_FOUND("not found");
    TestableTableDdlReconciler reconciler{&fixture.catalog, &client};

    reconciler.run_once_for_test();

    EXPECT_EQ(client.get_table_status_calls, 1);
    EXPECT_EQ(client.place_table_calls, 1);
    EXPECT_EQ(client.last_place_table.table_id, table.table_id);

    TableMeta stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(table.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::ADDING);
}

// 测试ADDING的table，sdm还是CREATING的时候，meta这边先保持ADDING继续等。
TEST_F(TableDdlReconcilerTest, AddingTableCreatingInSdmKeepsAdding) {
    Fixture fixture{make_sub_dir("adding_creating")};
    TableMeta table = create_table(fixture.catalog, TableState::ADDING);
    FakeSdmClient client;
    client.table_status_result = sdm_status(sdm::TablePhase::CREATING);
    TestableTableDdlReconciler reconciler{&fixture.catalog, &client};

    reconciler.run_once_for_test();

    EXPECT_EQ(client.get_table_status_calls, 1);
    EXPECT_EQ(client.place_table_calls, 0);

    TableMeta stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(table.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::ADDING);
}

// 测试ADDING的table，sdm那边已经READY了，meta这边要同步成NORMAL。
TEST_F(TableDdlReconcilerTest, AddingTableReadyInSdmMarksNormal) {
    Fixture fixture{make_sub_dir("adding_ready")};
    TableMeta table = create_table(fixture.catalog, TableState::ADDING);
    FakeSdmClient client;
    client.table_status_result = sdm_status(sdm::TablePhase::READY);
    TestableTableDdlReconciler reconciler{&fixture.catalog, &client};

    reconciler.run_once_for_test();

    TableMeta stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(table.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::NORMAL);
    EXPECT_TRUE(stored.last_error_msg.empty());
}

// 测试ADDING的table，sdm那边已经FAILED了，meta这边也要变成FAILED并且记录err_msg。
TEST_F(TableDdlReconcilerTest, AddingTableFailedInSdmMarksFailedWithError) {
    Fixture fixture{make_sub_dir("adding_failed")};
    TableMeta table = create_table(fixture.catalog, TableState::ADDING);
    FakeSdmClient client;
    client.table_status_result =
        sdm_status(sdm::TablePhase::FAILED, "sdm placement failed");
    TestableTableDdlReconciler reconciler{&fixture.catalog, &client};

    reconciler.run_once_for_test();

    TableMeta stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(table.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::FAILED);
    EXPECT_EQ(stored.last_error_msg, "sdm placement failed");
}

// 测试ADDING的table，查询sdm状态失败了，meta这边还是ADDING，然后记录err_msg。
TEST_F(TableDdlReconcilerTest, AddingTableGetStatusFailureKeepsAddingWithError) {
    Fixture fixture{make_sub_dir("adding_get_status_failure")};
    TableMeta table = create_table(fixture.catalog, TableState::ADDING);
    FakeSdmClient client;
    client.get_table_status_status = Status::ERROR("get status failed");
    TestableTableDdlReconciler reconciler{&fixture.catalog, &client};

    reconciler.run_once_for_test();

    TableMeta stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(table.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::ADDING);
    EXPECT_NE(stored.last_error_msg.find("get status failed"),
              std::string::npos);
}

// 测试DROPPING的table，sdm那边查不到table的时候，meta这边要直接变成DELETED。
TEST_F(TableDdlReconcilerTest, DroppingTableMissingInSdmMarksDeleted) {
    Fixture fixture{make_sub_dir("dropping_missing")};
    TableMeta table = create_table(fixture.catalog, TableState::DROPPING);
    FakeSdmClient client;
    client.get_table_status_status = Status::TABLE_NOT_FOUND("not found");
    TestableTableDdlReconciler reconciler{&fixture.catalog, &client};

    reconciler.run_once_for_test();

    EXPECT_EQ(client.drop_table_calls, 0);
    TableMeta stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(table.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::DELETED);
}

// 测试DROPPING的table，sdm那边还是DELETING的时候，meta这边先保持DROPPING继续等。
TEST_F(TableDdlReconcilerTest, DroppingTableDeletingInSdmKeepsDropping) {
    Fixture fixture{make_sub_dir("dropping_deleting")};
    TableMeta table = create_table(fixture.catalog, TableState::DROPPING);
    FakeSdmClient client;
    client.table_status_result = sdm_status(sdm::TablePhase::DELETING);
    TestableTableDdlReconciler reconciler{&fixture.catalog, &client};

    reconciler.run_once_for_test();

    EXPECT_EQ(client.drop_table_calls, 0);
    TableMeta stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(table.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::DROPPING);
}

// 测试DROPPING的table，sdm那边已经DELETED了，meta这边也要同步成DELETED。
TEST_F(TableDdlReconcilerTest, DroppingTableDeletedInSdmMarksDeleted) {
    Fixture fixture{make_sub_dir("dropping_deleted")};
    TableMeta table = create_table(fixture.catalog, TableState::DROPPING);
    FakeSdmClient client;
    client.table_status_result = sdm_status(sdm::TablePhase::DELETED);
    TestableTableDdlReconciler reconciler{&fixture.catalog, &client};

    reconciler.run_once_for_test();

    TableMeta stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(table.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::DELETED);
}

// 测试DROPPING的table，sdm那边还是READY的时候，会重新提交drop_table。
TEST_F(TableDdlReconcilerTest, DroppingTableReadyInSdmResubmitsDropTable) {
    Fixture fixture{make_sub_dir("dropping_ready")};
    TableMeta table = create_table(fixture.catalog, TableState::DROPPING);
    FakeSdmClient client;
    client.table_status_result = sdm_status(sdm::TablePhase::READY);
    TestableTableDdlReconciler reconciler{&fixture.catalog, &client};

    reconciler.run_once_for_test();

    EXPECT_EQ(client.drop_table_calls, 1);
    EXPECT_EQ(client.last_drop_table.table_id, table.table_id);
    TableMeta stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(table.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::DROPPING);
}

// 测试DROPPING的table，sdm那边已经FAILED了，meta这边也要变成FAILED并且记录err_msg。
TEST_F(TableDdlReconcilerTest, DroppingTableFailedInSdmMarksFailedWithError) {
    Fixture fixture{make_sub_dir("dropping_failed")};
    TableMeta table = create_table(fixture.catalog, TableState::DROPPING);
    FakeSdmClient client;
    client.table_status_result =
        sdm_status(sdm::TablePhase::FAILED, "sdm drop failed");
    TestableTableDdlReconciler reconciler{&fixture.catalog, &client};

    reconciler.run_once_for_test();

    TableMeta stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(table.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::FAILED);
    EXPECT_EQ(stored.last_error_msg, "sdm drop failed");
}

}  // namespace

}  // namespace adviskv::meta