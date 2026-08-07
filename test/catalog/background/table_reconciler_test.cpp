#include "catalog/background/table_reconciler.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <string>

#include "common/status.h"
#include "catalog/store/catalog_store.h"
#include "catalog/persist/catalog_persist_engine.h"
#include "catalog/proto/catalog_table_state_proto.h"
#include "catalog/service/topo_client.h"
#include "topo/model/model.h"

namespace adviskv::catalog {
namespace {

class FakeTopoClient : public ITopoClient {
   public:
    Status call_place_table(const TableDesc& table_desc) override {
        ++place_table_calls;
        last_place_table = table_desc;
        return place_table_status;
    }

    Status call_drop_table(const TableDesc& table_desc) override {
        ++drop_table_calls;
        last_drop_table = table_desc;
        return drop_table_status;
    }

    Status call_alter_table_replica_count(
        const TableDesc& table_desc) override {
        ++alter_table_calls;
        last_alter_table = table_desc;
        return alter_table_status;
    }

    Status get_table_status(const TableDesc& table_desc,
                            TopoTableStatus* table_status) override {
        ++get_table_status_calls;
        last_get_table = table_desc;
        if (table_status) {
            *table_status = table_status_result;
        }
        return get_table_status_status;
    }

    Status place_table_status{Status::OK()};
    Status drop_table_status{Status::OK()};
    Status alter_table_status{Status::OK()};
    Status get_table_status_status{Status::OK()};
    TopoTableStatus table_status_result;

    int place_table_calls{0};
    int drop_table_calls{0};
    int alter_table_calls{0};
    int get_table_status_calls{0};

    TableDesc last_place_table;
    TableDesc last_drop_table;
    TableDesc last_alter_table;
    TableDesc last_get_table;
};

class TestableTableReconciler : public TableReconciler {
   public:
    using TableReconciler::TableReconciler;

    void run_once_for_test() { run(); }
};

class TableReconcilerTest : public ::testing::Test {
   protected:
    void SetUp() override {
        test_dir_ = std::filesystem::temp_directory_path() /
                    ("table_reconciler_test_" + std::to_string(::getpid()));
        std::filesystem::create_directories(test_dir_);
    }

    void TearDown() override { std::filesystem::remove_all(test_dir_); }

    std::filesystem::path make_sub_dir(const std::string& name) {
        auto dir = test_dir_ / name;
        std::filesystem::create_directories(dir);
        return dir;
    }

    struct Fixture {
        CatalogPersistEngine engine;
        CatalogStore catalog;

        explicit Fixture(const std::filesystem::path& dir)
            : engine(dir.string()), catalog(&engine) {
            EXPECT_TRUE(engine.init().ok());
            EXPECT_TRUE(catalog.init().ok());
        }
    };

    TableDesc create_table(CatalogStore& catalog, TableState state) {
        DatabaseDesc db;
        EXPECT_TRUE(catalog.create_db(CreateDBParam{db_name_, zone_}, &db)
                        .ok());

        TableDesc table;
        EXPECT_TRUE(catalog
                        .create_table(CreateTableParam{db_name_,
                                                           table_name_,
                                                           4,
                                                           3,
                                                           resource_pool_},
                                      &table)
                        .ok());
        if (table.state != state) {
            EXPECT_TRUE(catalog.update_table_state(table.table_id, state).ok());
            EXPECT_TRUE(catalog.get_table_by_id(table.table_id, &table).ok());
        }
        return table;
    }

    TopoTableStatus topo_status(TopoTablePhase phase,
                              const std::string& error_msg = "") {
        TopoTableStatus status{};
        status.table_id = 0;
        status.desired = TopoTableDesired::PRESENT;
        status.phase = phase;
        status.last_error_msg = error_msg;
        return status;
    }

    std::filesystem::path test_dir_;
    const std::string db_name_{"commerce"};
    const std::string zone_{"zone-a"};
    const std::string table_name_{"orders"};
    const std::string resource_pool_{"pool-a"};
};

// 测试ADDING的table，topo那边查不到table的时候，会重新提交place_table。
TEST_F(TableReconcilerTest, AddingTableMissingInTopoResubmitsPlaceTable) {
    Fixture fixture{make_sub_dir("adding_missing")};
    TableDesc table = create_table(fixture.catalog, TableState::ADDING);
    FakeTopoClient client;
    client.get_table_status_status = Status::TABLE_NOT_FOUND("not found");
    TestableTableReconciler reconciler{&fixture.catalog, &client};

    reconciler.run_once_for_test();

    EXPECT_EQ(client.get_table_status_calls, 1);
    EXPECT_EQ(client.place_table_calls, 1);
    EXPECT_EQ(client.last_place_table.table_id, table.table_id);

    TableDesc stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(table.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::ADDING);
}

// 测试ADDING的table，topo还是CREATING的时候，catalog这边先保持ADDING继续等。
TEST_F(TableReconcilerTest, AddingTableCreatingInTopoKeepsAdding) {
    Fixture fixture{make_sub_dir("adding_creating")};
    TableDesc table = create_table(fixture.catalog, TableState::ADDING);
    FakeTopoClient client;
    client.table_status_result = topo_status(TopoTablePhase::CREATING);
    TestableTableReconciler reconciler{&fixture.catalog, &client};

    reconciler.run_once_for_test();

    EXPECT_EQ(client.get_table_status_calls, 1);
    EXPECT_EQ(client.place_table_calls, 0);

    TableDesc stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(table.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::ADDING);
}

// 测试 ADDING 的 table 在 TOPO 仍为 CREATING 时保持 ADDING，并保留已有错误信息。
TEST_F(TableReconcilerTest, AddingTableCreatingInTopoKeepsPreviousError) {
    Fixture fixture{make_sub_dir("adding_creating_keep_error")};
    TableDesc table = create_table(fixture.catalog, TableState::ADDING);
    const std::string previous_error = "previous place table rpc failed";
    ASSERT_TRUE(fixture.catalog
                    .update_table_state(table.table_id, TableState::ADDING,
                                        previous_error)
                    .ok());

    FakeTopoClient client;
    client.table_status_result = topo_status(TopoTablePhase::CREATING);
    TestableTableReconciler reconciler{&fixture.catalog, &client};

    reconciler.run_once_for_test();

    TableDesc stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(table.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::ADDING);
    EXPECT_EQ(stored.last_error_msg, previous_error);
}

// 测试ADDING的table，topo那边已经READY了，catalog这边要同步成NORMAL。
TEST_F(TableReconcilerTest, AddingTableReadyInTopoMarksNormal) {
    Fixture fixture{make_sub_dir("adding_ready")};
    TableDesc table = create_table(fixture.catalog, TableState::ADDING);
    FakeTopoClient client;
    client.table_status_result = topo_status(TopoTablePhase::READY);
    TestableTableReconciler reconciler{&fixture.catalog, &client};

    reconciler.run_once_for_test();

    TableDesc stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(table.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::NORMAL);
    EXPECT_TRUE(stored.last_error_msg.empty());
}

// 测试ADDING的table，topo那边已经FAILED了，catalog这边也要变成FAILED并且记录err_msg。
TEST_F(TableReconcilerTest, AddingTableFailedInTopoMarksFailedWithError) {
    Fixture fixture{make_sub_dir("adding_failed")};
    TableDesc table = create_table(fixture.catalog, TableState::ADDING);
    FakeTopoClient client;
    client.table_status_result =
        topo_status(TopoTablePhase::FAILED, "topo placement failed");
    TestableTableReconciler reconciler{&fixture.catalog, &client};

    reconciler.run_once_for_test();

    TableDesc stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(table.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::FAILED);
    EXPECT_EQ(stored.last_error_msg, "topo placement failed");
}

// 测试ADDING的table，查询topo状态失败了，catalog这边还是ADDING，然后记录err_msg。
TEST_F(TableReconcilerTest, AddingTableGetStatusFailureKeepsAddingWithError) {
    Fixture fixture{make_sub_dir("adding_get_status_failure")};
    TableDesc table = create_table(fixture.catalog, TableState::ADDING);
    FakeTopoClient client;
    client.get_table_status_status = Status::ERROR("get status failed");
    TestableTableReconciler reconciler{&fixture.catalog, &client};

    reconciler.run_once_for_test();

    TableDesc stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(table.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::ADDING);
    EXPECT_NE(stored.last_error_msg.find("get status failed"),
              std::string::npos);
}

// 测试 ADDING 的 table 在 TOPO 查不到后重新提交成功时，仍保留之前的错误信息。
TEST_F(TableReconcilerTest, AddingTableResubmitSuccessKeepsPreviousError) {
    Fixture fixture{make_sub_dir("adding_resubmit_keep_error")};
    TableDesc table = create_table(fixture.catalog, TableState::ADDING);
    const std::string previous_error = "previous place table rpc failed";
    ASSERT_TRUE(fixture.catalog
                    .update_table_state(table.table_id, TableState::ADDING,
                                        previous_error)
                    .ok());

    FakeTopoClient client;
    client.get_table_status_status = Status::TABLE_NOT_FOUND("not found");
    TestableTableReconciler reconciler{&fixture.catalog, &client};

    reconciler.run_once_for_test();

    EXPECT_EQ(client.place_table_calls, 1);
    TableDesc stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(table.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::ADDING);
    EXPECT_EQ(stored.last_error_msg, previous_error);
}

// 测试 ALTERING 的 table 在 TOPO 查不到时，Catalog 侧会标记为 FAILED。
TEST_F(TableReconcilerTest, AlteringTableMissingInTopoMarksFailed) {
    Fixture fixture{make_sub_dir("altering_missing")};
    TableDesc table = create_table(fixture.catalog, TableState::ALTERING);
    FakeTopoClient client;
    client.get_table_status_status = Status::TABLE_NOT_FOUND("not found");
    TestableTableReconciler reconciler{&fixture.catalog, &client};

    reconciler.run_once_for_test();

    EXPECT_EQ(client.get_table_status_calls, 1);
    EXPECT_EQ(client.alter_table_calls, 0);

    TableDesc stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(table.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::FAILED);
    EXPECT_EQ(stored.last_error_msg, "TOPO table alter failed");
}

// 测试 ALTERING 的 table 在 TOPO 仍为 RESIZING 时，Catalog 侧继续保持 ALTERING。
TEST_F(TableReconcilerTest, AlteringTableResizingInTopoKeepsAltering) {
    Fixture fixture{make_sub_dir("altering_resizing")};
    TableDesc table = create_table(fixture.catalog, TableState::ALTERING);
    FakeTopoClient client;
    client.table_status_result = topo_status(TopoTablePhase::RESIZING);
    TestableTableReconciler reconciler{&fixture.catalog, &client};

    reconciler.run_once_for_test();

    EXPECT_EQ(client.get_table_status_calls, 1);
    EXPECT_EQ(client.alter_table_calls, 0);

    TableDesc stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(table.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::ALTERING);
}

// 测试 ALTERING 的 table 在 TOPO 已 READY 时，Catalog 侧同步为 NORMAL。
TEST_F(TableReconcilerTest, AlteringTableReadyInTopoMarksNormal) {
    Fixture fixture{make_sub_dir("altering_ready")};
    TableDesc table = create_table(fixture.catalog, TableState::ALTERING);
    FakeTopoClient client;
    client.table_status_result = topo_status(TopoTablePhase::READY);
    TestableTableReconciler reconciler{&fixture.catalog, &client};

    reconciler.run_once_for_test();

    TableDesc stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(table.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::NORMAL);
    EXPECT_TRUE(stored.last_error_msg.empty());
}

// 测试DROPPING的table，topo那边查不到table的时候，catalog这边要直接变成DELETED。
TEST_F(TableReconcilerTest, DroppingTableMissingInTopoMarksDeleted) {
    Fixture fixture{make_sub_dir("dropping_missing")};
    TableDesc table = create_table(fixture.catalog, TableState::DROPPING);
    FakeTopoClient client;
    client.get_table_status_status = Status::TABLE_NOT_FOUND("not found");
    TestableTableReconciler reconciler{&fixture.catalog, &client};

    reconciler.run_once_for_test();

    EXPECT_EQ(client.drop_table_calls, 0);
    TableDesc stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(table.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::DELETED);
}

// 测试DROPPING的table，topo那边还是DELETING的时候，catalog这边先保持DROPPING继续等。
TEST_F(TableReconcilerTest, DroppingTableDeletingInTopoKeepsDropping) {
    Fixture fixture{make_sub_dir("dropping_deleting")};
    TableDesc table = create_table(fixture.catalog, TableState::DROPPING);
    FakeTopoClient client;
    client.table_status_result = topo_status(TopoTablePhase::DELETING);
    TestableTableReconciler reconciler{&fixture.catalog, &client};

    reconciler.run_once_for_test();

    EXPECT_EQ(client.drop_table_calls, 0);
    TableDesc stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(table.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::DROPPING);
}

// 测试DROPPING的table，topo那边已经DELETED了，catalog这边也要同步成DELETED。
TEST_F(TableReconcilerTest, DroppingTableDeletedInTopoMarksDeleted) {
    Fixture fixture{make_sub_dir("dropping_deleted")};
    TableDesc table = create_table(fixture.catalog, TableState::DROPPING);
    FakeTopoClient client;
    client.table_status_result = topo_status(TopoTablePhase::DELETED);
    TestableTableReconciler reconciler{&fixture.catalog, &client};

    reconciler.run_once_for_test();

    TableDesc stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(table.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::DELETED);
}

// 测试DROPPING的table，topo那边还是READY的时候，会重新提交drop_table。
TEST_F(TableReconcilerTest, DroppingTableReadyInTopoResubmitsDropTable) {
    Fixture fixture{make_sub_dir("dropping_ready")};
    TableDesc table = create_table(fixture.catalog, TableState::DROPPING);
    FakeTopoClient client;
    client.table_status_result = topo_status(TopoTablePhase::READY);
    TestableTableReconciler reconciler{&fixture.catalog, &client};

    reconciler.run_once_for_test();

    EXPECT_EQ(client.drop_table_calls, 1);
    EXPECT_EQ(client.last_drop_table.table_id, table.table_id);
    TableDesc stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(table.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::DROPPING);
}

// 测试DROPPING的table，topo那边已经FAILED了，catalog这边也要变成FAILED并且记录err_msg。
TEST_F(TableReconcilerTest, DroppingTableFailedInTopoMarksFailedWithError) {
    Fixture fixture{make_sub_dir("dropping_failed")};
    TableDesc table = create_table(fixture.catalog, TableState::DROPPING);
    FakeTopoClient client;
    client.table_status_result =
        topo_status(TopoTablePhase::FAILED, "topo drop failed");
    TestableTableReconciler reconciler{&fixture.catalog, &client};

    reconciler.run_once_for_test();

    TableDesc stored;
    ASSERT_TRUE(fixture.catalog.get_table_by_id(table.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::FAILED);
    EXPECT_EQ(stored.last_error_msg, "topo drop failed");
}

}  // namespace

}  // namespace adviskv::catalog
