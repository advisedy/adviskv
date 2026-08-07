#include "catalog/store/catalog_store.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <string>

#include "common/status.h"
#include "catalog/model/model.h"
#include "catalog/persist/i_catalog_persist_engine.h"
#include "catalog/persist/catalog_persist_engine.h"

namespace adviskv::catalog {
namespace {

class FaultyCatalogPersistEngine : public ICatalogPersistEngine {
   public:
    Status init() override { return Status::OK(); }
    Status close() override { return Status::OK(); }
    Status save_catalog(const CatalogPersistedRecord&) override {
        return Status::ERROR("inject save_catalog failure");
    }
    Status load_catalog(CatalogPersistedRecord& record) override {
        record = {};
        return Status::OK();
    }
};

class CatalogStoreTest : public ::testing::Test {
   protected:
    void SetUp() override {
        test_dir_ = std::filesystem::temp_directory_path() /
                    ("catalog_mgr_test_" + std::to_string(::getpid()));
        std::filesystem::create_directories(test_dir_);
    }

    void TearDown() override { std::filesystem::remove_all(test_dir_); }

    std::filesystem::path make_sub_dir(const std::string& name) {
        auto dir = test_dir_ / name;
        std::filesystem::create_directories(dir);
        return dir;
    }

    std::filesystem::path test_dir_;
};

// 测试正常创建db和table的功能
TEST_F(CatalogStoreTest, CreateDbAndTableBasic) {
    auto dir = make_sub_dir("basic_create");

    CatalogPersistEngine engine(dir.string());
    ASSERT_TRUE(engine.init().ok());
    CatalogStore catalog(&engine);
    ASSERT_TRUE(catalog.init().ok());

    DatabaseDesc db_desc;
    ASSERT_TRUE(
        catalog.create_db(CreateDBParam{"test_db", "zone-1"}, &db_desc)
            .ok());
    EXPECT_EQ(db_desc.db_id, 0);
    EXPECT_EQ(db_desc.db_name, "test_db");
    EXPECT_EQ(db_desc.zone, "zone-1");

    DatabaseDesc db_desc2;
    ASSERT_TRUE(
        catalog.create_db(CreateDBParam{"test_db2", "zone-2"}, &db_desc2)
            .ok());
    EXPECT_EQ(db_desc2.db_id, 1);
    EXPECT_EQ(db_desc2.db_name, "test_db2");

    TableDesc table_desc;
    ASSERT_TRUE(catalog
                    .create_table(CreateTableParam{"test_db", "users", 4, 3,
                                                       "pool-a"},
                                  &table_desc)
                    .ok());
    EXPECT_EQ(table_desc.table_id, 0);
    EXPECT_EQ(table_desc.db_name, "test_db");
    EXPECT_EQ(table_desc.table_name, "users");
    EXPECT_EQ(table_desc.shard_count, 4);
    EXPECT_EQ(table_desc.replica_count, 3);
    EXPECT_EQ(table_desc.db_id, db_desc.db_id);
    EXPECT_EQ(table_desc.resource_pool, "pool-a");
    EXPECT_EQ(table_desc.state, TableState::ADDING);
    EXPECT_FALSE(table_desc.operation_id.empty());
    EXPECT_TRUE(table_desc.last_error_msg.empty());
    EXPECT_GT(table_desc.create_ts, 0);
    EXPECT_GT(table_desc.update_ts, 0);

    TableDesc table_desc2;
    ASSERT_TRUE(catalog
                    .create_table(CreateTableParam{"test_db", "orders", 8,
                                                       2, "pool-b"},
                                  &table_desc2)
                    .ok());
    EXPECT_EQ(table_desc2.table_id, 1);
    EXPECT_EQ(table_desc2.table_name, "orders");
    EXPECT_EQ(table_desc2.shard_count, 8);
    EXPECT_EQ(table_desc2.replica_count, 2);

    DatabaseDesc got_db;
    ASSERT_TRUE(catalog.get_db("test_db", &got_db).ok());
    EXPECT_EQ(got_db.db_id, db_desc.db_id);
    EXPECT_EQ(got_db.db_name, "test_db");
    EXPECT_EQ(got_db.zone, "zone-1");

    TableDesc got_table;
    ASSERT_TRUE(catalog.get_table_by_name("test_db", "users", &got_table).ok());
    EXPECT_EQ(got_table.table_id, 0);
    EXPECT_EQ(got_table.table_name, "users");
    EXPECT_EQ(got_table.shard_count, 4);
    EXPECT_EQ(got_table.replica_count, 3);
    EXPECT_EQ(got_table.resource_pool, "pool-a");
    EXPECT_EQ(got_table.db_name, "test_db");
    EXPECT_EQ(got_table.db_id, db_desc.db_id);
    EXPECT_EQ(got_table.state, TableState::ADDING);
    EXPECT_EQ(got_table.operation_id, table_desc.operation_id);

    std::vector<TableDesc> tables;
    ASSERT_TRUE(catalog.list_tables("test_db", &tables).ok());
    EXPECT_EQ(tables.size(), 2u);

    ASSERT_TRUE(
        catalog.update_table_state(table_desc.table_id, TableState::NORMAL)
            .ok());
    ASSERT_TRUE(catalog.get_table_by_name("test_db", "users", &got_table).ok());
    EXPECT_EQ(got_table.state, TableState::NORMAL);

    std::vector<TableDesc> adding_tables;
    ASSERT_TRUE(
        catalog.list_tables_by_state(TableState::ADDING, &adding_tables).ok());
    EXPECT_EQ(adding_tables.size(), 1U);
    EXPECT_EQ(adding_tables[0].table_id, table_desc2.table_id);

    // 这次再插入一次db和table，期待的结果是错误码是已经存在了。
}

// 测试普通删表只允许 NORMAL 表进入 DROPPING，避免创建中表被提前落成删除状态。
TEST_F(CatalogStoreTest, DeleteTableRejectsNonNormalTable) {
    auto dir = make_sub_dir("delete_rejects_non_normal");

    CatalogPersistEngine engine(dir.string());
    ASSERT_TRUE(engine.init().ok());
    CatalogStore catalog(&engine);
    ASSERT_TRUE(catalog.init().ok());

    ASSERT_TRUE(
        catalog.create_db(CreateDBParam{"commerce", "z1"}, nullptr).ok());
    TableDesc table;
    ASSERT_TRUE(catalog
                    .create_table(CreateTableParam{"commerce", "orders", 4,
                                                       3, "pool-a"},
                                  &table)
                    .ok());
    ASSERT_EQ(table.state, TableState::ADDING);

    TableDesc dropped;
    Status status = catalog.delete_table(table.table_id, &dropped);

    EXPECT_EQ(status.code(), StatusCode::INVALID_ARGUMENT);
    TableDesc stored;
    ASSERT_TRUE(catalog.get_table_by_id(table.table_id, &stored).ok());
    EXPECT_EQ(stored.state, TableState::ADDING);
    EXPECT_EQ(stored.operation_id, table.operation_id);

    ASSERT_TRUE(
        catalog.update_table_state(table.table_id, TableState::NORMAL).ok());
    ASSERT_TRUE(catalog.delete_table(table.table_id, &dropped).ok());
    EXPECT_EQ(dropped.state, TableState::DROPPING);
    EXPECT_NE(dropped.operation_id, table.operation_id);
}

// 测试一下当catalog_manager这边创建db失败了（因为persist）之后，
// 查询db或者table是否还会存在
TEST_F(CatalogStoreTest, CreateDbFailsDueToPersist_DbShouldNotExist) {
    FaultyCatalogPersistEngine faulty;

    CatalogStore catalog(&faulty);
    ASSERT_TRUE(catalog.init().ok());

    DatabaseDesc db_desc;
    Status status =
        catalog.create_db(CreateDBParam{"fail_db", "z1"}, &db_desc);
    EXPECT_TRUE(status.fail());

    DatabaseDesc check_db_desc;
    EXPECT_EQ(catalog.get_db("fail_db", &check_db_desc).code(),
              StatusCode::DB_NOT_FOUND);
}

// 测试一下当catalog_manager这边创建table失败了（因为persist）之后，
// 查询table是否还会存在
TEST_F(CatalogStoreTest, CreateTableFailsDueToPersist_TableShouldNotExist) {
    auto dir = make_sub_dir("persist_fail_table");

    CatalogPersistEngine engine(dir.string());
    ASSERT_TRUE(engine.init().ok());
    CatalogStore catalog(&engine);
    ASSERT_TRUE(catalog.init().ok());

    DatabaseDesc db_desc;
    ASSERT_TRUE(catalog.create_db(CreateDBParam{"mydb", "z1"}, &db_desc).ok());

    TableDesc table_desc;
    ASSERT_TRUE(catalog
                    .create_table(CreateTableParam{"mydb", "existing_table", 4,
                                                   3, "pool-a"},
                                  &table_desc)
                    .ok());

    FaultyCatalogPersistEngine faulty;
    CatalogStore faulty_catalog(&faulty);
    ASSERT_TRUE(faulty_catalog.init().ok());

    TableDesc fail_table_desc;
    Status status = faulty_catalog.create_table(
        CreateTableParam{"mydb", "fail_table", 2, 1, "pool-b"},
        &fail_table_desc);
    EXPECT_TRUE(status.fail());

    TableDesc check_table_desc;
    EXPECT_EQ(
        faulty_catalog.get_table_by_name("mydb", "fail_table", &check_table_desc)
            .code(),
        StatusCode::DB_NOT_FOUND);
}

// 测试 catalog 层 alter replica_count 会更新表副本数，并把表状态标记为 ALTERING。
TEST_F(CatalogStoreTest, AlterTableReplicaCountMarksAltering) {
    auto dir = make_sub_dir("alter_replica_count");

    CatalogPersistEngine engine(dir.string());
    ASSERT_TRUE(engine.init().ok());
    CatalogStore catalog(&engine);
    ASSERT_TRUE(catalog.init().ok());

    DatabaseDesc db_desc;
    ASSERT_TRUE(catalog.create_db(CreateDBParam{"mydb", "z1"}, &db_desc).ok());

    TableDesc table_desc;
    ASSERT_TRUE(
        catalog
            .create_table(CreateTableParam{"mydb", "users", 4, 3, "pool-a"},
                          &table_desc)
            .ok());
    ASSERT_TRUE(
        catalog.update_table_state(table_desc.table_id, TableState::NORMAL)
            .ok());

    TableDesc altered;
    ASSERT_TRUE(catalog
                    .alter_table_replica_count(
                        AlterTableReplicaCountParam{table_desc.db_name,
                                                    table_desc.table_name, 5},
                        &altered)
                    .ok());

    EXPECT_EQ(altered.table_id, table_desc.table_id);
    EXPECT_EQ(altered.replica_count, 5);
    EXPECT_EQ(altered.state, TableState::ALTERING);
    EXPECT_FALSE(altered.operation_id.empty());
    EXPECT_TRUE(altered.last_error_msg.empty());

    TableDesc stored;
    ASSERT_TRUE(catalog.get_table_by_id(table_desc.table_id, &stored).ok());
    EXPECT_EQ(stored.replica_count, 5);
    EXPECT_EQ(stored.state, TableState::ALTERING);
}

// 测试一下catalog的list_tables，lookup_db_by_name，lookup_table_by_name是否有问题
TEST_F(CatalogStoreTest, ListTablesAndGetQueries) {
    auto dir = make_sub_dir("query_test");

    CatalogPersistEngine engine(dir.string());
    ASSERT_TRUE(engine.init().ok());
    CatalogStore catalog(&engine);
    ASSERT_TRUE(catalog.init().ok());

    ASSERT_TRUE(
        catalog.create_db(CreateDBParam{"db1", "z1"}, nullptr).ok());
    ASSERT_TRUE(
        catalog.create_db(CreateDBParam{"db2", "z2"}, nullptr).ok());

    ASSERT_TRUE(
        catalog
            .create_table(CreateTableParam{"db1", "table1", 4, 3, "pool-a"},
                          nullptr)
            .ok());
    ASSERT_TRUE(
        catalog
            .create_table(CreateTableParam{"db1", "table2", 2, 1, "pool-b"},
                          nullptr)
            .ok());
    ASSERT_TRUE(
        catalog
            .create_table(CreateTableParam{"db2", "table3", 8, 2, "pool-c"},
                          nullptr)
            .ok());

    DatabaseDesc got_db1;
    ASSERT_TRUE(catalog.get_db("db1", &got_db1).ok());
    EXPECT_EQ(got_db1.db_name, "db1");
    EXPECT_EQ(got_db1.zone, "z1");

    DatabaseDesc got_db2;
    ASSERT_TRUE(catalog.get_db("db2", &got_db2).ok());
    EXPECT_EQ(got_db2.db_name, "db2");
    EXPECT_EQ(got_db2.zone, "z2");

    TableDesc got_t1;
    ASSERT_TRUE(catalog.get_table_by_name("db1", "table1", &got_t1).ok());
    EXPECT_EQ(got_t1.table_name, "table1");
    EXPECT_EQ(got_t1.shard_count, 4);
    EXPECT_EQ(got_t1.db_name, "db1");

    TableDesc got_t3;
    ASSERT_TRUE(catalog.get_table_by_name("db2", "table3", &got_t3).ok());
    EXPECT_EQ(got_t3.table_name, "table3");
    EXPECT_EQ(got_t3.shard_count, 8);

    std::vector<TableDesc> tables_in_db1;
    ASSERT_TRUE(catalog.list_tables("db1", &tables_in_db1).ok());
    EXPECT_EQ(tables_in_db1.size(), 2u);
    std::vector<std::string> names_db1;
    for (const auto& t : tables_in_db1) {
        names_db1.push_back(t.table_name);
    }
    EXPECT_NE(std::find(names_db1.begin(), names_db1.end(), "table1"),
              names_db1.end());
    EXPECT_NE(std::find(names_db1.begin(), names_db1.end(), "table2"),
              names_db1.end());

    std::vector<TableDesc> tables_in_db2;
    ASSERT_TRUE(catalog.list_tables("db2", &tables_in_db2).ok());
    EXPECT_EQ(tables_in_db2.size(), 1u);
    EXPECT_EQ(tables_in_db2[0].table_name, "table3");

    DatabaseDesc db3;
    ASSERT_TRUE(
        catalog.create_db(CreateDBParam{"empty_db", "z3"}, &db3).ok());
    std::vector<TableDesc> empty_tables;
    ASSERT_TRUE(catalog.list_tables("empty_db", &empty_tables).ok());
    EXPECT_TRUE(empty_tables.empty());
}

// 测试catalog_manager持久化之后，再次恢复的数据是否一致
TEST_F(CatalogStoreTest, PersistAndRecoverDataConsistency) {
    auto dir = make_sub_dir("recover_test");

    {
        CatalogPersistEngine engine(dir.string());
        ASSERT_TRUE(engine.init().ok());
        CatalogStore catalog(&engine);
        ASSERT_TRUE(catalog.init().ok());

        ASSERT_TRUE(
            catalog.create_db(CreateDBParam{"db_alpha", "zone-a"}, nullptr)
                .ok());
        ASSERT_TRUE(
            catalog.create_db(CreateDBParam{"db_beta", "zone-b"}, nullptr)
                .ok());

        ASSERT_TRUE(catalog
                        .create_table(CreateTableParam{"db_alpha", "users",
                                                           4, 3, "pool-a"},
                                      nullptr)
                        .ok());
        ASSERT_TRUE(catalog
                        .create_table(CreateTableParam{"db_alpha", "orders",
                                                           8, 2, "pool-b"},
                                      nullptr)
                        .ok());
        ASSERT_TRUE(
            catalog
                .create_table(
                    CreateTableParam{"db_beta", "products", 2, 1, "pool-c"},
                    nullptr)
                .ok());
    }

    {
        CatalogPersistEngine engine(dir.string());
        ASSERT_TRUE(engine.init().ok());
        CatalogStore catalog(&engine);
        ASSERT_TRUE(catalog.init().ok());

        DatabaseDesc got_db1, got_db2;
        ASSERT_TRUE(catalog.get_db("db_alpha", &got_db1).ok());
        EXPECT_EQ(got_db1.db_name, "db_alpha");
        EXPECT_EQ(got_db1.zone, "zone-a");

        ASSERT_TRUE(catalog.get_db("db_beta", &got_db2).ok());
        EXPECT_EQ(got_db2.db_name, "db_beta");
        EXPECT_EQ(got_db2.zone, "zone-b");

        std::vector<TableDesc> alpha_tables;
        ASSERT_TRUE(catalog.list_tables("db_alpha", &alpha_tables).ok());
        EXPECT_EQ(alpha_tables.size(), 2u);

        TableDesc got_users;
        ASSERT_TRUE(
            catalog.get_table_by_name("db_alpha", "users", &got_users).ok());
        EXPECT_EQ(got_users.table_name, "users");
        EXPECT_EQ(got_users.shard_count, 4);
        EXPECT_EQ(got_users.replica_count, 3);
        EXPECT_EQ(got_users.resource_pool, "pool-a");
        EXPECT_EQ(got_users.db_name, "db_alpha");
        EXPECT_EQ(got_users.state, TableState::ADDING);

        TableDesc got_orders;
        ASSERT_TRUE(
            catalog.get_table_by_name("db_alpha", "orders", &got_orders).ok());
        EXPECT_EQ(got_orders.table_name, "orders");
        EXPECT_EQ(got_orders.shard_count, 8);
        EXPECT_EQ(got_orders.replica_count, 2);
        EXPECT_EQ(got_orders.resource_pool, "pool-b");

        std::vector<TableDesc> beta_tables;
        ASSERT_TRUE(catalog.list_tables("db_beta", &beta_tables).ok());
        EXPECT_EQ(beta_tables.size(), 1u);

        TableDesc got_products;
        ASSERT_TRUE(
            catalog.get_table_by_name("db_beta", "products", &got_products)
                .ok());
        EXPECT_EQ(got_products.table_name, "products");
        EXPECT_EQ(got_products.shard_count, 2);
        EXPECT_EQ(got_products.replica_count, 1);
        EXPECT_EQ(got_products.resource_pool, "pool-c");

        DatabaseDesc new_db;
        ASSERT_TRUE(
            catalog.create_db(CreateDBParam{"db_gamma", "zone-c"}, &new_db)
                .ok());
        EXPECT_EQ(new_db.db_id, 2);

        TableDesc new_table;
        ASSERT_TRUE(
            catalog
                .create_table(CreateTableParam{"db_gamma", "new_table", 1,
                                                   1, "pool-d"},
                              &new_table)
                .ok());
        EXPECT_EQ(new_table.table_id, 3);
    }
}

// 测试关于创建一个table了之后删除他，然后查找table是否找得到
TEST_F(CatalogStoreTest, DeletedTableRecreateSameNameUsesNewTableId) {
    auto dir = make_sub_dir("deleted_recreate_same_name");

    CatalogPersistEngine engine(dir.string());
    ASSERT_TRUE(engine.init().ok());
    CatalogStore catalog(&engine);
    ASSERT_TRUE(catalog.init().ok());

    ASSERT_TRUE(
        catalog.create_db(CreateDBParam{"commerce", "z1"}, nullptr).ok());

    TableDesc old_table;
    ASSERT_TRUE(catalog
                    .create_table(CreateTableParam{"commerce", "orders", 4,
                                                       3, "pool-a"},
                                  &old_table)
                    .ok());
    ASSERT_EQ(old_table.table_id, 0);

    ASSERT_TRUE(
        catalog.update_table_state(old_table.table_id, TableState::DELETED)
            .ok());

    TableDesc by_id;
    ASSERT_TRUE(catalog.get_table_by_id(old_table.table_id, &by_id).ok());
    EXPECT_EQ(by_id.state, TableState::DELETED);
    EXPECT_EQ(catalog.get_table_by_name("commerce", "orders", &by_id).code(),
              StatusCode::TABLE_NOT_FOUND);

    std::vector<TableDesc> tables;
    ASSERT_TRUE(catalog.list_tables("commerce", &tables).ok());
    EXPECT_TRUE(tables.empty());

    TableDesc new_table;
    ASSERT_TRUE(catalog
                    .create_table(CreateTableParam{"commerce", "orders", 8,
                                                       2, "pool-b"},
                                  &new_table)
                    .ok());
    EXPECT_EQ(new_table.table_id, 1);

    TableDesc by_name;
    ASSERT_TRUE(catalog.get_table_by_name("commerce", "orders", &by_name).ok());
    EXPECT_EQ(by_name.table_id, new_table.table_id);
    EXPECT_EQ(by_name.shard_count, 8);

    ASSERT_TRUE(catalog.get_table_by_id(old_table.table_id, &by_id).ok());
    EXPECT_EQ(by_id.table_id, old_table.table_id);
    EXPECT_EQ(by_id.state, TableState::DELETED);
}
// 关于table删除后的持久化
TEST_F(CatalogStoreTest, RecoverDeletedTableKeepsIdLookupAndClearsNameIndex) {
    auto dir = make_sub_dir("recover_deleted");
    {
        CatalogPersistEngine engine(dir.string());
        ASSERT_TRUE(engine.init().ok());
        CatalogStore catalog(&engine);
        ASSERT_TRUE(catalog.init().ok());

        ASSERT_TRUE(
            catalog.create_db(CreateDBParam{"commerce", "z1"}, nullptr)
                .ok());
        TableDesc old_table;
        ASSERT_TRUE(catalog
                        .create_table(CreateTableParam{"commerce", "orders",
                                                           4, 3, "pool-a"},
                                      &old_table)
                        .ok());
        ASSERT_TRUE(
            catalog.update_table_state(old_table.table_id, TableState::DELETED)
                .ok());
    }

    {
        CatalogPersistEngine engine(dir.string());
        ASSERT_TRUE(engine.init().ok());
        CatalogStore catalog(&engine);
        ASSERT_TRUE(catalog.init().ok());

        TableDesc by_name;

        Status status =
            catalog.get_table_by_name("commerce", "orders", &by_name).code();
        ASSERT_EQ(status.code(), StatusCode::TABLE_NOT_FOUND);
        TableDesc old_by_id;
        status = catalog.get_table_by_id(0, &old_by_id);
        EXPECT_EQ(status.code(), StatusCode::OK);
        EXPECT_EQ(old_by_id.state, TableState::DELETED);

        std::vector<TableDesc> tables;
        ASSERT_TRUE(catalog.list_tables("commerce", &tables).ok());
        ASSERT_TRUE(tables.empty());
    }
}

// 关于同名字table删除又创建后的持久化是否正常
TEST_F(CatalogStoreTest, RecoverSkipsDeletedNameIndexAndKeepsIdLookup) {
    auto dir = make_sub_dir("recover_deleted_current_name");
    {
        CatalogPersistEngine engine(dir.string());
        ASSERT_TRUE(engine.init().ok());
        CatalogStore catalog(&engine);
        ASSERT_TRUE(catalog.init().ok());

        ASSERT_TRUE(
            catalog.create_db(CreateDBParam{"commerce", "z1"}, nullptr)
                .ok());
        TableDesc old_table;
        ASSERT_TRUE(catalog
                        .create_table(CreateTableParam{"commerce", "orders",
                                                           4, 3, "pool-a"},
                                      &old_table)
                        .ok());
        ASSERT_TRUE(
            catalog.update_table_state(old_table.table_id, TableState::DELETED)
                .ok());
        TableDesc new_table;
        ASSERT_TRUE(catalog
                        .create_table(CreateTableParam{"commerce", "orders",
                                                           8, 2, "pool-b"},
                                      &new_table)
                        .ok());
        ASSERT_EQ(new_table.table_id, 1);
    }

    {
        CatalogPersistEngine engine(dir.string());
        ASSERT_TRUE(engine.init().ok());
        CatalogStore catalog(&engine);
        ASSERT_TRUE(catalog.init().ok());

        TableDesc by_name;
        ASSERT_TRUE(
            catalog.get_table_by_name("commerce", "orders", &by_name).ok());
        EXPECT_EQ(by_name.table_id, 1);
        EXPECT_EQ(by_name.shard_count, 8);

        TableDesc old_by_id;
        ASSERT_TRUE(catalog.get_table_by_id(0, &old_by_id).ok());
        EXPECT_EQ(old_by_id.state, TableState::DELETED);

        std::vector<TableDesc> tables;
        ASSERT_TRUE(catalog.list_tables("commerce", &tables).ok());
        ASSERT_EQ(tables.size(), 1U);
        EXPECT_EQ(tables[0].table_id, 1);
    }
}

// 测试删除仍包含活跃表的 DB 时会被拒绝，并确认 DB 元信息不会被误删。
TEST_F(CatalogStoreTest, DeleteDbRejectsActiveTables) {
    auto dir = make_sub_dir("delete_db_rejects_active");

    CatalogPersistEngine engine(dir.string());
    ASSERT_TRUE(engine.init().ok());
    CatalogStore catalog(&engine);
    ASSERT_TRUE(catalog.init().ok());

    DatabaseDesc db;
    ASSERT_TRUE(
        catalog.create_db(CreateDBParam{"commerce", "z1"}, &db).ok());
    ASSERT_TRUE(catalog
                    .create_table(CreateTableParam{"commerce", "orders", 4,
                                                       3, "pool-a"},
                                  nullptr)
                    .ok());

    Status status = catalog.delete_db(db.db_id);

    EXPECT_EQ(status.code(), StatusCode::INVALID_ARGUMENT);
    DatabaseDesc stored;
    ASSERT_TRUE(catalog.get_db("commerce", &stored).ok());
    EXPECT_EQ(stored.db_id, db.db_id);
}

// 测试 DB 只剩 DELETED 历史表时允许删除，同时保留旧表的 table_id 查询能力。
TEST_F(CatalogStoreTest, DeleteDbAllowsOnlyDeletedTables) {
    auto dir = make_sub_dir("delete_db_deleted_tables");

    CatalogPersistEngine engine(dir.string());
    ASSERT_TRUE(engine.init().ok());
    CatalogStore catalog(&engine);
    ASSERT_TRUE(catalog.init().ok());

    DatabaseDesc db;
    ASSERT_TRUE(
        catalog.create_db(CreateDBParam{"commerce", "z1"}, &db).ok());
    TableDesc table;
    ASSERT_TRUE(catalog
                    .create_table(CreateTableParam{"commerce", "orders", 4,
                                                       3, "pool-a"},
                                  &table)
                    .ok());
    ASSERT_TRUE(
        catalog.update_table_state(table.table_id, TableState::DELETED).ok());

    ASSERT_TRUE(catalog.delete_db(db.db_id).ok());

    DatabaseDesc dropped_db;
    EXPECT_EQ(catalog.get_db("commerce", &dropped_db).code(),
              StatusCode::DB_NOT_FOUND);
    TableDesc deleted_table;
    ASSERT_TRUE(catalog.get_table_by_id(table.table_id, &deleted_table).ok());
    EXPECT_EQ(deleted_table.state, TableState::DELETED);

    DatabaseDesc recreated_db;
    ASSERT_TRUE(
        catalog.create_db(CreateDBParam{"commerce", "z2"}, &recreated_db)
            .ok());
    EXPECT_NE(recreated_db.db_id, db.db_id);
    std::vector<TableDesc> tables;
    ASSERT_TRUE(catalog.list_tables("commerce", &tables).ok());
    EXPECT_TRUE(tables.empty());
}

}  // namespace
}  // namespace adviskv::catalog
