#include "meta/catalog/catalog_manager.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <string>

#include "common/status.h"
#include "meta/persist/i_meta_persist_engine.h"
#include "meta/persist/meta_persist_engine.h"

namespace adviskv::meta {
namespace {

class FaultyMetaPersistEngine : public IMetaPersistEngine {
   public:
    Status init() override { return Status::OK(); }
    Status close() override { return Status::OK(); }
    Status save_meta(const PersistedMetaRecord&) override {
        return Status::ERROR("inject save_meta failure");
    }
    Status load_meta(PersistedMetaRecord& record) override {
        record = {};
        return Status::OK();
    }
};

class CatalogManagerTest : public ::testing::Test {
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
TEST_F(CatalogManagerTest, CreateDbAndTableBasic) {
    auto dir = make_sub_dir("basic_create");

    MetaPersistEngine engine(dir.string());
    ASSERT_TRUE(engine.init().ok());
    CatalogManager catalog(&engine);
    ASSERT_TRUE(catalog.init().ok());

    DBMeta db_meta;
    ASSERT_TRUE(
        catalog.create_db({.db_name = "test_db", .zone = "zone-1"}, &db_meta)
            .ok());
    EXPECT_EQ(db_meta.db_id, 0);
    EXPECT_EQ(db_meta.db_name, "test_db");
    EXPECT_EQ(db_meta.zone, "zone-1");

    DBMeta db_meta2;
    ASSERT_TRUE(
        catalog.create_db({.db_name = "test_db2", .zone = "zone-2"}, &db_meta2)
            .ok());
    EXPECT_EQ(db_meta2.db_id, 1);
    EXPECT_EQ(db_meta2.db_name, "test_db2");

    TableMeta table_meta;
    ASSERT_TRUE(catalog
                    .create_table({.db_name = "test_db",
                                   .table_name = "users",
                                   .shard_count = 4,
                                   .replica_count = 3,
                                   .resource_pool = "pool-a"},
                                  &table_meta)
                    .ok());
    EXPECT_EQ(table_meta.table_id, 0);
    EXPECT_EQ(table_meta.db_name, "test_db");
    EXPECT_EQ(table_meta.table_name, "users");
    EXPECT_EQ(table_meta.shard_count, 4);
    EXPECT_EQ(table_meta.replica_count, 3);
    EXPECT_EQ(table_meta.db_id, db_meta.db_id);
    EXPECT_EQ(table_meta.resource_pool, "pool-a");

    TableMeta table_meta2;
    ASSERT_TRUE(catalog
                    .create_table({.db_name = "test_db",
                                   .table_name = "orders",
                                   .shard_count = 8,
                                   .replica_count = 2,
                                   .resource_pool = "pool-b"},
                                  &table_meta2)
                    .ok());
    EXPECT_EQ(table_meta2.table_id, 1);
    EXPECT_EQ(table_meta2.table_name, "orders");
    EXPECT_EQ(table_meta2.shard_count, 8);
    EXPECT_EQ(table_meta2.replica_count, 2);

    DBMeta got_db;
    ASSERT_TRUE(catalog.get_db("test_db", &got_db).ok());
    EXPECT_EQ(got_db.db_id, db_meta.db_id);
    EXPECT_EQ(got_db.db_name, "test_db");
    EXPECT_EQ(got_db.zone, "zone-1");

    TableMeta got_table;
    ASSERT_TRUE(catalog.get_table_by_name("test_db", "users", &got_table).ok());
    EXPECT_EQ(got_table.table_id, 0);
    EXPECT_EQ(got_table.table_name, "users");
    EXPECT_EQ(got_table.shard_count, 4);
    EXPECT_EQ(got_table.replica_count, 3);
    EXPECT_EQ(got_table.resource_pool, "pool-a");
    EXPECT_EQ(got_table.db_name, "test_db");
    EXPECT_EQ(got_table.db_id, db_meta.db_id);

    std::vector<TableMeta> tables;
    ASSERT_TRUE(catalog.list_tables("test_db", &tables).ok());
    EXPECT_EQ(tables.size(), 2u);

    // 这次再插入一次db和table，期待的结果是错误码是已经存在了。
}

// 测试一下当catalog_manager这边创建db失败了（因为persist）之后，
// 查询db或者table是否还会存在
TEST_F(CatalogManagerTest, CreateDbFailsDueToPersist_DbShouldNotExist) {
    FaultyMetaPersistEngine faulty;

    CatalogManager catalog(&faulty);
    ASSERT_TRUE(catalog.init().ok());

    DBMeta db_meta;
    Status status =
        catalog.create_db({.db_name = "fail_db", .zone = "z1"}, &db_meta);
    EXPECT_TRUE(status.fail());

    DBMeta check_meta;
    EXPECT_EQ(catalog.get_db("fail_db", &check_meta).code(),
              StatusCode::DB_NOT_FOUND);
}

// 测试一下当catalog_manager这边创建table失败了（因为persist）之后，
// 查询table是否还会存在
TEST_F(CatalogManagerTest, CreateTableFailsDueToPersist_TableShouldNotExist) {
    auto dir = make_sub_dir("persist_fail_table");

    MetaPersistEngine engine(dir.string());
    ASSERT_TRUE(engine.init().ok());
    CatalogManager catalog(&engine);
    ASSERT_TRUE(catalog.init().ok());

    DBMeta db_meta;
    ASSERT_TRUE(
        catalog.create_db({.db_name = "mydb", .zone = "z1"}, &db_meta).ok());

    TableMeta table_meta;
    ASSERT_TRUE(catalog
                    .create_table({.db_name = "mydb",
                                   .table_name = "existing_table",
                                   .shard_count = 4,
                                   .replica_count = 3,
                                   .resource_pool = "pool-a"},
                                  &table_meta)
                    .ok());

    FaultyMetaPersistEngine faulty;
    CatalogManager faulty_catalog(&faulty);
    ASSERT_TRUE(faulty_catalog.init().ok());

    TableMeta fail_table_meta;
    Status status = faulty_catalog.create_table({.db_name = "mydb",
                                                 .table_name = "fail_table",
                                                 .shard_count = 2,
                                                 .replica_count = 1,
                                                 .resource_pool = "pool-b"},
                                                &fail_table_meta);
    EXPECT_TRUE(status.fail());

    TableMeta check_meta;
    EXPECT_EQ(
        faulty_catalog.get_table_by_name("mydb", "fail_table", &check_meta)
            .code(),
        StatusCode::DB_NOT_FOUND);
}

// 测试一下catalog的list_tables，lookup_db_by_name，lookup_table_by_name是否有问题
TEST_F(CatalogManagerTest, ListTablesAndGetQueries) {
    auto dir = make_sub_dir("query_test");

    MetaPersistEngine engine(dir.string());
    ASSERT_TRUE(engine.init().ok());
    CatalogManager catalog(&engine);
    ASSERT_TRUE(catalog.init().ok());

    ASSERT_TRUE(
        catalog.create_db({.db_name = "db1", .zone = "z1"}, nullptr).ok());
    ASSERT_TRUE(
        catalog.create_db({.db_name = "db2", .zone = "z2"}, nullptr).ok());

    ASSERT_TRUE(catalog
                    .create_table({.db_name = "db1",
                                   .table_name = "table1",
                                   .shard_count = 4,
                                   .replica_count = 3,
                                   .resource_pool = "pool-a"},
                                  nullptr)
                    .ok());
    ASSERT_TRUE(catalog
                    .create_table({.db_name = "db1",
                                   .table_name = "table2",
                                   .shard_count = 2,
                                   .replica_count = 1,
                                   .resource_pool = "pool-b"},
                                  nullptr)
                    .ok());
    ASSERT_TRUE(catalog
                    .create_table({.db_name = "db2",
                                   .table_name = "table3",
                                   .shard_count = 8,
                                   .replica_count = 2,
                                   .resource_pool = "pool-c"},
                                  nullptr)
                    .ok());

    DBMeta got_db1;
    ASSERT_TRUE(catalog.get_db("db1", &got_db1).ok());
    EXPECT_EQ(got_db1.db_name, "db1");
    EXPECT_EQ(got_db1.zone, "z1");

    DBMeta got_db2;
    ASSERT_TRUE(catalog.get_db("db2", &got_db2).ok());
    EXPECT_EQ(got_db2.db_name, "db2");
    EXPECT_EQ(got_db2.zone, "z2");

    TableMeta got_t1;
    ASSERT_TRUE(catalog.get_table_by_name("db1", "table1", &got_t1).ok());
    EXPECT_EQ(got_t1.table_name, "table1");
    EXPECT_EQ(got_t1.shard_count, 4);
    EXPECT_EQ(got_t1.db_name, "db1");

    TableMeta got_t3;
    ASSERT_TRUE(catalog.get_table_by_name("db2", "table3", &got_t3).ok());
    EXPECT_EQ(got_t3.table_name, "table3");
    EXPECT_EQ(got_t3.shard_count, 8);

    std::vector<TableMeta> tables_in_db1;
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

    std::vector<TableMeta> tables_in_db2;
    ASSERT_TRUE(catalog.list_tables("db2", &tables_in_db2).ok());
    EXPECT_EQ(tables_in_db2.size(), 1u);
    EXPECT_EQ(tables_in_db2[0].table_name, "table3");

    DBMeta db3;
    ASSERT_TRUE(
        catalog.create_db({.db_name = "empty_db", .zone = "z3"}, &db3).ok());
    std::vector<TableMeta> empty_tables;
    ASSERT_TRUE(catalog.list_tables("empty_db", &empty_tables).ok());
    EXPECT_TRUE(empty_tables.empty());
}

// 测试catalog_manager持久化之后，再次恢复的数据是否一致
TEST_F(CatalogManagerTest, PersistAndRecoverDataConsistency) {
    auto dir = make_sub_dir("recover_test");

    {
        MetaPersistEngine engine(dir.string());
        ASSERT_TRUE(engine.init().ok());
        CatalogManager catalog(&engine);
        ASSERT_TRUE(catalog.init().ok());

        ASSERT_TRUE(
            catalog
                .create_db({.db_name = "db_alpha", .zone = "zone-a"}, nullptr)
                .ok());
        ASSERT_TRUE(
            catalog.create_db({.db_name = "db_beta", .zone = "zone-b"}, nullptr)
                .ok());

        ASSERT_TRUE(catalog
                        .create_table({.db_name = "db_alpha",
                                       .table_name = "users",
                                       .shard_count = 4,
                                       .replica_count = 3,
                                       .resource_pool = "pool-a"},
                                      nullptr)
                        .ok());
        ASSERT_TRUE(catalog
                        .create_table({.db_name = "db_alpha",
                                       .table_name = "orders",
                                       .shard_count = 8,
                                       .replica_count = 2,
                                       .resource_pool = "pool-b"},
                                      nullptr)
                        .ok());
        ASSERT_TRUE(catalog
                        .create_table({.db_name = "db_beta",
                                       .table_name = "products",
                                       .shard_count = 2,
                                       .replica_count = 1,
                                       .resource_pool = "pool-c"},
                                      nullptr)
                        .ok());
    }

    {
        MetaPersistEngine engine(dir.string());
        ASSERT_TRUE(engine.init().ok());
        CatalogManager catalog(&engine);
        ASSERT_TRUE(catalog.init().ok());

        DBMeta got_db1, got_db2;
        ASSERT_TRUE(catalog.get_db("db_alpha", &got_db1).ok());
        EXPECT_EQ(got_db1.db_name, "db_alpha");
        EXPECT_EQ(got_db1.zone, "zone-a");

        ASSERT_TRUE(catalog.get_db("db_beta", &got_db2).ok());
        EXPECT_EQ(got_db2.db_name, "db_beta");
        EXPECT_EQ(got_db2.zone, "zone-b");

        std::vector<TableMeta> alpha_tables;
        ASSERT_TRUE(catalog.list_tables("db_alpha", &alpha_tables).ok());
        EXPECT_EQ(alpha_tables.size(), 2u);

        TableMeta got_users;
        ASSERT_TRUE(
            catalog.get_table_by_name("db_alpha", "users", &got_users).ok());
        EXPECT_EQ(got_users.table_name, "users");
        EXPECT_EQ(got_users.shard_count, 4);
        EXPECT_EQ(got_users.replica_count, 3);
        EXPECT_EQ(got_users.resource_pool, "pool-a");
        EXPECT_EQ(got_users.db_name, "db_alpha");

        TableMeta got_orders;
        ASSERT_TRUE(
            catalog.get_table_by_name("db_alpha", "orders", &got_orders).ok());
        EXPECT_EQ(got_orders.table_name, "orders");
        EXPECT_EQ(got_orders.shard_count, 8);
        EXPECT_EQ(got_orders.replica_count, 2);
        EXPECT_EQ(got_orders.resource_pool, "pool-b");

        std::vector<TableMeta> beta_tables;
        ASSERT_TRUE(catalog.list_tables("db_beta", &beta_tables).ok());
        EXPECT_EQ(beta_tables.size(), 1u);

        TableMeta got_products;
        ASSERT_TRUE(
            catalog.get_table_by_name("db_beta", "products", &got_products)
                .ok());
        EXPECT_EQ(got_products.table_name, "products");
        EXPECT_EQ(got_products.shard_count, 2);
        EXPECT_EQ(got_products.replica_count, 1);
        EXPECT_EQ(got_products.resource_pool, "pool-c");

        DBMeta new_db;
        ASSERT_TRUE(
            catalog
                .create_db({.db_name = "db_gamma", .zone = "zone-c"}, &new_db)
                .ok());
        EXPECT_EQ(new_db.db_id, 2);

        TableMeta new_table;
        ASSERT_TRUE(catalog
                        .create_table({.db_name = "db_gamma",
                                       .table_name = "new_table",
                                       .shard_count = 1,
                                       .replica_count = 1,
                                       .resource_pool = "pool-d"},
                                      &new_table)
                        .ok());
        EXPECT_EQ(new_table.table_id, 3);
    }
}

}  // namespace
}  // namespace adviskv::meta
