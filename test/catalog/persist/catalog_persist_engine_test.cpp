#include "catalog/persist/catalog_persist_engine.h"

#include <filesystem>
#include <string>

#include <gtest/gtest.h>

#include "common/status.h"
#include "catalog/store/catalog_store.h"
#include "catalog/model/model.h"

namespace adviskv::catalog {
namespace {

class CatalogPersistEngineTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = std::filesystem::temp_directory_path() / ("catalog_persist_test_" + std::to_string(::getpid()));
        std::filesystem::create_directories(test_dir_);
    }

    void TearDown() override {
        std::filesystem::remove_all(test_dir_);
    }

    std::filesystem::path test_dir_;
};

// 验证空的Catalog是正常运行，没有问题的。
TEST_F(CatalogPersistEngineTest, SaveAndLoadEmptyCatalog) {
    CatalogPersistEngine engine(test_dir_.string());
    ASSERT_TRUE(engine.init().ok());

    CatalogPersistedRecord record;
    ASSERT_TRUE(engine.save_catalog(record).ok());

    CatalogPersistedRecord loaded;
    ASSERT_TRUE(engine.load_catalog(loaded).ok());

    EXPECT_TRUE(loaded.db_desc_map.empty());
    EXPECT_TRUE(loaded.table_id2table_desc.empty());
    EXPECT_EQ(loaded.next_db_id, 0);
    EXPECT_EQ(loaded.next_table_id, 0);
}

// 验证一下正常情况的catalog持久化效果如何
TEST_F(CatalogPersistEngineTest, SaveAndLoadWithDBAndTable) {
    CatalogPersistEngine engine(test_dir_.string());
    ASSERT_TRUE(engine.init().ok());

    CatalogPersistedRecord record;
    DatabaseDesc db1{1, "test_db", "zone-a"};
    DatabaseDesc db2{2, "another_db", "zone-b"};
    record.db_desc_map[1] = db1;
    record.db_desc_map[2] = db2;

    TableDesc t1{10, 4, 3, 1, "test_db", "users", "pool-a", TableState::NORMAL, "op-users", "", 100, 200};
    TableDesc t2{11,  2,  1, 1, "test_db", "orders", "pool-b", TableState::FAILED, "op-orders", "placement failed",
                 300, 400};
    t2.engine_type = EngineType::MAP;
    record.table_id2table_desc[10] = t1;
    record.table_id2table_desc[11] = t2;
    record.next_db_id = 3;
    record.next_table_id = 12;

    ASSERT_TRUE(engine.save_catalog(record).ok());

    CatalogPersistedRecord loaded;
    ASSERT_TRUE(engine.load_catalog(loaded).ok());

    EXPECT_EQ(loaded.db_desc_map.size(), 2u);
    EXPECT_EQ(loaded.db_desc_map[1].db_id, 1);
    EXPECT_EQ(loaded.db_desc_map[1].db_name, "test_db");
    EXPECT_EQ(loaded.db_desc_map[1].zone, "zone-a");
    EXPECT_EQ(loaded.db_desc_map[2].db_id, 2);
    EXPECT_EQ(loaded.db_desc_map[2].db_name, "another_db");
    EXPECT_EQ(loaded.db_desc_map[2].zone, "zone-b");

    EXPECT_EQ(loaded.table_id2table_desc.size(), 2u);
    EXPECT_EQ(loaded.table_id2table_desc[10].table_id, 10);
    EXPECT_EQ(loaded.table_id2table_desc[10].shard_count, 4);
    EXPECT_EQ(loaded.table_id2table_desc[10].replica_count, 3);
    EXPECT_EQ(loaded.table_id2table_desc[10].db_id, 1);
    EXPECT_EQ(loaded.table_id2table_desc[10].db_name, "test_db");
    EXPECT_EQ(loaded.table_id2table_desc[10].table_name, "users");
    EXPECT_EQ(loaded.table_id2table_desc[10].resource_pool, "pool-a");
    EXPECT_EQ(loaded.table_id2table_desc[10].engine_type, EngineType::MAP);
    EXPECT_EQ(loaded.table_id2table_desc[10].state, TableState::NORMAL);
    EXPECT_EQ(loaded.table_id2table_desc[10].operation_id, "op-users");
    EXPECT_EQ(loaded.table_id2table_desc[10].create_ts, 100);
    EXPECT_EQ(loaded.table_id2table_desc[10].update_ts, 200);
    EXPECT_EQ(loaded.table_id2table_desc[11].table_id, 11);
    EXPECT_EQ(loaded.table_id2table_desc[11].shard_count, 2);
    EXPECT_EQ(loaded.table_id2table_desc[11].replica_count, 1);
    EXPECT_EQ(loaded.table_id2table_desc[11].db_name, "test_db");
    EXPECT_EQ(loaded.table_id2table_desc[11].table_name, "orders");
    EXPECT_EQ(loaded.table_id2table_desc[11].resource_pool, "pool-b");
    EXPECT_EQ(loaded.table_id2table_desc[11].engine_type, EngineType::MAP);
    EXPECT_EQ(loaded.table_id2table_desc[11].state, TableState::FAILED);
    EXPECT_EQ(loaded.table_id2table_desc[11].operation_id, "op-orders");
    EXPECT_EQ(loaded.table_id2table_desc[11].last_error_msg, "placement failed");
    EXPECT_EQ(loaded.table_id2table_desc[11].create_ts, 300);
    EXPECT_EQ(loaded.table_id2table_desc[11].update_ts, 400);

    EXPECT_EQ(loaded.next_db_id, 3);
    EXPECT_EQ(loaded.next_table_id, 12);
}

// 验证重复保存的时候是留着新覆盖的那个信息
TEST_F(CatalogPersistEngineTest, OverwriteSaveAndReload) {
    CatalogPersistEngine engine(test_dir_.string());
    ASSERT_TRUE(engine.init().ok());

    {
        CatalogPersistedRecord record;
        record.db_desc_map[1] = DatabaseDesc{1, "first_db", "z1"};
        record.next_db_id = 2;
        ASSERT_TRUE(engine.save_catalog(record).ok());
    }

    {
        CatalogPersistedRecord record;
        record.db_desc_map[10] = DatabaseDesc{10, "second_db", "z2"};
        {
            TableDesc table{};
            table.table_id = 20, table.shard_count = 8, table.replica_count = 2;
            table.db_id = 10, table.db_name = "second_db", table.table_name = "new_table",
            table.resource_pool = "pool-c";
            record.table_id2table_desc[20] = {table};
        }

        record.next_db_id = 11;
        record.next_table_id = 21;
        ASSERT_TRUE(engine.save_catalog(record).ok());
    }

    CatalogPersistedRecord loaded;
    ASSERT_TRUE(engine.load_catalog(loaded).ok());

    EXPECT_EQ(loaded.db_desc_map.size(), 1u);
    EXPECT_EQ(loaded.db_desc_map[10].db_name, "second_db");
    EXPECT_EQ(loaded.table_id2table_desc.size(), 1u);
    EXPECT_EQ(loaded.table_id2table_desc[20].table_name, "new_table");
    EXPECT_EQ(loaded.next_db_id, 11);
    EXPECT_EQ(loaded.next_table_id, 21);
}

// 验证从不存在的目录加载Catalog是空的，同时不会报错
TEST_F(CatalogPersistEngineTest, LoadFromNonExistentDir) {
    auto non_exist_dir = test_dir_ / "not_exist";
    CatalogPersistEngine engine(non_exist_dir.string());
    ASSERT_TRUE(engine.init().ok());

    CatalogPersistedRecord loaded;
    ASSERT_TRUE(engine.load_catalog(loaded).ok());

    EXPECT_TRUE(loaded.db_desc_map.empty());
    EXPECT_TRUE(loaded.table_id2table_desc.empty());
    EXPECT_EQ(loaded.next_db_id, 0);
    EXPECT_EQ(loaded.next_table_id, 0);
}

}  // namespace
}  // namespace adviskv::catalog
