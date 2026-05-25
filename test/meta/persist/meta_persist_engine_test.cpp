#include "meta/persist/meta_persist_engine.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <string>

#include "common/status.h"
#include "meta/catalog/catalog_manager.h"
#include "meta/catalog/meta_types.h"

namespace adviskv::meta {
namespace {

class MetaPersistEngineTest : public ::testing::Test {
   protected:
    void SetUp() override {
        test_dir_ = std::filesystem::temp_directory_path() /
                    ("meta_persist_test_" + std::to_string(::getpid()));
        std::filesystem::create_directories(test_dir_);
    }

    void TearDown() override { std::filesystem::remove_all(test_dir_); }

    std::filesystem::path test_dir_;
};

// 验证空的Meta是正常运行，没有问题的。
TEST_F(MetaPersistEngineTest, SaveAndLoadEmptyMeta) {
    MetaPersistEngine engine(test_dir_.string());
    ASSERT_TRUE(engine.init().ok());

    PersistedMetaRecord record;
    ASSERT_TRUE(engine.save_meta(record).ok());

    PersistedMetaRecord loaded;
    ASSERT_TRUE(engine.load_meta(loaded).ok());

    EXPECT_TRUE(loaded.db_meta_map.empty());
    EXPECT_TRUE(loaded.table_id2table_meta.empty());
    EXPECT_EQ(loaded.next_db_id, 0);
    EXPECT_EQ(loaded.next_table_id, 0);
}

// 验证一下正常情况的meta持久化效果如何
TEST_F(MetaPersistEngineTest, SaveAndLoadWithDBAndTable) {
    MetaPersistEngine engine(test_dir_.string());
    ASSERT_TRUE(engine.init().ok());

    PersistedMetaRecord record;
    DBMeta db1{1, "test_db", "zone-a"};
    DBMeta db2{2, "another_db", "zone-b"};
    record.db_meta_map[1] = db1;
    record.db_meta_map[2] = db2;

    TableMeta t1{10,         4,       3,        1,
                 "test_db",  "users", "pool-a", TableState::NORMAL,
                 "op-users", "",      100,      200};
    TableMeta t2{11,          2,
                 1,           1,
                 "test_db",   "orders",
                 "pool-b",    TableState::FAILED,
                 "op-orders", "placement failed",
                 300,         400};
    record.table_id2table_meta[10] = t1;
    record.table_id2table_meta[11] = t2;
    record.next_db_id = 3;
    record.next_table_id = 12;

    ASSERT_TRUE(engine.save_meta(record).ok());

    PersistedMetaRecord loaded;
    ASSERT_TRUE(engine.load_meta(loaded).ok());

    EXPECT_EQ(loaded.db_meta_map.size(), 2u);
    EXPECT_EQ(loaded.db_meta_map[1].db_id, 1);
    EXPECT_EQ(loaded.db_meta_map[1].db_name, "test_db");
    EXPECT_EQ(loaded.db_meta_map[1].zone, "zone-a");
    EXPECT_EQ(loaded.db_meta_map[2].db_id, 2);
    EXPECT_EQ(loaded.db_meta_map[2].db_name, "another_db");
    EXPECT_EQ(loaded.db_meta_map[2].zone, "zone-b");

    EXPECT_EQ(loaded.table_id2table_meta.size(), 2u);
    EXPECT_EQ(loaded.table_id2table_meta[10].table_id, 10);
    EXPECT_EQ(loaded.table_id2table_meta[10].shard_count, 4);
    EXPECT_EQ(loaded.table_id2table_meta[10].replica_count, 3);
    EXPECT_EQ(loaded.table_id2table_meta[10].db_id, 1);
    EXPECT_EQ(loaded.table_id2table_meta[10].db_name, "test_db");
    EXPECT_EQ(loaded.table_id2table_meta[10].table_name, "users");
    EXPECT_EQ(loaded.table_id2table_meta[10].resource_pool, "pool-a");
    EXPECT_EQ(loaded.table_id2table_meta[10].state, TableState::NORMAL);
    EXPECT_EQ(loaded.table_id2table_meta[10].operation_id, "op-users");
    EXPECT_EQ(loaded.table_id2table_meta[10].create_ts, 100);
    EXPECT_EQ(loaded.table_id2table_meta[10].update_ts, 200);
    EXPECT_EQ(loaded.table_id2table_meta[11].table_id, 11);
    EXPECT_EQ(loaded.table_id2table_meta[11].shard_count, 2);
    EXPECT_EQ(loaded.table_id2table_meta[11].replica_count, 1);
    EXPECT_EQ(loaded.table_id2table_meta[11].db_name, "test_db");
    EXPECT_EQ(loaded.table_id2table_meta[11].table_name, "orders");
    EXPECT_EQ(loaded.table_id2table_meta[11].resource_pool, "pool-b");
    EXPECT_EQ(loaded.table_id2table_meta[11].state, TableState::FAILED);
    EXPECT_EQ(loaded.table_id2table_meta[11].operation_id, "op-orders");
    EXPECT_EQ(loaded.table_id2table_meta[11].last_error_msg,
              "placement failed");
    EXPECT_EQ(loaded.table_id2table_meta[11].create_ts, 300);
    EXPECT_EQ(loaded.table_id2table_meta[11].update_ts, 400);

    EXPECT_EQ(loaded.next_db_id, 3);
    EXPECT_EQ(loaded.next_table_id, 12);
}

// 验证重复保存的时候是留着新覆盖的那个信息
TEST_F(MetaPersistEngineTest, OverwriteSaveAndReload) {
    MetaPersistEngine engine(test_dir_.string());
    ASSERT_TRUE(engine.init().ok());

    {
        PersistedMetaRecord record;
        record.db_meta_map[1] = DBMeta{1, "first_db", "z1"};
        record.next_db_id = 2;
        ASSERT_TRUE(engine.save_meta(record).ok());
    }

    {
        PersistedMetaRecord record;
        record.db_meta_map[10] = DBMeta{10, "second_db", "z2"};
        {
            TableMeta table{};
            table.table_id = 20, table.shard_count = 8, table.replica_count = 2;
            table.db_id = 10, table.db_name = "second_db",
            table.table_name = "new_table", table.resource_pool = "pool-c";
            record.table_id2table_meta[20] = {table};
        }

        record.next_db_id = 11;
        record.next_table_id = 21;
        ASSERT_TRUE(engine.save_meta(record).ok());
    }

    PersistedMetaRecord loaded;
    ASSERT_TRUE(engine.load_meta(loaded).ok());

    EXPECT_EQ(loaded.db_meta_map.size(), 1u);
    EXPECT_EQ(loaded.db_meta_map[10].db_name, "second_db");
    EXPECT_EQ(loaded.table_id2table_meta.size(), 1u);
    EXPECT_EQ(loaded.table_id2table_meta[20].table_name, "new_table");
    EXPECT_EQ(loaded.next_db_id, 11);
    EXPECT_EQ(loaded.next_table_id, 21);
}

// 验证从不存在的目录加载Meta是空的，同时不会报错
TEST_F(MetaPersistEngineTest, LoadFromNonExistentDir) {
    auto non_exist_dir = test_dir_ / "not_exist";
    MetaPersistEngine engine(non_exist_dir.string());
    ASSERT_TRUE(engine.init().ok());

    PersistedMetaRecord loaded;
    ASSERT_TRUE(engine.load_meta(loaded).ok());

    EXPECT_TRUE(loaded.db_meta_map.empty());
    EXPECT_TRUE(loaded.table_id2table_meta.empty());
    EXPECT_EQ(loaded.next_db_id, 0);
    EXPECT_EQ(loaded.next_table_id, 0);
}

}  // namespace
}  // namespace adviskv::meta