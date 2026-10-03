#include "meta/service/ddl_service.h"

#include <gtest/gtest.h>

#include "common/status.h"
#include "meta/store/meta_store.h"
#include "meta/store/meta_store_txn.h"
#include "test_env.h"

namespace adviskv::meta {

TEST(DdlServiceTest, CreateDbAndTablePersistsAndIsIdempotent) {
    const auto dir = test::make_unique_test_dir("ddl_create", 1);
    MetaStore store{PersistentMetaStoreParam{dir.string()}};
    ASSERT_TRUE(store.init().ok());
    DdlService ddl(&store);

    Database db;
    ASSERT_TRUE(ddl.create_db(CreateDBParam{"commerce", "zone-a"}, &db).ok());
    EXPECT_GE(db.db_id, 0);

    Table table;
    ASSERT_TRUE(ddl.create_table(CreateTableParam{"commerce", "orders", 4, 3, "default"}, &table).ok());
    EXPECT_EQ(table.state.phase, TablePhase::CREATING);
    EXPECT_EQ(table.shard_count(), 4);
    EXPECT_EQ(table.spec.target_replica_count, 3);

    Table again;
    ASSERT_TRUE(ddl.create_table(CreateTableParam{"commerce", "orders", 4, 3, "default"}, &again).ok());
    EXPECT_EQ(again.table_id, table.table_id);

    Table conflict;
    EXPECT_EQ(ddl.create_table(CreateTableParam{"commerce", "orders", 2, 3, "default"}, &conflict).code(),
              StatusCode::ALREADY_EXIST);

    MetaStore reopened{PersistentMetaStoreParam{dir.string()}};
    ASSERT_TRUE(reopened.init().ok());
    DdlService ddl2(&reopened);
    Table loaded;
    ASSERT_TRUE(ddl2.get_table(GetTableParam{"commerce", "orders"}, &loaded).ok());
    EXPECT_EQ(loaded.table_id, table.table_id);
    EXPECT_EQ(loaded.shard_count(), 4);
}

TEST(DdlServiceTest, DropTableKeepsNameUntilDeletedAndAllowsCreatingDrop) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    DdlService ddl(&store);

    ASSERT_TRUE(ddl.create_db(CreateDBParam{"commerce", "zone-a"}, nullptr).ok());
    Table table;
    ASSERT_TRUE(ddl.create_table(CreateTableParam{"commerce", "orders", 2, 1, "default"}, &table).ok());
    EXPECT_EQ(table.state.phase, TablePhase::CREATING);

    Table dropped;
    ASSERT_TRUE(ddl.drop_table(DropTableByNameParam{"commerce", "orders"}, &dropped).ok());
    EXPECT_EQ(dropped.state.phase, TablePhase::DELETING);
    EXPECT_EQ(dropped.table_id, table.table_id);

    Table retry;
    ASSERT_TRUE(ddl.drop_table(DropTableByNameParam{"commerce", "orders"}, &retry).ok());
    EXPECT_EQ(retry.table_id, table.table_id);
    EXPECT_EQ(retry.state.phase, TablePhase::DELETING);

    Table conflict;
    EXPECT_EQ(ddl.create_table(CreateTableParam{"commerce", "orders", 2, 1, "default"}, &conflict).code(),
              StatusCode::ALREADY_EXIST);

    Table visible;
    ASSERT_TRUE(ddl.get_table(GetTableParam{"commerce", "orders"}, &visible).ok());
    EXPECT_EQ(visible.table_id, table.table_id);
    EXPECT_EQ(visible.state.phase, TablePhase::DELETING);
}

TEST(DdlServiceTest, DeletedTableRemainsQueryableByIdAfterDatabaseDrop) {
    const auto dir = test::make_unique_test_dir("ddl_drop_db", 1);
    MetaStore store{PersistentMetaStoreParam{dir.string()}};
    ASSERT_TRUE(store.init().ok());
    DdlService ddl(&store);

    ASSERT_TRUE(ddl.create_db(CreateDBParam{"commerce", "zone-a"}, nullptr).ok());
    Table table;
    ASSERT_TRUE(ddl.create_table(CreateTableParam{"commerce", "orders", 1, 0, "default"}, &table).ok());
    table.state.phase = TablePhase::DELETED;
    ASSERT_TRUE(store.write_with([&](MetaStoreTxn& txn) { return txn.put_table(table); }).ok());

    Table deleted;
    ASSERT_TRUE(ddl.get_table(GetTableParam{"", "", true, table.table_id}, &deleted).ok());
    EXPECT_EQ(deleted.state.phase, TablePhase::DELETED);
    EXPECT_EQ(ddl.get_table(GetTableParam{"commerce", "orders"}, &deleted).code(), StatusCode::TABLE_NOT_FOUND);
    ASSERT_TRUE(ddl.drop_db(DropDBParam{"commerce"}, nullptr).ok());

    MetaStore reopened{PersistentMetaStoreParam{dir.string()}};
    ASSERT_TRUE(reopened.init().ok());
    DdlService reopened_ddl(&reopened);
    ASSERT_TRUE(reopened_ddl.get_table(GetTableParam{"", "", true, table.table_id}, &deleted).ok());
    EXPECT_EQ(deleted.state.phase, TablePhase::DELETED);
}

TEST(DdlServiceTest, NameBasedTableOperationsReportMissingDatabase) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    DdlService ddl(&store);

    Table table;
    EXPECT_EQ(ddl.get_table(GetTableParam{"missing", "orders"}, &table).code(), StatusCode::DB_NOT_FOUND);
    EXPECT_EQ(ddl.drop_table(DropTableByNameParam{"missing", "orders"}, &table).code(), StatusCode::DB_NOT_FOUND);
    EXPECT_EQ(ddl.alter_table_replica_count(AlterTableReplicaCountParam{"missing", "orders", 1}, &table).code(),
              StatusCode::DB_NOT_FOUND);
}

}  // namespace adviskv::meta
