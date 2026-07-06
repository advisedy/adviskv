#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

#include "sdm/store/memory_metastore.h"

namespace adviskv::sdm {
namespace {

Table make_table(TableID table_id, const std::string& table_name,
                 TablePhase phase = TablePhase::CREATING) {
    TableState state{};
    state.desired = TableDesired::PRESENT;
    state.phase = phase;
    state.update_ts = 100 + table_id;
    return Table{table_id,
                 TableSpec{table_name, 11, "commerce", 2, 2, "pool-a",
                           "create-table-" + std::to_string(table_id)},
                 state};
}

Replica make_replica(const ReplicaID& replica_id, const NodeID& node_id,
                     ReplicaRole role = ReplicaRole::FOLLOWER) {
    ReplicaState state{};
    state.desired = ReplicaDesired::PRESENT;
    state.phase = ReplicaPhase::READY;
    state.observed_raft_role = role;
    state.observed_endpoint = Endpoint{"127.0.0.1", 18080};
    state.term = 7;
    return Replica{replica_id,
                   ReplicaSpec{"dc-a", node_id, EngineType::MAP},
                   state};
}

std::vector<TableID> sorted_table_ids(const std::vector<TablePtr>& tables) {
    std::vector<TableID> ids;
    ids.reserve(tables.size());
    for (const auto& table : tables) {
        ids.push_back(table->table_id);
    }
    std::sort(ids.begin(), ids.end());
    return ids;
}

std::vector<ReplicaID> sorted_replica_ids(
    const std::vector<ReplicaPtr>& replicas) {
    std::vector<ReplicaID> ids;
    ids.reserve(replicas.size());
    for (const auto& replica : replicas) {
        ids.push_back(replica->replica_id);
    }
    std::sort(ids.begin(), ids.end(),
              [](const ReplicaID& lhs, const ReplicaID& rhs) {
                  if (lhs.table_id != rhs.table_id) {
                      return lhs.table_id < rhs.table_id;
                  }
                  if (lhs.shard_index != rhs.shard_index) {
                      return lhs.shard_index < rhs.shard_index;
                  }
                  return lhs.replica_seq < rhs.replica_seq;
              });
    return ids;
}

std::vector<std::string> sorted_pool_names(
    const std::vector<ResourcePoolPtr>& pools) {
    std::vector<std::string> names;
    names.reserve(pools.size());
    for (const auto& pool : pools) {
        names.push_back(pool->name);
    }
    std::sort(names.begin(), names.end());
    return names;
}

// 验证Table这几个基础接口能不能正常新增、覆盖、查询、列表和删除。
TEST(MemoryMetaStoreTest, TableUpsertGetListAndDeleteWork) {
    MemoryMetaStore store;
    Table table_a = make_table(1001, "orders");
    Table table_b = make_table(1002, "payments");

    ASSERT_TRUE(store.upsert_table(table_a).ok());
    ASSERT_TRUE(store.upsert_table(table_b).ok());

    TablePtr out;
    ASSERT_TRUE(store.get_table(table_a.table_id, out).ok());
    ASSERT_TRUE(out != nullptr);
    EXPECT_EQ(out->spec.table_name, "orders");
    EXPECT_EQ(out->state.phase, TablePhase::CREATING);

    Table updated =
        make_table(table_a.table_id, "orders_v2", TablePhase::READY);
    ASSERT_TRUE(store.upsert_table(updated).ok());
    ASSERT_TRUE(store.get_table(table_a.table_id, out).ok());
    ASSERT_TRUE(out != nullptr);
    EXPECT_EQ(out->spec.table_name, "orders_v2");
    EXPECT_EQ(out->state.phase, TablePhase::READY);

    std::vector<TablePtr> tables;
    ASSERT_TRUE(store.list_tables(tables).ok());
    EXPECT_EQ(sorted_table_ids(tables),
              std::vector<TableID>({table_a.table_id, table_b.table_id}));

    ASSERT_TRUE(store.delete_table(table_a.table_id).ok());
    ASSERT_TRUE(store.get_table(table_a.table_id, out).ok());
    EXPECT_EQ(out, nullptr);

    ASSERT_TRUE(store.list_tables(tables).ok());
    EXPECT_EQ(sorted_table_ids(tables),
              std::vector<TableID>({table_b.table_id}));
}

// 验证Replica单个写入、批量写入、覆盖、查询、列表和删除这些基本路径。
TEST(MemoryMetaStoreTest, ReplicaUpsertBatchGetListAndDeleteWork) {
    MemoryMetaStore store;
    Replica replica_a = make_replica(ReplicaID{1001, 0, 0}, "node-a");
    Replica replica_b = make_replica(ReplicaID{1001, 0, 1}, "node-b");
    Replica replica_c = make_replica(ReplicaID{1001, 1, 0}, "node-c");

    ASSERT_TRUE(store.upsert_replica(replica_a).ok());
    ASSERT_TRUE(store.upsert_replicas({replica_b, replica_c}).ok());

    ReplicaPtr out;
    ASSERT_TRUE(store.get_replica(replica_a.replica_id, out).ok());
    ASSERT_TRUE(out != nullptr);
    EXPECT_EQ(out->spec.assign_node_id, "node-a");
    EXPECT_EQ(out->state.phase, ReplicaPhase::READY);

    Replica updated =
        make_replica(replica_a.replica_id, "node-d", ReplicaRole::LEADER);
    ASSERT_TRUE(store.upsert_replica(updated).ok());
    ASSERT_TRUE(store.get_replica(replica_a.replica_id, out).ok());
    ASSERT_TRUE(out != nullptr);
    EXPECT_EQ(out->spec.assign_node_id, "node-d");
    EXPECT_EQ(out->state.observed_raft_role, ReplicaRole::LEADER);

    std::vector<ReplicaPtr> replicas;
    ASSERT_TRUE(store.list_replicas(replicas).ok());
    EXPECT_EQ(
        sorted_replica_ids(replicas),
        std::vector<ReplicaID>({replica_a.replica_id, replica_b.replica_id,
                                replica_c.replica_id}));

    ASSERT_TRUE(store.delete_replica(replica_b.replica_id).ok());
    ASSERT_TRUE(store.get_replica(replica_b.replica_id, out).ok());
    EXPECT_EQ(out, nullptr);

    ASSERT_TRUE(store.list_replicas(replicas).ok());
    EXPECT_EQ(
        sorted_replica_ids(replicas),
        std::vector<ReplicaID>({replica_a.replica_id, replica_c.replica_id}));
}

// 验证ResourcePool按名字维护，写入、查询、列表、删除这几个接口都能跑通。
TEST(MemoryMetaStoreTest, ResourcePoolUpsertGetListAndDeleteWork) {
    MemoryMetaStore store;

    ASSERT_TRUE(
        store.upsert_resource_pool(ResourcePool{"pool-a"}).ok());
    ASSERT_TRUE(
        store.upsert_resource_pool(ResourcePool{"pool-b"}).ok());

    ResourcePoolPtr out;
    ASSERT_TRUE(store.get_resource_pool("pool-a", out).ok());
    ASSERT_TRUE(out != nullptr);
    EXPECT_EQ(out->name, "pool-a");

    ASSERT_TRUE(
        store.upsert_resource_pool(ResourcePool{"pool-a-renamed"})
            .ok());
    ASSERT_TRUE(store.get_resource_pool("pool-a-renamed", out).ok());
    ASSERT_TRUE(out != nullptr);
    EXPECT_EQ(out->name, "pool-a-renamed");

    std::vector<ResourcePoolPtr> pools;
    ASSERT_TRUE(store.list_resource_pools(pools).ok());
    EXPECT_EQ(sorted_pool_names(pools),
              std::vector<std::string>({"pool-a", "pool-a-renamed", "pool-b"}));

    ASSERT_TRUE(store.delete_resource_pool("pool-b").ok());
    ASSERT_TRUE(store.get_resource_pool("pool-b", out).ok());
    EXPECT_EQ(out, nullptr);
}

// 验证clone_memory_snapshot真的是内存快照，后面原store再改也不应该污染它。
TEST(MemoryMetaStoreTest, CloneMemorySnapshotCopiesCurrentDataIndependently) {
    MemoryMetaStore store;
    TableID table_id = 1001;
    NodeID node_id = "node-a";
    ReplicaID replica_id{1001, 0, 0};

    ASSERT_TRUE(store.upsert_table(make_table(table_id, "orders")).ok());
    ASSERT_TRUE(store.upsert_replica(make_replica(replica_id, node_id)).ok());
    ASSERT_TRUE(
        store.upsert_resource_pool(ResourcePool{"pool-a"}).ok());

    std::unique_ptr<ISdmMetaStore> snapshot = store.clone_memory_snapshot();
    ASSERT_TRUE(snapshot != nullptr);

    ASSERT_TRUE(store.delete_table(table_id).ok());
    ASSERT_TRUE(store.delete_replica(replica_id).ok());
    ASSERT_TRUE(store.delete_resource_pool("pool-a").ok());

    TablePtr table;
    ASSERT_TRUE(snapshot->get_table(table_id, table).ok());
    ASSERT_TRUE(table != nullptr);
    EXPECT_EQ(table->spec.table_name, "orders");

    ReplicaPtr replica;
    ASSERT_TRUE(snapshot->get_replica(replica_id, replica).ok());
    ASSERT_TRUE(replica != nullptr);
    EXPECT_EQ(replica->spec.assign_node_id, node_id);

    ResourcePoolPtr pool;
    ASSERT_TRUE(snapshot->get_resource_pool("pool-a", pool).ok());
    ASSERT_TRUE(pool != nullptr);
    EXPECT_EQ(pool->name, "pool-a");
}

}  // namespace
}  // namespace adviskv::sdm
