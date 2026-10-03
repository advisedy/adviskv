#include "meta/store/meta_index.h"

#include <algorithm>
#include <vector>

#include <gtest/gtest.h>

namespace adviskv::meta {
namespace {

Table make_table(TableID table_id, const std::string& db_name, const std::string& table_name) {
    TableState state{};
    state.phase = TablePhase::CREATING;
    return Table{table_id,
                 TableSpec{11, db_name, table_name, EngineType::MAP, make_shard_ranges(2), 2, "pool-a"},
                 state};
}

Node make_node(const NodeID& id, const std::string& resource_pool) {
    return Node{id, NodeMeta{resource_pool, "dc-a"}, NodeState{NodeStatus::ONLINE, Endpoint{"127.0.0.1", 18080}}};
}

Replica make_replica(const ReplicaID& replica_id, const NodeID& node_id) {
    ReplicaState state{};
    state.phase = ReplicaPhase::READY;
    return Replica{replica_id, ReplicaSpec{"dc-a", node_id, EngineType::MAP}, state};
}

std::vector<NodeID> sorted_nodes(std::vector<NodeID> nodes) {
    std::sort(nodes.begin(), nodes.end());
    return nodes;
}

std::vector<ReplicaID> sorted_replicas(std::vector<ReplicaID> replicas) {
    std::sort(replicas.begin(), replicas.end(), [](const ReplicaID& lhs, const ReplicaID& rhs) {
        if (lhs.table_id != rhs.table_id) {
            return lhs.table_id < rhs.table_id;
        }
        if (lhs.shard_index != rhs.shard_index) {
            return lhs.shard_index < rhs.shard_index;
        }
        return lhs.replica_seq < rhs.replica_seq;
    });
    return replicas;
}

}  // namespace

// 检测 runtime index 的正常 upsert、查询、更新和删除流程。
TEST(MetaIndexTest, NormalIndexFlowWorks) {
    MetaIndex index;

    Table table = make_table(1001, "commerce", "orders");
    index.on_table_upsert(nullptr, table);
    TableID table_id = -1;
    ASSERT_TRUE(index.find_table_by_name("commerce", "orders", table_id).ok());
    EXPECT_EQ(table_id, 1001);

    Table deleting = make_table(1001, "commerce", "orders");
    deleting.state.phase = TablePhase::DELETING;
    index.on_table_upsert(&table, deleting);
    ASSERT_TRUE(index.find_table_by_name("commerce", "orders", table_id).ok());
    EXPECT_EQ(table_id, 1001);

    Table deleted = deleting;
    deleted.state.phase = TablePhase::DELETED;
    index.on_table_upsert(&deleting, deleted);
    EXPECT_EQ(index.find_table_by_name("commerce", "orders", table_id).code(), StatusCode::TABLE_NOT_FOUND);

    index.on_table_upsert(nullptr, table);

    Table renamed = make_table(1001, "commerce_v2", "orders_v2");
    index.on_table_upsert(&table, renamed);
    EXPECT_EQ(index.find_table_by_name("commerce", "orders", table_id).code(), StatusCode::TABLE_NOT_FOUND);
    ASSERT_TRUE(index.find_table_by_name("commerce_v2", "orders_v2", table_id).ok());
    EXPECT_EQ(table_id, 1001);

    Node node_a = make_node("node-a", "pool-a");
    Node node_b = make_node("node-b", "pool-a");
    index.on_node_upsert(nullptr, node_a);
    index.on_node_upsert(nullptr, node_b);
    std::vector<NodeID> nodes;
    ASSERT_TRUE(index.list_nodes_by_resource_pool("pool-a", nodes).ok());
    EXPECT_EQ(sorted_nodes(nodes), std::vector<NodeID>({"node-a", "node-b"}));

    Node migrated_node_a = make_node("node-a", "pool-b");
    index.on_node_upsert(&node_a, migrated_node_a);
    ASSERT_TRUE(index.list_nodes_by_resource_pool("pool-a", nodes).ok());
    EXPECT_EQ(sorted_nodes(nodes), std::vector<NodeID>({"node-b"}));
    ASSERT_TRUE(index.list_nodes_by_resource_pool("pool-b", nodes).ok());
    EXPECT_EQ(sorted_nodes(nodes), std::vector<NodeID>({"node-a"}));

    ReplicaID replica_id{1001, 0, 0};
    Replica replica = make_replica(replica_id, "node-a");
    index.on_replica_upsert(nullptr, replica);
    std::vector<ReplicaID> replicas;
    ASSERT_TRUE(index.list_replicas_by_shard(ShardID{1001, 0}, replicas).ok());
    EXPECT_EQ(sorted_replicas(replicas), std::vector<ReplicaID>({replica_id}));
    ASSERT_TRUE(index.list_replicas_by_node("node-a", replicas).ok());
    EXPECT_EQ(sorted_replicas(replicas), std::vector<ReplicaID>({replica_id}));

    Replica moved_replica = make_replica(replica_id, "node-b");
    index.on_replica_upsert(&replica, moved_replica);
    ASSERT_TRUE(index.list_replicas_by_node("node-a", replicas).ok());
    EXPECT_TRUE(replicas.empty());
    ASSERT_TRUE(index.list_replicas_by_node("node-b", replicas).ok());
    EXPECT_EQ(sorted_replicas(replicas), std::vector<ReplicaID>({replica_id}));

    index.on_replica_delete(moved_replica);
    ASSERT_TRUE(index.list_replicas_by_shard(ShardID{1001, 0}, replicas).ok());
    EXPECT_TRUE(replicas.empty());
    index.on_table_delete(renamed);
    EXPECT_EQ(index.find_table_by_name("commerce_v2", "orders_v2", table_id).code(), StatusCode::TABLE_NOT_FOUND);
}

// 验证 db 名字索引：upsert、改名、删除路径都通。
TEST(MetaIndexTest, DbNameIndexWorks) {
    MetaIndex index;

    Database db_a{11, "commerce", "zone-a"};
    index.on_db_upsert(nullptr, db_a);
    DatabaseID db_id = -1;
    ASSERT_TRUE(index.find_db_by_name("commerce", db_id).ok());
    EXPECT_EQ(db_id, 11);

    Database renamed{11, "commerce_v2", "zone-a"};
    index.on_db_upsert(&db_a, renamed);
    EXPECT_EQ(index.find_db_by_name("commerce", db_id).code(), StatusCode::DB_NOT_FOUND);
    ASSERT_TRUE(index.find_db_by_name("commerce_v2", db_id).ok());
    EXPECT_EQ(db_id, 11);

    index.on_db_delete(renamed);
    EXPECT_EQ(index.find_db_by_name("commerce_v2", db_id).code(), StatusCode::DB_NOT_FOUND);
}

}  // namespace adviskv::meta
