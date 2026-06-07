#include "sdm/model/sdm_store.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

namespace adviskv::sdm {
namespace {

Table make_table(TableID table_id, const std::string& db_name,
                 const std::string& table_name,
                 TableDesired desired = TableDesired::PRESENT) {
    TableState state{};
    state.desired = desired;
    state.phase = TablePhase::READY;
    return Table{table_id,
                 TableSpec{table_name, 11, db_name, 2, 2, "pool-a",
                           "create-table-" + std::to_string(table_id)},
                 state};
}

Node make_node(const NodeID& id, const std::string& resource_pool,
               int32_t port = 18080) {
    return Node{id,
                NodeSpec{resource_pool, "dc-a", NodeStatus::ONLINE},
                NodeState{Endpoint{"127.0.0.1", port}, 100},
                NodeDerived{}};
}

Replica make_replica(const ReplicaID& replica_id, const NodeID& node_id) {
    ReplicaState state{};
    state.desired = ReplicaDesired::PRESENT;
    state.phase = ReplicaPhase::READY;
    state.observed_raft_role = ReplicaRole::FOLLOWER;
    state.observed_endpoint = Endpoint{"127.0.0.1", 18080};
    return Replica{replica_id,
                   ReplicaSpec{"dc-a", node_id, EngineType::MAP, {}},
                   state};
}

ShardRoute make_route(const ShardID& shard_id) {
    return ShardRoute{shard_id,
                      {RouteEntry{ReplicaID{shard_id.table_id,
                                            shard_id.shard_index, 0},
                                  "node-a", "127.0.0.1", 18080,
                                  ReplicaRole::LEADER, 7}}};
}

std::vector<NodeID> node_ids(std::vector<Node> nodes) {
    std::vector<NodeID> ids;
    ids.reserve(nodes.size());
    for (const Node& node : nodes) {
        ids.push_back(node.id);
    }
    std::sort(ids.begin(), ids.end());
    return ids;
}

std::vector<ReplicaID> replica_ids(std::vector<Replica> replicas) {
    std::vector<ReplicaID> ids;
    ids.reserve(replicas.size());
    for (const Replica& replica : replicas) {
        ids.push_back(replica.replica_id);
    }
    std::sort(ids.begin(), ids.end(),
              [](const ReplicaID& lhs, const ReplicaID& rhs) {
                  if (lhs.table_id != rhs.table_id) {
                      return lhs.table_id < rhs.table_id;
                  }
                  if (lhs.shard_index != rhs.shard_index) {
                      return lhs.shard_index < rhs.shard_index;
                  }
                  return lhs.replica_index < rhs.replica_index;
              });
    return ids;
}

class FailOnceOnReplicaDeleteIndex : public SdmRuntimeIndex {
   public:
    Status on_replica_delete(const Replica& replica) override {
        if (!failed_) {
            failed_ = true;
            return Status::ERROR("injected runtime index delete failure");
        }
        return SdmRuntimeIndex::on_replica_delete(replica);
    }

   private:
    bool failed_{false};
};

}  // namespace

// 检测 SdmStore 的正常写入、查询、更新和删除流程。
TEST(SdmStoreTest, NormalStoreFlowWorks) {
    SdmStore store{SdmMetaStoreType::MEMORY};

    Table table = make_table(1001, "commerce", "orders");
    ASSERT_TRUE(store.put_table(table).ok());
    TableOr table_out;
    ASSERT_TRUE(store.get_table(1001, table_out).ok());
    ASSERT_FALSE(table_out.is_empty());
    EXPECT_EQ(table_out->spec.table_name, "orders");

    TableOr table_by_name;
    ASSERT_TRUE(
        store.get_table_by_name("commerce", "orders", table_by_name).ok());
    ASSERT_FALSE(table_by_name.is_empty());
    EXPECT_EQ(table_by_name->table_id, 1001);

    ASSERT_TRUE(store.put_node(make_node("node-a", "pool-a")).ok());
    ASSERT_TRUE(store.put_node(make_node("node-b", "pool-a")).ok());
    std::vector<Node> nodes;
    ASSERT_TRUE(store.list_nodes_by_resource_pool("pool-a", nodes).ok());
    EXPECT_EQ(node_ids(nodes), std::vector<NodeID>({"node-a", "node-b"}));

    ReplicaID replica_id{1001, 0, 0};
    ASSERT_TRUE(store.put_replica(make_replica(replica_id, "node-a")).ok());
    std::vector<Replica> replicas;
    ASSERT_TRUE(store.list_replicas_by_shard(ShardID{1001, 0}, replicas).ok());
    EXPECT_EQ(replica_ids(replicas), std::vector<ReplicaID>({replica_id}));
    ASSERT_TRUE(store.list_replicas_by_node("node-a", replicas).ok());
    EXPECT_EQ(replica_ids(replicas), std::vector<ReplicaID>({replica_id}));

    Replica moved_replica = make_replica(replica_id, "node-b");
    ASSERT_TRUE(store.put_replica(moved_replica).ok());
    ASSERT_TRUE(store.list_replicas_by_node("node-a", replicas).ok());
    EXPECT_TRUE(replicas.empty());
    ASSERT_TRUE(store.list_replicas_by_node("node-b", replicas).ok());
    EXPECT_EQ(replica_ids(replicas), std::vector<ReplicaID>({replica_id}));

    ShardID shard_id{1001, 0};
    ASSERT_TRUE(store.put_shard_route(make_route(shard_id)).ok());
    ShardRouteOr route;
    ASSERT_TRUE(store.get_shard_route(shard_id, route).ok());
    ASSERT_FALSE(route.is_empty());
    ASSERT_EQ(route->replicas.size(), 1U);

    ASSERT_TRUE(store.del_replica(replica_id).ok());
    ASSERT_TRUE(store.list_replicas_by_shard(shard_id, replicas).ok());
    EXPECT_TRUE(replicas.empty());
    ASSERT_TRUE(store.delete_shard_route(shard_id).ok());
    ASSERT_TRUE(store.get_shard_route(shard_id, route).ok());
    EXPECT_TRUE(route.is_empty());
    ASSERT_TRUE(store.delete_table(1001).ok());
    EXPECT_EQ(
        store.get_table_by_name("commerce", "orders", table_by_name).code(),
        StatusCode::TABLE_NOT_FOUND);
}

// 检测 runtime index 更新失败时，SdmStore 会 rebuild runtime index
// 并恢复索引一致性。
TEST(SdmStoreTest, RebuildsRuntimeIndexWhenReplicaDeleteIndexUpdateFails) {
    auto failing_index = std::make_unique<FailOnceOnReplicaDeleteIndex>();
    SdmStore store{SdmMetaStoreType::MEMORY, std::move(failing_index)};

    ASSERT_TRUE(store.put_table(make_table(1001, "commerce", "orders")).ok());
    ASSERT_TRUE(store.put_node(make_node("node-a", "pool-a")).ok());
    ASSERT_TRUE(store.put_node(make_node("node-b", "pool-a")).ok());
    ReplicaID deleted_id{1001, 0, 0};
    ReplicaID kept_id{1001, 0, 1};
    ASSERT_TRUE(store.put_replica(make_replica(deleted_id, "node-a")).ok());
    ASSERT_TRUE(store.put_replica(make_replica(kept_id, "node-b")).ok());
    ASSERT_TRUE(store.put_shard_route(make_route(ShardID{1001, 0})).ok());

    Status status = store.del_replica(deleted_id);

    ASSERT_TRUE(status.ok()) << status.to_string();
    ReplicaOr deleted;
    ASSERT_TRUE(store.get_replica(deleted_id, deleted).ok());
    EXPECT_TRUE(deleted.is_empty());

    std::vector<Replica> replicas;
    ASSERT_TRUE(store.list_replicas_by_node("node-a", replicas).ok());
    EXPECT_TRUE(replicas.empty());
    ASSERT_TRUE(store.list_replicas_by_node("node-b", replicas).ok());
    EXPECT_EQ(replica_ids(replicas), std::vector<ReplicaID>({kept_id}));
    ASSERT_TRUE(store.list_replicas_by_shard(ShardID{1001, 0}, replicas).ok());
    EXPECT_EQ(replica_ids(replicas), std::vector<ReplicaID>({kept_id}));

    TableOr table;
    ASSERT_TRUE(store.get_table_by_name("commerce", "orders", table).ok());
    ASSERT_FALSE(table.is_empty());
    std::vector<Node> nodes;
    ASSERT_TRUE(store.list_nodes_by_resource_pool("pool-a", nodes).ok());
    EXPECT_EQ(node_ids(nodes), std::vector<NodeID>({"node-a", "node-b"}));
    ShardRouteOr route;
    ASSERT_TRUE(store.get_shard_route(ShardID{1001, 0}, route).ok());
    ASSERT_FALSE(route.is_empty());
}

}  // namespace adviskv::sdm