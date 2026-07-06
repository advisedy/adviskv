#include "sdm/store/sdm_store.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "sdm/store/sdm_store_test_helper.h"
#include "test_env.h"

namespace fs = std::filesystem;

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
    return Node{id, NodeMeta{resource_pool, "dc-a"},
                NodeState{NodeStatus::ONLINE, Endpoint{"127.0.0.1", port},
                          100},
                NodeDerived{}};
}

Replica make_replica(const ReplicaID& replica_id, const NodeID& node_id) {
    ReplicaState state{};
    state.desired = ReplicaDesired::PRESENT;
    state.phase = ReplicaPhase::READY;
    state.observed_raft_role = ReplicaRole::FOLLOWER;
    state.observed_endpoint = Endpoint{"127.0.0.1", 18080};
    return Replica{replica_id,
                   ReplicaSpec{"dc-a", node_id, EngineType::MAP}, state};
}

ShardRoute make_route(const ShardID& shard_id) {
    return ShardRoute{
        shard_id,
        {RouteEntry{ReplicaID{shard_id.table_id, shard_id.shard_index, 0},
                    "node-a", "127.0.0.1", 18080, ReplicaRole::LEADER, 7}}};
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
                  return lhs.replica_seq < rhs.replica_seq;
              });
    return ids;
}

class FailOnceOnReplicaDeleteIndex : public SdmRuntimeIndex {
   public:
    std::unique_ptr<SdmRuntimeIndex> clone() const override {
        return std::make_unique<FailOnceOnReplicaDeleteIndex>(*this);
    }

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

int persistent_sequence{0};

}  // namespace

TEST(SdmStoreTest, MemoryStoreInitAllowsReadWrite) {
    SdmStore store{MemoryMetaStoreParam{}};

    Status status = store.init();
    ASSERT_TRUE(status.ok()) << status.to_string();

    status = store.read_with([](const SdmStoreTxn&) {
        return Status::OK();
    });
    EXPECT_TRUE(status.ok()) << status.to_string();

    status = store.write_with([](SdmStoreTxn&) {
        return Status::OK();
    });
    EXPECT_TRUE(status.ok()) << status.to_string();
}

TEST(SdmStoreTest, InitRejectsNullRuntimeIndex) {
    SdmStore store(MemoryMetaStoreParam{},
                   std::unique_ptr<SdmRuntimeIndex>{},
                   std::make_unique<SdmRuntimeStore>());

    Status status = store.init();

    EXPECT_EQ(status.code(), StatusCode::INVALID_ARGUMENT);
    EXPECT_EQ(status.msg(), "sdm runtime index is nullptr");
}

// 检测 SdmStore 的正常写入、查询、更新和删除流程。
TEST(SdmStoreTest, NormalStoreFlowWorks) {
    SdmStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());

    Table table = make_table(1001, "commerce", "orders");
    ASSERT_TRUE(store_test::put_table(store, table).ok());
    TableOr table_out;
    ASSERT_TRUE(store_test::get_table(store, 1001, table_out).ok());
    ASSERT_FALSE(table_out.is_empty());
    EXPECT_EQ(table_out->spec.table_name, "orders");

    TableOr table_by_name;
    ASSERT_TRUE(store_test::get_table_by_name(store, "commerce", "orders",
                                              table_by_name)
                    .ok());
    ASSERT_FALSE(table_by_name.is_empty());
    EXPECT_EQ(table_by_name->table_id, 1001);

    ASSERT_TRUE(
        store_test::put_node(store, make_node("node-a", "pool-a")).ok());
    ASSERT_TRUE(
        store_test::put_node(store, make_node("node-b", "pool-a")).ok());
    std::vector<Node> nodes;
    ASSERT_TRUE(
        store_test::list_nodes_by_resource_pool(store, "pool-a", nodes).ok());
    EXPECT_EQ(node_ids(nodes), std::vector<NodeID>({"node-a", "node-b"}));

    ReplicaID replica_id{1001, 0, 0};
    ASSERT_TRUE(
        store_test::put_replica(store, make_replica(replica_id, "node-a"))
            .ok());
    std::vector<Replica> replicas;
    ASSERT_TRUE(
        store_test::list_replicas_by_shard(store, ShardID{1001, 0}, replicas)
            .ok());
    EXPECT_EQ(replica_ids(replicas), std::vector<ReplicaID>({replica_id}));
    ASSERT_TRUE(
        store_test::list_replicas_by_node(store, "node-a", replicas).ok());
    EXPECT_EQ(replica_ids(replicas), std::vector<ReplicaID>({replica_id}));

    Replica moved_replica = make_replica(replica_id, "node-b");
    ASSERT_TRUE(store_test::put_replica(store, moved_replica).ok());
    ASSERT_TRUE(
        store_test::list_replicas_by_node(store, "node-a", replicas).ok());
    EXPECT_TRUE(replicas.empty());
    ASSERT_TRUE(
        store_test::list_replicas_by_node(store, "node-b", replicas).ok());
    EXPECT_EQ(replica_ids(replicas), std::vector<ReplicaID>({replica_id}));

    ShardID shard_id{1001, 0};
    ASSERT_TRUE(store_test::put_shard_route(store, make_route(shard_id)).ok());
    ShardRouteOr route;
    ASSERT_TRUE(store_test::get_shard_route(store, shard_id, route).ok());
    ASSERT_FALSE(route.is_empty());
    ASSERT_EQ(route->replicas.size(), 1U);

    ASSERT_TRUE(store_test::delete_replica(store, replica_id).ok());
    ASSERT_TRUE(
        store_test::list_replicas_by_shard(store, shard_id, replicas).ok());
    EXPECT_TRUE(replicas.empty());
    ASSERT_TRUE(store_test::delete_shard_route(store, shard_id).ok());
    ASSERT_TRUE(store_test::get_shard_route(store, shard_id, route).ok());
    EXPECT_TRUE(route.is_empty());
    ASSERT_TRUE(store_test::delete_table(store, 1001).ok());
    EXPECT_EQ(store_test::get_table_by_name(store, "commerce", "orders",
                                            table_by_name)
                  .code(),
              StatusCode::TABLE_NOT_FOUND);
}

TEST(SdmStoreTest, FailedRouteEntryMutationDoesNotPublish) {
    SdmStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());

    ShardID shard_id{1001, 0};
    ShardRoute route = make_route(shard_id);
    route.replicas.push_back(
        RouteEntry{ReplicaID{1001, 0, 1}, "node-b", "127.0.0.2", 18081,
                   ReplicaRole::FOLLOWER, 7});
    ASSERT_TRUE(store_test::put_shard_route(store, route).ok());

    Status status = store.write_with([&](SdmStoreTxn& txn) {
        RETURN_IF_INVALID_STATUS(
            txn.del_shard_route_entry(shard_id, ReplicaID{1001, 0, 0}))
        return Status::ERROR("rollback route mutation");
    });
    ASSERT_TRUE(status.fail());

    ShardRouteOr loaded;
    ASSERT_TRUE(store_test::get_shard_route(store, shard_id, loaded).ok());
    ASSERT_FALSE(loaded.is_empty());
    ASSERT_EQ(loaded->replicas.size(), 2U);
    EXPECT_EQ(loaded->replicas[0].replica_id, ReplicaID({1001, 0, 0}));
    EXPECT_EQ(loaded->replicas[1].replica_id, ReplicaID({1001, 0, 1}));
}

TEST(SdmStoreTest, PersistentStoreReportsCorruptedMetaOnInit) {
    const fs::path dir = test::make_unique_test_dir("sdm_store_corrupt", 1);
    fs::create_directories(dir);
    {
        std::ofstream out(dir / "sdm_meta", std::ios::binary);
        out << "corrupted sdm meta";
    }

    SdmStore store{PersistentMetaStoreParam{dir.string()}};

    Status init_status = store.init();
    EXPECT_TRUE(init_status.fail()) << init_status.to_string();
}

// 检测 runtime index 更新失败时，SdmStore 会 rebuild runtime index
// 并恢复索引一致性。
TEST(SdmStoreTest, RebuildsRuntimeIndexWhenReplicaDeleteIndexUpdateFails) {
    auto failing_index = std::make_unique<FailOnceOnReplicaDeleteIndex>();
    SdmStore store{MemoryMetaStoreParam{}, std::move(failing_index),
                   std::make_unique<SdmRuntimeStore>()};
    ASSERT_TRUE(store.init().ok());

    ASSERT_TRUE(
        store_test::put_table(store, make_table(1001, "commerce", "orders"))
            .ok());
    ASSERT_TRUE(
        store_test::put_node(store, make_node("node-a", "pool-a")).ok());
    ASSERT_TRUE(
        store_test::put_node(store, make_node("node-b", "pool-a")).ok());
    ReplicaID deleted_id{1001, 0, 0};
    ReplicaID kept_id{1001, 0, 1};
    ASSERT_TRUE(
        store_test::put_replica(store, make_replica(deleted_id, "node-a"))
            .ok());
    ASSERT_TRUE(
        store_test::put_replica(store, make_replica(kept_id, "node-b")).ok());
    ASSERT_TRUE(
        store_test::put_shard_route(store, make_route(ShardID{1001, 0})).ok());

    Status status = store_test::delete_replica(store, deleted_id);

    ASSERT_TRUE(status.ok()) << status.to_string();
    ReplicaOr deleted;
    ASSERT_TRUE(store_test::get_replica(store, deleted_id, deleted).ok());
    EXPECT_TRUE(deleted.is_empty());

    std::vector<Replica> replicas;
    ASSERT_TRUE(
        store_test::list_replicas_by_node(store, "node-a", replicas).ok());
    EXPECT_TRUE(replicas.empty());
    ASSERT_TRUE(
        store_test::list_replicas_by_node(store, "node-b", replicas).ok());
    EXPECT_EQ(replica_ids(replicas), std::vector<ReplicaID>({kept_id}));
    ASSERT_TRUE(
        store_test::list_replicas_by_shard(store, ShardID{1001, 0}, replicas)
            .ok());
    EXPECT_EQ(replica_ids(replicas), std::vector<ReplicaID>({kept_id}));

    TableOr table;
    ASSERT_TRUE(
        store_test::get_table_by_name(store, "commerce", "orders", table).ok());
    ASSERT_FALSE(table.is_empty());
    std::vector<Node> nodes;
    ASSERT_TRUE(
        store_test::list_nodes_by_resource_pool(store, "pool-a", nodes).ok());
    EXPECT_EQ(node_ids(nodes), std::vector<NodeID>({"node-a", "node-b"}));
    ShardRouteOr route;
    ASSERT_TRUE(
        store_test::get_shard_route(store, ShardID{1001, 0}, route).ok());
    ASSERT_FALSE(route.is_empty());
    ASSERT_EQ(route->replicas.size(), 1U);
    EXPECT_EQ(route->replicas[0].replica_id, deleted_id);
}

TEST(SdmStoreTest, RouteOnlyWriteDoesNotCreatePersistentSnapshot) {
    fs::path data_dir = adviskv::test::make_unique_test_dir(
        "sdm_store_route_only", persistent_sequence++);
    std::error_code ec;
    fs::remove_all(data_dir, ec);

    {
        SdmStore store{PersistentMetaStoreParam{data_dir.string()}};
        ASSERT_TRUE(store.init().ok());
        ASSERT_TRUE(store_test::put_shard_route(
                        store, make_route(ShardID{1001, 0}))
                        .ok());

        ShardRouteOr route;
        ASSERT_TRUE(store_test::get_shard_route(store, ShardID{1001, 0}, route)
                        .ok());
        ASSERT_FALSE(route.is_empty());
        EXPECT_FALSE(fs::exists(data_dir / "sdm_meta"));
    }

    {
        SdmStore reloaded{PersistentMetaStoreParam{data_dir.string()}};
        ASSERT_TRUE(reloaded.init().ok());
        ShardRouteOr route;
        ASSERT_TRUE(store_test::get_shard_route(reloaded, ShardID{1001, 0}, route)
                        .ok());
        EXPECT_TRUE(route.is_empty());
    }

    fs::remove_all(data_dir, ec);
}

// 检测 PERSISTENT 类型的 SdmStore 写入后，重新加载仍能恢复 table 和 group；node 当前仍是内存态。
TEST(SdmStoreTest, WriteWithPersistentStoreSurvivesReload) {
    fs::path data_dir = adviskv::test::make_unique_test_dir(
        "sdm_store_persistent", persistent_sequence++);
    std::error_code ec;
    fs::remove_all(data_dir, ec);

    ShardID shard_id{1001, 0};
    ReplicaID replica_id{1001, 0, 0};

    {
        SdmStore store{PersistentMetaStoreParam{data_dir.string()}};
        ASSERT_TRUE(store.init().ok());
        Status status = store.write_with([&](SdmStoreTxn& txn) {
            Status put_status =
                txn.put_table(make_table(1001, "commerce", "orders"));
            if (put_status.fail()) {
                return put_status;
            }
            put_status = txn.put_node(make_node("node-a", "pool-a"));
            if (put_status.fail()) {
                return put_status;
            }

            ReplicaGroup group;
            group.shard_id = shard_id;
            group.mode = ReplicaGroupMode::RAFT_RECONFIG;
            group.target_replica_count = 1;
            group.desired_members = {replica_id};
            group.seq_allocator = IDAllocator<ReplicaSeq>(1);
            return txn.put_replica_group(group);
        });
        ASSERT_TRUE(status.ok()) << status.to_string();
    }

    {
        SdmStore reloaded{PersistentMetaStoreParam{data_dir.string()}};
        ASSERT_TRUE(reloaded.init().ok());
        TableOr table;
        ASSERT_TRUE(store_test::get_table(reloaded, 1001, table).ok());
        ASSERT_FALSE(table.is_empty());
        EXPECT_EQ(table->spec.table_name, "orders");

        ReplicaGroupOr group;
        ASSERT_TRUE(
            store_test::get_replica_group(reloaded, shard_id, group).ok());
        ASSERT_FALSE(group.is_empty());
        EXPECT_EQ(group->mode, ReplicaGroupMode::RAFT_RECONFIG);
        EXPECT_EQ(group->target_replica_count, 1);
        EXPECT_EQ(group->desired_members, std::vector<ReplicaID>({replica_id}));
        EXPECT_EQ(group->seq_allocator.current_id(), 1);

        NodeOr node;
        ASSERT_TRUE(store_test::get_node(reloaded, "node-a", node).ok());
        EXPECT_TRUE(node.is_empty());

        std::vector<Node> nodes;
        ASSERT_TRUE(
            store_test::list_nodes_by_resource_pool(reloaded, "pool-a", nodes)
                .ok());
        EXPECT_TRUE(nodes.empty());
    }

    fs::remove_all(data_dir, ec);
}

}  // namespace adviskv::sdm
