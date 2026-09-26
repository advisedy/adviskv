#include "meta/store/meta_store.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "test/meta/store/meta_store_test_helper.h"
#include "test_env.h"

namespace fs = std::filesystem;

namespace adviskv::meta {
namespace {

Table make_table(TableID table_id, const std::string& db_name,
                 const std::string& table_name,
                 TableDesired desired = TableDesired::PRESENT) {
    TableState state{};
    state.desired = desired;
    state.phase = TablePhase::READY;
    return Table{table_id,
                 TableSpec{table_name, 11, db_name, 2, 2, "pool-a",
                           "create-table-" + std::to_string(table_id), EngineType::MAP,
                           make_shard_ranges(2)},
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

int persistent_sequence{0};

}  // namespace

TEST(MetaStoreTest, MemoryStoreInitAllowsReadWrite) {
    MetaStore store{MemoryMetaStoreParam{}};

    Status status = store.init();
    ASSERT_TRUE(status.ok()) << status.to_string();

    status = store.read_with([](const MetaStoreTxn&) {
        return Status::OK();
    });
    EXPECT_TRUE(status.ok()) << status.to_string();

    status = store.write_with([](MetaStoreTxn&) {
        return Status::OK();
    });
    EXPECT_TRUE(status.ok()) << status.to_string();
}

// 检测 MetaStore 的正常写入、查询、更新和删除流程。
TEST(MetaStoreTest, NormalStoreFlowWorks) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());

    Table table = make_table(1001, "commerce", "orders");
    ASSERT_TRUE(store_test::put_table(store, table).ok());
    TableOr table_out;
    ASSERT_TRUE(store_test::get_table(store, 1001, table_out).ok());
    ASSERT_FALSE(table_out.is_empty());
    EXPECT_EQ(table_out->spec.name, "orders");

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
    ASSERT_TRUE(store_test::get_table_by_name(store, "commerce", "orders", table_by_name).ok());
    EXPECT_TRUE(table_by_name.is_empty());
}

TEST(MetaStoreTest, FailedRouteEntryMutationDoesNotPublish) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());

    ShardID shard_id{1001, 0};
    ShardRoute route = make_route(shard_id);
    route.replicas.push_back(
        RouteEntry{ReplicaID{1001, 0, 1}, "node-b", "127.0.0.2", 18081,
                   ReplicaRole::FOLLOWER, 7});
    ASSERT_TRUE(store_test::put_shard_route(store, route).ok());

    Status status = store.write_with([&](MetaStoreTxn& txn) {
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

// 同一事务内写入后立即查询应可见（read-your-writes），且索引同步。
TEST(MetaStoreTest, WriteThenReadWithinSameTxnSeesWrites) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());

    Table table = make_table(1001, "commerce", "orders");
    Node node = make_node("node-a", "pool-a");
    ReplicaID replica_id{1001, 0, 0};
    Replica replica = make_replica(replica_id, "node-a");

    Status status = store.write_with([&](MetaStoreTxn& txn) -> Status {
        RETURN_IF_INVALID_STATUS(txn.put_table(table))
        RETURN_IF_INVALID_STATUS(txn.put_node(node))
        RETURN_IF_INVALID_STATUS(txn.put_replica(replica))

        TableOr table_out;
        RETURN_IF_INVALID_STATUS(txn.get_table_by_name("commerce", "orders", table_out))
        if (table_out.is_empty()) {
            return Status::ERROR("table not visible in same txn");
        }
        std::vector<Replica> by_node;
        RETURN_IF_INVALID_STATUS(txn.list_replicas_by_node("node-a", by_node))
        if (by_node.size() != 1U) {
            return Status::ERROR("replica not visible by node in same txn");
        }
        std::vector<Replica> by_shard;
        RETURN_IF_INVALID_STATUS(txn.list_replicas_by_shard(ShardID{1001, 0}, by_shard))
        if (by_shard.size() != 1U) {
            return Status::ERROR("replica not visible by shard in same txn");
        }
        return Status::OK();
    });
    ASSERT_TRUE(status.ok()) << status.to_string();

    // 提交后依然可见。
    TableOr table_out;
    ASSERT_TRUE(store_test::get_table_by_name(store, "commerce", "orders", table_out).ok());
    ASSERT_FALSE(table_out.is_empty());
    std::vector<Replica> replicas;
    ASSERT_TRUE(store_test::list_replicas_by_node(store, "node-a", replicas).ok());
    EXPECT_EQ(replica_ids(replicas), std::vector<ReplicaID>({replica_id}));
}

TEST(MetaStoreTest, MissingTableQueriesClearReusedOutput) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    const Table existing = make_table(1001, "commerce", "orders");
    ASSERT_TRUE(store_test::put_table(store, existing).ok());
    TableOr out = existing;
    ASSERT_TRUE(store_test::get_table(store, 9999, out).ok());
    EXPECT_TRUE(out.is_empty());
    out = existing;
    ASSERT_TRUE(store_test::get_table_by_name(store, "commerce", "missing", out).ok());
    EXPECT_TRUE(out.is_empty());
    out = existing;
    ASSERT_TRUE(store_test::get_table_by_name(store, "missing", "orders", out).ok());
    EXPECT_TRUE(out.is_empty());
}

TEST(MetaStoreTest, RuntimeAndEmptyTransactionsDoNotInheritDurableDirty) {
    const fs::path dir = test::make_unique_test_dir("meta_store_txn_dirty", persistent_sequence++);
    MetaStore store{PersistentMetaStoreParam{dir.string()}};
    ASSERT_TRUE(store.init().ok());
    ASSERT_TRUE(store.write_with([](MetaStoreTxn& txn) {
        return txn.put_database(Database{11, "commerce", "zone-a"});
    }).ok());
    ASSERT_TRUE(fs::exists(dir / "meta.json"));

    // 阻止下一次快照写入。纯运行时事务和空事务不应访问这个路径。
    ASSERT_TRUE(fs::create_directory(dir / "meta.json.tmp"));
    ASSERT_TRUE(store.write_with([](MetaStoreTxn& txn) {
        EXPECT_FALSE(txn.durable_dirty());
        return Status::OK();
    }).ok());
    ASSERT_TRUE(store.write_with([&](MetaStoreTxn& txn) -> Status {
        EXPECT_FALSE(txn.durable_dirty());
        RETURN_IF_INVALID_STATUS(txn.put_node(make_node("node-a", "pool-a")))
        RETURN_IF_INVALID_STATUS(txn.put_shard_route(make_route(ShardID{1001, 0})))
        EXPECT_FALSE(txn.durable_dirty());
        return Status::OK();
    }).ok());

    // 持久写确实仍会落盘；失败时连同节点和索引一起回滚。
    EXPECT_TRUE(store.write_with([&](MetaStoreTxn& txn) -> Status {
        RETURN_IF_INVALID_STATUS(txn.put_database(Database{12, "failed", "zone-a"}))
        RETURN_IF_INVALID_STATUS(txn.put_node(make_node("node-a", "pool-b")))
        EXPECT_TRUE(txn.durable_dirty());
        return Status::OK();
    }).fail());
    ASSERT_TRUE(store.read_with([](const MetaStoreTxn& txn) -> Status {
        DatabaseOr db;
        RETURN_IF_INVALID_STATUS(txn.get_database_by_name("failed", db))
        EXPECT_TRUE(db.is_empty());
        NodeOr node;
        RETURN_IF_INVALID_STATUS(txn.get_node("node-a", node))
        EXPECT_TRUE(node.has_value());
        if (node.has_value()) {
            EXPECT_EQ(node->meta.resource_pool, "pool-a");
        }
        std::vector<Node> nodes;
        RETURN_IF_INVALID_STATUS(txn.list_nodes_by_resource_pool("pool-b", nodes))
        EXPECT_TRUE(nodes.empty());
        return Status::OK();
    }).ok());
    ASSERT_TRUE(store.write_with([](MetaStoreTxn& txn) {
        EXPECT_FALSE(txn.durable_dirty());
        return Status::OK();
    }).ok());
    fs::remove_all(dir);
}

TEST(MetaStoreTest, PersistentStoreReportsCorruptedMetaOnInit) {
    const fs::path dir = test::make_unique_test_dir("meta_store_corrupt", 1);
    fs::create_directories(dir);
    {
        std::ofstream out(dir / "meta.json", std::ios::binary);
        out << "corrupted meta";
    }

    MetaStore store{PersistentMetaStoreParam{dir.string()}};

    Status init_status = store.init();
    EXPECT_TRUE(init_status.fail()) << init_status.to_string();
}

TEST(MetaStoreTest, RouteOnlyWriteDoesNotCreatePersistentSnapshot) {
    fs::path data_dir = adviskv::test::make_unique_test_dir(
        "meta_store_route_only", persistent_sequence++);
    std::error_code ec;
    fs::remove_all(data_dir, ec);

    {
        MetaStore store{PersistentMetaStoreParam{data_dir.string()}};
        ASSERT_TRUE(store.init().ok());
        ASSERT_TRUE(store_test::put_shard_route(
                        store, make_route(ShardID{1001, 0}))
                        .ok());

        ShardRouteOr route;
        ASSERT_TRUE(store_test::get_shard_route(store, ShardID{1001, 0}, route)
                        .ok());
        ASSERT_FALSE(route.is_empty());
        EXPECT_FALSE(fs::exists(data_dir / "meta.json"));
    }

    {
        MetaStore reloaded{PersistentMetaStoreParam{data_dir.string()}};
        ASSERT_TRUE(reloaded.init().ok());
        ShardRouteOr route;
        ASSERT_TRUE(store_test::get_shard_route(reloaded, ShardID{1001, 0}, route)
                        .ok());
        EXPECT_TRUE(route.is_empty());
    }

    fs::remove_all(data_dir, ec);
}

// 检测 PERSISTENT 类型的 MetaStore 写入后，重新加载仍能恢复 table 和 group；node 当前仍是内存态。
TEST(MetaStoreTest, WriteWithPersistentStoreSurvivesReload) {
    fs::path data_dir = adviskv::test::make_unique_test_dir(
        "meta_store_persistent", persistent_sequence++);
    std::error_code ec;
    fs::remove_all(data_dir, ec);

    ShardID shard_id{1001, 0};
    ReplicaID replica_id{1001, 0, 0};

    {
        MetaStore store{PersistentMetaStoreParam{data_dir.string()}};
        ASSERT_TRUE(store.init().ok());
        Status status = store.write_with([&](MetaStoreTxn& txn) {
            Status put_status = txn.put_database(Database{11, "commerce", "zone-a"});
            if (put_status.fail()) {
                return put_status;
            }
            put_status = txn.put_table(make_table(1001, "commerce", "orders"));
            if (put_status.fail()) {
                return put_status;
            }
            put_status = txn.put_node(make_node("node-a", "pool-a"));
            if (put_status.fail()) {
                return put_status;
            }
            put_status = txn.put_replica(make_replica(replica_id, "node-a"));
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
        MetaStore reloaded{PersistentMetaStoreParam{data_dir.string()}};
        ASSERT_TRUE(reloaded.init().ok());
        TableOr table;
        ASSERT_TRUE(store_test::get_table(reloaded, 1001, table).ok());
        ASSERT_FALSE(table.is_empty());
        EXPECT_EQ(table->spec.name, "orders");

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

}  // namespace adviskv::meta
