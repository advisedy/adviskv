#include "meta/store/meta_store.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include "test/meta/store/meta_store_test_helper.h"
#include "test_env.h"

namespace fs = std::filesystem;

namespace adviskv::meta {
namespace {

Table make_table(TableID table_id, const std::string& db_name,
                 const std::string& table_name) {
    TableState state{};
    state.phase = TablePhase::READY;
    return Table{table_id,
                 TableSpec{11, db_name, table_name, EngineType::MAP, make_shard_ranges(2), 2, "pool-a"},
                 state};
}

Node make_node(const NodeID& id, const std::string& resource_pool,
               int32_t port = 18080) {
    return Node{id, NodeMeta{resource_pool, "dc-a"},
                NodeState{NodeStatus::ONLINE, Endpoint{"127.0.0.1", port},
                          100}};
}

Replica make_replica(const ReplicaID& replica_id, const NodeID& node_id) {
    ReplicaState state{};
    state.phase = ReplicaPhase::READY;
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

// COW 克隆粒度：只写过 runtime 的事务不应克隆 durable/index 之外的组件，
// 更不应克隆 durable——这是本次重构的核心收益（心跳事务不再全量拷贝）。
TEST(MetaStoreTest, RuntimeOnlyTxnClonesOnlyRuntimeAndIndex) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    // 先造一些 durable 内容，保证"没克隆"不是因为本来为空。
    ASSERT_TRUE(store_test::put_table(store, make_table(1001, "commerce", "orders")).ok());

    Status status = store.write_with([&](MetaStoreTxn& txn) -> Status {
        EXPECT_FALSE(txn.durable_cloned_for_test());
        EXPECT_FALSE(txn.runtime_cloned_for_test());
        EXPECT_FALSE(txn.index_cloned_for_test());
        RETURN_IF_INVALID_STATUS(txn.put_replica_observed(ReplicaID{1001, 0, 0}, ReplicaObserved{}))
        // 纯 runtime 观测写：只应克隆 runtime。
        EXPECT_FALSE(txn.durable_cloned_for_test());
        EXPECT_TRUE(txn.runtime_cloned_for_test());
        EXPECT_FALSE(txn.index_cloned_for_test());
        return Status::OK();
    });
    ASSERT_TRUE(status.ok()) << status.to_string();

    // 提交后观测可见，且 durable 内容不受影响。
    ReplicaObservedOr observed;
    ASSERT_TRUE(store_test::get_replica_observed(store, ReplicaID{1001, 0, 0}, observed).ok());
    EXPECT_FALSE(observed.is_empty());
    TableOr table;
    ASSERT_TRUE(store_test::get_table(store, 1001, table).ok());
    EXPECT_FALSE(table.is_empty());
}

// 写 durable 的事务应克隆 durable 与 index（索引同步），但不必克隆 runtime。
TEST(MetaStoreTest, DurableTxnClonesDurableAndIndexNotRuntime) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    ASSERT_TRUE(store_test::put_node(store, make_node("node-a", "pool-a")).ok());

    Status status = store.write_with([&](MetaStoreTxn& txn) -> Status {
        RETURN_IF_INVALID_STATUS(txn.put_database(Database{11, "commerce", "zone-a"}))
        EXPECT_TRUE(txn.durable_cloned_for_test());
        EXPECT_TRUE(txn.index_cloned_for_test());  // db_name_index 同步更新
        EXPECT_FALSE(txn.runtime_cloned_for_test());
        return Status::OK();
    });
    ASSERT_TRUE(status.ok()) << status.to_string();

    // runtime 的既有节点仍然可见（未因 durable 事务被复制丢失）。
    NodeOr node;
    ASSERT_TRUE(store_test::get_node(store, "node-a", node).ok());
    EXPECT_FALSE(node.is_empty());
}

// 失败回滚：事务克隆了组件但返回失败，克隆丢弃后已发布状态必须逐字节不变，
// 且其他事务/读者完全看不到半成品。
TEST(MetaStoreTest, FailedTxnRollbackLeavesPublishedSnapshotUntouched) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    const Table existing = make_table(1001, "commerce", "orders");
    ASSERT_TRUE(store_test::put_table(store, existing).ok());

    Status status = store.write_with([&](MetaStoreTxn& txn) -> Status {
        RETURN_IF_INVALID_STATUS(txn.put_table(make_table(2002, "commerce", "ghost")))
        RETURN_IF_INVALID_STATUS(txn.put_node(make_node("node-x", "pool-x")))
        return Status::ERROR("rollback on purpose");
    });
    EXPECT_TRUE(status.fail());

    TableOr ghost;
    ASSERT_TRUE(store_test::get_table(store, 2002, ghost).ok());
    EXPECT_TRUE(ghost.is_empty());
    TableOr ghost_by_name;
    ASSERT_TRUE(store_test::get_table_by_name(store, "commerce", "ghost", ghost_by_name).ok());
    EXPECT_TRUE(ghost_by_name.is_empty());  // index 也已回滚
    NodeOr node_x;
    ASSERT_TRUE(store_test::get_node(store, "node-x", node_x).ok());
    EXPECT_TRUE(node_x.is_empty());
    TableOr kept;
    ASSERT_TRUE(store_test::get_table(store, 1001, kept).ok());
    EXPECT_FALSE(kept.is_empty());
}

// 读快照稳定性：read_with 拿到的是一次性加载的不可变快照，事务体内多次
// 读取必然看到同一瞬间的状态（旧实现靠 shared_mutex 挡写者保证，新实现
// 靠快照本身保证）。
TEST(MetaStoreTest, ReaderSeesStableSnapshot) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    ASSERT_TRUE(store_test::put_table(store, make_table(1001, "commerce", "orders")).ok());

    // 读者事务内先读一次，再（通过外部写者）提交新数据，读者快照不应变化。
    std::atomic<bool> writer_done{false};
    Status status = store.read_with([&](const MetaStoreTxn& txn) -> Status {
        TableOr before;
        RETURN_IF_INVALID_STATUS(txn.get_table(1001, before))
        EXPECT_FALSE(before.is_empty());
        TableOr mid;
        RETURN_IF_INVALID_STATUS(txn.get_table(2002, mid))
        EXPECT_TRUE(mid.is_empty());
        writer_done.store(true);
        // 再次确认：同一事务内始终看得到旧快照。
        TableOr again;
        RETURN_IF_INVALID_STATUS(txn.get_table(2002, again))
        EXPECT_TRUE(again.is_empty());
        return Status::OK();
    });
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_TRUE(writer_done.load());

    // 提交另一份数据后，新读者能看到，旧快照的读者不受影响（上面已验证）。
    ASSERT_TRUE(store_test::put_table(store, make_table(2002, "commerce", "later")).ok());
    TableOr later;
    ASSERT_TRUE(store_test::get_table(store, 2002, later).ok());
    EXPECT_FALSE(later.is_empty());
}

// 并发冒烟：写者持续提交（runtime 心跳式写 + durable 写交替），读者持续
// 全表扫描。验证无锁读路径与快照发布在真实线程交叉下不崩、不死锁、每次
// 读都拿到自洽状态（observed 条数与写入进度单调相容）。
TEST(MetaStoreTest, ConcurrentReadersAndWriterStayConsistent) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());

    constexpr int kIterations = 2000;
    std::atomic<int> committed_writes{0};
    std::atomic<bool> readers_stop{false};
    std::atomic<bool> reader_saw_inconsistency{false};

    // 预置一个 durable 基线，让读路径能顺带校验 durable 部分始终可见。
    ASSERT_TRUE(store_test::put_table(store, make_table(1001, "commerce", "orders")).ok());

    std::vector<std::thread> readers;
    for (int r = 0; r < 4; r++) {
        readers.emplace_back([&]() {
            while (!readers_stop.load()) {
                Status status = store.read_with([&](const MetaStoreTxn& txn) -> Status {
                    TableOr table;
                    RETURN_IF_INVALID_STATUS(txn.get_table(1001, table))
                    if (table.is_empty()) {
                        return Status::ERROR("baseline table vanished");
                    }
                    // 遍历全部 observed：数量不会超前于已开始写入的总数（读
                    // 快照不会包含没写过的 key），且每条的值必须和 key 编号
                    // 一致（能抓出错位的克隆/发布竞态）。
                    int seen = 0;
                    for (int i = 0; i < kIterations; i++) {
                        ReplicaObservedOr observed;
                        RETURN_IF_INVALID_STATUS(txn.get_replica_observed(ReplicaID{1001, 0, i}, observed))
                        if (!observed.is_empty()) {
                            if (observed->update_ts != i) {
                                return Status::ERROR("observed value does not match key seq");
                            }
                            seen++;
                        }
                    }
                    if (seen > committed_writes.load()) {
                        return Status::ERROR("reader observed more writes than committed");
                    }
                    return Status::OK();
                });
                if (status.fail()) {
                    reader_saw_inconsistency.store(true);
                    return;
                }
            }
        });
    }

    for (int i = 0; i < kIterations; i++) {
        // 计数先于发布递增：读者拿到的快照只会落后、不会超前计数，检查才是
        // 无假阳性的必要条件。
        committed_writes.store(i + 1);
        Status status = store.write_with([&](MetaStoreTxn& txn) -> Status {
            ReplicaObserved observed;
            observed.update_ts = i;
            return txn.put_replica_observed(ReplicaID{1001, 0, i}, observed);
        });
        ASSERT_TRUE(status.ok()) << status.to_string();
        if (i % 500 == 0) {
            // 穿插 durable 写，触发落盘路径与快照整体替换。
            ASSERT_TRUE(store.write_with([&](MetaStoreTxn& txn) {
                return txn.put_database(Database{2000 + i, "db-" + std::to_string(i), "zone-a"});
            }).ok());
        }
    }

    readers_stop.store(true);
    for (std::thread& t : readers) {
        t.join();
    }
    EXPECT_FALSE(reader_saw_inconsistency.load());
}

}  // namespace adviskv::meta
