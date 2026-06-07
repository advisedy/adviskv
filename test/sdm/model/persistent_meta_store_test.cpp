#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "sdm/model/i_sdm_metastore.h"

namespace adviskv::sdm {
namespace {

Table make_table(TableID table_id, const std::string& table_name) {
    TableState state{};
    state.desired = TableDesired::PRESENT;
    state.phase = TablePhase::CREATING;
    state.update_ts = 100;
    return Table{table_id,
                 TableSpec{table_name, 11, "commerce", 2, 2, "pool-a",
                           "create-table-" + std::to_string(table_id)},
                 state};
}

Node make_node(const NodeID& id) {
    return Node{id,
                NodeSpec{"pool-a", "dc-a", NodeStatus::ONLINE},
                NodeState{Endpoint{"127.0.0.1", 18080}, 200},
                NodeDerived{}};
}

Replica make_replica(const ReplicaID& replica_id, const NodeID& node_id) {
    ReplicaState state{};
    state.desired = ReplicaDesired::PRESENT;
    state.phase = ReplicaPhase::READY;
    state.observed_raft_role = ReplicaRole::FOLLOWER;
    state.observed_endpoint = Endpoint{"127.0.0.1", 18080};
    state.term = 7;
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

class FakeSdmPersistEngine : public ISdmPersistEngine {
   public:
    Status init() override { return Status::OK(); }

    Status close() override { return Status::OK(); }

    Status save_sdm_meta(const SdmPersistedRecord& record) override {
        ++save_count;
        last_saved_record = record;
        if (save_status.fail()) {
            return save_status;
        }
        saved_records.push_back(record);
        return Status::OK();
    }

    Status load_sdm_meta(SdmPersistedRecord& record) override {
        record = load_record;
        return load_status;
    }

    Status save_status = Status::OK();
    Status load_status = Status::OK();
    SdmPersistedRecord load_record;
    SdmPersistedRecord last_saved_record;
    std::vector<SdmPersistedRecord> saved_records;
    int save_count{0};
};

PersistentMetaStore make_store(FakeSdmPersistEngine** fake_out) {
    auto fake = std::make_unique<FakeSdmPersistEngine>();
    *fake_out = fake.get();
    return PersistentMetaStore(std::make_unique<MemoryMetaStore>(),
                               std::move(fake));
}

}  // namespace

// 检测 commit_with
// 的正常流程：副本内存修改成功并持久化成功后，新的内存状态才对外可见。
TEST(PersistentMetaStoreTest, CommitWithPublishesMemoryAfterPersistSuccess) {
    FakeSdmPersistEngine* fake{nullptr};
    PersistentMetaStore store = make_store(&fake);

    Table table = make_table(1001, "orders");
    Status status = store.upsert_table(table);

    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(fake->save_count, 1);
    ASSERT_EQ(fake->saved_records.size(), 1U);
    ASSERT_EQ(fake->last_saved_record.tables.size(), 1U);
    EXPECT_EQ(fake->last_saved_record.tables[1001].spec.table_name, "orders");

    TablePtr stored_table;
    ASSERT_TRUE(store.get_table(1001, stored_table).ok());
    ASSERT_NE(stored_table, nullptr);
    EXPECT_EQ(stored_table->spec.table_name, "orders");

    ASSERT_TRUE(store.upsert_node(make_node("node-a")).ok());
    ASSERT_TRUE(
        store.upsert_replica(make_replica(ReplicaID{1001, 0, 0}, "node-a"))
            .ok());
    ASSERT_TRUE(
        store.upsert_resource_pool(ResourcePool{"pool-a"}).ok());
    ASSERT_TRUE(store.upsert_shard_route(make_route(ShardID{1001, 0})).ok());

    EXPECT_EQ(fake->save_count, 5);
    EXPECT_EQ(fake->last_saved_record.tables.size(), 1U);
    EXPECT_EQ(fake->last_saved_record.nodes.size(), 1U);
    EXPECT_EQ(fake->last_saved_record.replicas.size(), 1U);
    EXPECT_EQ(fake->last_saved_record.resource_pools.size(), 1U);
    EXPECT_EQ(fake->last_saved_record.shard_routes.size(), 1U);
}

// 检测 commit_with 里 persist_record
// 失败时，返回错误且当前内存状态不会被替换成修改后的副本。
TEST(PersistentMetaStoreTest, CommitWithKeepsOldMemoryWhenPersistFails) {
    FakeSdmPersistEngine* fake{nullptr};
    auto memory_store = std::make_unique<MemoryMetaStore>();
    ASSERT_TRUE(
        memory_store->upsert_table(make_table(1001, "old_orders")).ok());

    auto fake_engine = std::make_unique<FakeSdmPersistEngine>();
    fake = fake_engine.get();
    fake->save_status = Status::ERROR("save failed");

    PersistentMetaStore store(std::move(memory_store), std::move(fake_engine));

    Status status = store.upsert_table(make_table(1001, "new_orders"));

    ASSERT_TRUE(status.fail());
    EXPECT_EQ(status.msg(), "save failed");
    ASSERT_EQ(fake->save_count, 1);
    ASSERT_EQ(fake->last_saved_record.tables.size(), 1U);
    EXPECT_EQ(fake->last_saved_record.tables[1001].spec.table_name,
              "new_orders");

    TablePtr stored_table;
    ASSERT_TRUE(store.get_table(1001, stored_table).ok());
    ASSERT_NE(stored_table, nullptr);
    EXPECT_EQ(stored_table->spec.table_name, "old_orders");

    fake->save_status = Status::OK();
    status = store.upsert_table(make_table(1001, "new_orders"));

    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(store.get_table(1001, stored_table).ok());
    ASSERT_NE(stored_table, nullptr);
    EXPECT_EQ(stored_table->spec.table_name, "new_orders");
}

}  // namespace adviskv::sdm