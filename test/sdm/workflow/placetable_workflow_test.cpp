#include "sdm/workflow/placetable_workflow.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <string>
#include <vector>

#include "common/status.h"
#include "sdm/model/sdm_store.h"
#include "sdm/selector/node_selector/node_selector.h"

namespace adviskv::sdm {
namespace {

Node make_node(const NodeID& id, const std::string& pool, int32_t port,
               int32_t owned_replica_count = 0,
               NodeStatus status = NodeStatus::ONLINE) {
    return Node{
        .id = id,
        .spec{
            .resource_pool = pool,
            .dc = "dc-a",
            .status = status,
        },
        .state{
            .endpoint = Endpoint{.ip = "127.0.0.1", .port = port},
        },
        .derived{
            .owned_replica_count = owned_replica_count,
        },
    };
}

Table make_placing_table() {
    return Table{
        .table_id = 1001,
        .spec{
            .table_name = "orders",
            .db_id = 11,
            .db_name = "commerce",
            .shard_count = 2,
            .replica_count = 2,
            .resource_pool = "pool-a",
        },
        .state{
            .status = TableStatus::CREATEING,
            .lifecycle = TableLifecycle::PLACING,
        },
    };
}

Table make_creating_replicas_table() {
    Table table = make_placing_table();
    table.state.lifecycle = TableLifecycle::CREATING_REPLICAS;
    return table;
}

class FakeStorageClient : public IStorageClient {
   public:
    Status create_replica(const CreateReplicaParam& param) override {
        requests.push_back(param);
        if (fail_on_call > 0 && requests.size() == fail_on_call) {
            return failure_status;
        }
        return Status::OK();
    }

    std::vector<CreateReplicaParam> requests;
    size_t fail_on_call{0}; // 等到第几次创建replica的时候就会失败
    Status failure_status = Status::ERROR("fake create replica failed");
};

class PlaceTableWorkflowTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_TRUE(
            store_.put_node(make_node("node-a", "pool-a", 18080, 2)).ok());
        ASSERT_TRUE(
            store_.put_node(make_node("node-b", "pool-a", 18081, 0)).ok());
        ASSERT_TRUE(
            store_.put_node(make_node("node-c", "pool-a", 18082, 1)).ok());
    }

    Status put_pending_replicas(const Table& table) {
        Status status = store_.put_table(table);
        if (status.fail()) {
            return status;
        }

        for (int32_t shard_idx = 0; shard_idx < table.spec.shard_count;
             ++shard_idx) {
            for (int32_t replica_idx = 0;
                 replica_idx < table.spec.replica_count; ++replica_idx) {
                const bool is_leader = (replica_idx == 0);
                const NodeID node_id = is_leader ? "node-a" : "node-b";
                Replica replica{
                    .replica_id{
                        .table_id = table.table_id,
                        .shard_index = shard_idx,
                        .replica_index = replica_idx,
                    },
                    .spec{
                        .dc = "dc-a",
                        .assign_node_id = node_id,
                        .role = is_leader ? ReplicaRole::LEADER
                                          : ReplicaRole::FOLLOWER,
                        .status = ReplicaStatus::PENDING,
                    },
                    .state{
                        .endpoint = Endpoint{.ip = "127.0.0.1",
                                             .port = 18080 + replica_idx},
                        .role = ReplicaRole::FOLLOWER,
                    },
                };
                status = store_.put_replica(replica);
                if (status.fail()) {
                    return status;
                }
            }
        }
        return Status::OK();
    }

    SdmStore store_{SdmMetaStoreType::MEMORY};
};

// 默认节点选择器应按负载优先选择节点，并为每个shard生成指定副本数的放置结果
TEST_F(PlaceTableWorkflowTest,
       DefaultSelectorPlacesEachShardOnLeastLoadedNodes) {
    DefaultNodeSelector selector(&store_);
    TablePlacementResult result;

    Status status = selector.select_table_nodes(
        PlaceNodesParam{
            .resource_pool = "pool-a",
            .shard_count = 2,
            .replica_count = 2,
        },
        result);

    ASSERT_TRUE(status.ok()) << status.msg();
    ASSERT_EQ(result.shards.size(), 2U);

    ASSERT_EQ(result.shards[0].nodes.size(), 2U);
    EXPECT_EQ(result.shards[0].shard_index, 0);
    EXPECT_EQ(result.shards[0].nodes[0]->id, "node-b");
    EXPECT_EQ(result.shards[0].nodes[1]->id, "node-c");

    ASSERT_EQ(result.shards[1].nodes.size(), 2U);
    EXPECT_EQ(result.shards[1].shard_index, 1);
    EXPECT_EQ(result.shards[1].nodes[0]->id, "node-b");
    EXPECT_EQ(result.shards[1].nodes[1]->id, "node-a");
}

// 默认节点选择器应过滤离线节点和无效endpoint，候选节点不足时返回失败状态
TEST(DefaultNodeSelectorTest,
     RejectsWhenOnlineNodesWithValidEndpointAreNotEnough) {
    SdmStore store{SdmMetaStoreType::MEMORY};
    ASSERT_TRUE(store.put_node(make_node("online-node", "pool-a", 18080)).ok());
    ASSERT_TRUE(store
                    .put_node(make_node("offline-node", "pool-a", 18081, 0,
                                        NodeStatus::OFFLINE))
                    .ok());
    ASSERT_TRUE(
        store.put_node(make_node("bad-endpoint-node", "pool-a", 0)).ok());

    DefaultNodeSelector selector(&store);
    TablePlacementResult result;
    Status status = selector.select_table_nodes(
        PlaceNodesParam{
            .resource_pool = "pool-a",
            .shard_count = 1,
            .replica_count = 2,
        },
        result);

    EXPECT_TRUE(status.fail());
    EXPECT_EQ(status.code(), StatusCode::INVALID_ARGUMENT);
    EXPECT_TRUE(result.shards.empty());
}

// table处于PLACING阶段时，workflow应根据放置结果创建replica并推进到CREATING_REPLICAS
TEST_F(PlaceTableWorkflowTest, StepPlacingCreatesReplicasAndTransitionsTable) {
    DefaultNodeSelector selector(&store_);
    PlaceTableWorkflow workflow(&store_, nullptr, &selector);
    Table table = make_placing_table();

    Status status = workflow.step(table);

    ASSERT_TRUE(status.ok()) << status.msg();
    EXPECT_EQ(table.state.lifecycle, TableLifecycle::CREATING_REPLICAS);
    EXPECT_EQ(table.state.status, TableStatus::CREATEING);

    TablePtr stored_table;
    ASSERT_TRUE(store_.get_table(table.table_id, stored_table).ok());
    ASSERT_NE(stored_table, nullptr);
    EXPECT_EQ(stored_table->state.lifecycle, TableLifecycle::CREATING_REPLICAS);

    for (int32_t shard_idx = 0; shard_idx < table.spec.shard_count;
         ++shard_idx) {
        std::vector<ReplicaPtr> replicas;
        ASSERT_TRUE(
            store_
                .list_replicas_by_shard(ShardID{.table_id = table.table_id,
                                                .shard_index = shard_idx},
                                        replicas)
                .ok());
        std::sort(replicas.begin(), replicas.end(),
                  [](const ReplicaPtr& lhs, const ReplicaPtr& rhs) {
                      return lhs->replica_id.replica_index <
                             rhs->replica_id.replica_index;
                  });

        ASSERT_EQ(replicas.size(), 2U);
        EXPECT_EQ(replicas[0]->replica_id.replica_index, 0);
        EXPECT_EQ(replicas[0]->spec.role, ReplicaRole::LEADER);
        EXPECT_EQ(replicas[0]->spec.status, ReplicaStatus::PENDING);
        EXPECT_EQ(replicas[1]->replica_id.replica_index, 1);
        EXPECT_EQ(replicas[1]->spec.role, ReplicaRole::FOLLOWER);
        EXPECT_EQ(replicas[1]->spec.status, ReplicaStatus::PENDING);
    }
}

// CREATING_REPLICAS阶段应调用storage client创建所有PENDING
// replica，并把状态更新为ADDING
TEST_F(PlaceTableWorkflowTest,
       StepCreatingReplicasSendsRequestsAndMarksReplicasAdding) {
    FakeStorageClient client;
    PlaceTableWorkflow workflow(&store_, &client, nullptr);
    Table table = make_creating_replicas_table();
    ASSERT_TRUE(put_pending_replicas(table).ok());

    Status status = workflow.step(table);

    ASSERT_TRUE(status.ok()) << status.msg();
    EXPECT_EQ(table.state.lifecycle, TableLifecycle::CREATING_REPLICAS);

    std::vector<CreateReplicaParam> requests = client.requests;
    std::sort(
        requests.begin(), requests.end(),
        [](const CreateReplicaParam& lhs, const CreateReplicaParam& rhs) {
            if (lhs.replica_id.shard_index != rhs.replica_id.shard_index) {
                return lhs.replica_id.shard_index < rhs.replica_id.shard_index;
            }
            return lhs.replica_id.replica_index < rhs.replica_id.replica_index;
        });
    ASSERT_EQ(requests.size(), 4U);
    for (const CreateReplicaParam& request : requests) {
        EXPECT_EQ(request.replica_id.table_id, table.table_id);
        EXPECT_EQ(request.engine_type, EngineType::MAP);
        EXPECT_EQ(request.members.size(), 2U);
        EXPECT_FALSE(request.endpoint.ip.empty());
        EXPECT_GT(request.endpoint.port, 0);
    }
    EXPECT_EQ(requests[0].replica_id, (ReplicaID{table.table_id, 0, 0}));
    EXPECT_EQ(requests[1].replica_id, (ReplicaID{table.table_id, 0, 1}));
    EXPECT_EQ(requests[2].replica_id, (ReplicaID{table.table_id, 1, 0}));
    EXPECT_EQ(requests[3].replica_id, (ReplicaID{table.table_id, 1, 1}));

    for (int32_t shard_idx = 0; shard_idx < table.spec.shard_count;
         ++shard_idx) {
        std::vector<ReplicaPtr> replicas;
        ASSERT_TRUE(
            store_
                .list_replicas_by_shard(ShardID{.table_id = table.table_id,
                                                .shard_index = shard_idx},
                                        replicas)
                .ok());
        ASSERT_EQ(replicas.size(), 2U);
        for (const ReplicaPtr& replica : replicas) {
            ASSERT_NE(replica, nullptr);
            EXPECT_EQ(replica->spec.status, ReplicaStatus::ADDING);
        }
    }
}

// storage
// client创建replica失败时，workflow应把table推进到ROLLING_BACK并记录错误信息
TEST_F(PlaceTableWorkflowTest, StepCreatingReplicasRollsBackWhenClientFails) {
    FakeStorageClient client;
    client.fail_on_call = 2;
    PlaceTableWorkflow workflow(&store_, &client, nullptr);
    Table table = make_creating_replicas_table();
    ASSERT_TRUE(put_pending_replicas(table).ok());

    Status status = workflow.step(table);

    ASSERT_TRUE(status.ok()) << status.msg();
    EXPECT_EQ(client.requests.size(), 2U);
    EXPECT_EQ(table.state.lifecycle, TableLifecycle::ROLLING_BACK);
    EXPECT_EQ(table.state.status, TableStatus::CREATEING);
    EXPECT_FALSE(table.state.last_error_msg.empty());

    TablePtr stored_table;
    ASSERT_TRUE(store_.get_table(table.table_id, stored_table).ok());
    ASSERT_NE(stored_table, nullptr);
    EXPECT_EQ(stored_table->state.lifecycle, TableLifecycle::ROLLING_BACK);
    EXPECT_FALSE(stored_table->state.last_error_msg.empty());
}

}  // namespace
}  // namespace adviskv::sdm
