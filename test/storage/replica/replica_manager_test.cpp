#include "storage/replica/replica_manager.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <string>
#include <vector>

#include "storage/model/param.h"
#include "test/test_env.h"

namespace fs = std::filesystem;

namespace adviskv::storage {
namespace {

class ReplicaManagerTest : public ::testing::Test {
   protected:
    void SetUp() override {
        base_dir_ =
            adviskv::test::make_unique_test_dir("replica_manager", sequence_++);
        ASSERT_TRUE(fs::create_directories(base_dir_)) << base_dir_.string();
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(base_dir_, ec);
    }

    ReplicaInitParam make_param(ReplicaID replica_id) const {
        return ReplicaInitParam{
            .replica_id = replica_id,
            .engine_type = EngineType::MAP,
            .local_endpoint = Endpoint{"127.0.0.1", 18080},
            .members =
                {
                    PeerMember{
                        .node_id = "node-1",
                        .replica_id = replica_id,
                        .endpoint = Endpoint{"127.0.0.1", 18080},
                    },
                },
            .data_dir = base_dir_.string(),
        };
    }

    static inline int sequence_{0};

    fs::path base_dir_;
};

TEST_F(ReplicaManagerTest, EmptyManagerReturnsNullForMissingReplica) {
    ReplicaManager manager(base_dir_.string());

    EXPECT_EQ(manager.get_replica_by_id({1, 1, 1}), nullptr);
    EXPECT_EQ(manager.get_replica_by_shard({1, 1}), nullptr);
    EXPECT_TRUE(manager.get_replicas().empty());
    EXPECT_EQ(manager.get_data_dir(), base_dir_.string());
}

TEST_F(ReplicaManagerTest, AddReplicaIndexesByIdAndShard) {
    ReplicaManager manager(base_dir_.string());
    ReplicaID replica_id{.table_id = 7, .shard_index = 3, .replica_index = 0};

    Status status = manager.add_replica(make_param(replica_id));
    ASSERT_TRUE(status.ok())
        << static_cast<int>(status.code()) << " " << status.msg();

    Replica* by_id = manager.get_replica_by_id(replica_id);
    ASSERT_NE(by_id, nullptr);
    EXPECT_EQ(by_id->get_replica_id(), replica_id);

    Replica* by_shard = manager.get_replica_by_shard(
        {replica_id.table_id, replica_id.shard_index});
    ASSERT_NE(by_shard, nullptr);
    EXPECT_EQ(by_shard->get_replica_id(), replica_id);

    std::vector<Replica*> replicas = manager.get_replicas();
    ASSERT_EQ(replicas.size(), 1U);
    EXPECT_EQ(replicas[0]->get_replica_id(), replica_id);
}

TEST_F(ReplicaManagerTest, RejectsDuplicateReplicaAndDuplicateShard) {
    ReplicaManager manager(base_dir_.string());
    ReplicaID replica_id{.table_id = 9, .shard_index = 1, .replica_index = 0};

    ASSERT_TRUE(manager.add_replica(make_param(replica_id)).ok());

    Status duplicate_id_status = manager.add_replica(make_param(replica_id));
    EXPECT_EQ(duplicate_id_status.code(), StatusCode::INVALID_ARGUMENT);

    ReplicaID another_replica_same_shard{
        .table_id = 9,
        .shard_index = 1,
        .replica_index = 1,
    };
    Status duplicate_shard_status =
        manager.add_replica(make_param(another_replica_same_shard));
    EXPECT_EQ(duplicate_shard_status.code(), StatusCode::INVALID_ARGUMENT);
}

TEST_F(ReplicaManagerTest, DeleteReplicaRemovesIndexes) {
    ReplicaManager manager(base_dir_.string());
    ReplicaID replica_id{.table_id = 5, .shard_index = 2, .replica_index = 0};

    ASSERT_TRUE(manager.add_replica(make_param(replica_id)).ok());
    ASSERT_TRUE(manager.delete_replica(replica_id).ok());

    EXPECT_EQ(manager.get_replica_by_id(replica_id), nullptr);
    EXPECT_EQ(manager.get_replica_by_shard(
                  {replica_id.table_id, replica_id.shard_index}),
              nullptr);
    EXPECT_TRUE(manager.get_replicas().empty());
}

}  // namespace
}  // namespace adviskv::storage
