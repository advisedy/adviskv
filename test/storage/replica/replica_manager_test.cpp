#include "storage/replica/replica_manager.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <string>
#include <vector>

#include "storage/model/param.h"
#include "storage/replica/replica.h"
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
            replica_id,
            EngineType::MAP,
            Endpoint{"127.0.0.1", 18080},
            {PeerMember{"node-1", replica_id, Endpoint{"127.0.0.1", 18080}}},
            base_dir_.string(),
        };
    }

    static inline int sequence_{0};

    fs::path base_dir_;
};

// 空ReplicaManager按ID/shard查询应返回nullptr，replicas列表应为空
TEST_F(ReplicaManagerTest, EmptyManagerReturnsNullForMissingReplica) {
    ReplicaManager manager(base_dir_.string());

    EXPECT_EQ(manager.get_replica_by_id({1, 1, 1}), nullptr);
    EXPECT_EQ(manager.get_replica_by_shard({1, 1}), nullptr);
    EXPECT_TRUE(manager.get_replicas().empty());
    EXPECT_EQ(manager.get_data_dir(), base_dir_.string());
}

// add_replica后应能按ID和shard索引查到对应replica
TEST_F(ReplicaManagerTest, AddReplicaIndexesByIdAndShard) {
    ReplicaManager manager(base_dir_.string());
    ReplicaID replica_id{7, 3, 0};

    Status status = manager.add_replica(make_param(replica_id));
    ASSERT_TRUE(status.ok())
        << static_cast<int>(status.code()) << " " << status.msg();

    ReplicaPtr by_id = manager.get_replica_by_id(replica_id);
    ASSERT_NE(by_id, nullptr);
    EXPECT_EQ(by_id->get_replica_id(), replica_id);

    ReplicaPtr by_shard = manager.get_replica_by_shard(
        {replica_id.table_id, replica_id.shard_index});
    ASSERT_NE(by_shard, nullptr);
    EXPECT_EQ(by_shard->get_replica_id(), replica_id);

    std::vector<ReplicaPtr> replicas = manager.get_replicas();
    ASSERT_EQ(replicas.size(), 1U);
    EXPECT_EQ(replicas[0]->get_replica_id(), replica_id);
}

// 重复添加相同replica_id或相同shard的replica应返回INVALID_ARGUMENT
TEST_F(ReplicaManagerTest, RejectsDuplicateReplicaAndDuplicateShard) {
    ReplicaManager manager(base_dir_.string());
    ReplicaID replica_id{9, 1, 0};

    ASSERT_TRUE(manager.add_replica(make_param(replica_id)).ok());

    Status duplicate_id_status = manager.add_replica(make_param(replica_id));
    EXPECT_TRUE(duplicate_id_status.ok());

    ReplicaID another_replica_same_shard{9, 1, 1};
    Status duplicate_shard_status =
        manager.add_replica(make_param(another_replica_same_shard));
    EXPECT_EQ(duplicate_shard_status.code(), StatusCode::ALREADY_EXIST);
}

// delete_replica后应无法再按ID或shard查到该replica
TEST_F(ReplicaManagerTest, DeleteReplicaRemovesIndexes) {
    ReplicaManager manager(base_dir_.string());
    ReplicaID replica_id{5, 2, 0};

    ASSERT_TRUE(manager.add_replica(make_param(replica_id)).ok());
    fs::path replica_dir =
        base_dir_ / (std::to_string(replica_id.table_id) + "-" +
                     std::to_string(replica_id.shard_index));
    ASSERT_TRUE(fs::exists(replica_dir));

    ASSERT_TRUE(manager.delete_replica(replica_id).ok());

    EXPECT_EQ(manager.get_replica_by_id(replica_id), nullptr);
    EXPECT_EQ(manager.get_replica_by_shard(
                  {replica_id.table_id, replica_id.shard_index}),
              nullptr);
    EXPECT_TRUE(manager.get_replicas().empty());
    EXPECT_FALSE(fs::exists(replica_dir));
}

// delete_replica后，即使调用方还持有旧ReplicaPtr，也不能继续进入该replica。
TEST_F(ReplicaManagerTest, DeleteReplicaShutsDownExistingReferences) {
    ReplicaManager manager(base_dir_.string());
    ReplicaID replica_id{6, 2, 0};

    ASSERT_TRUE(manager.add_replica(make_param(replica_id)).ok());
    fs::path replica_dir =
        base_dir_ / (std::to_string(replica_id.table_id) + "-" +
                     std::to_string(replica_id.shard_index));
    ASSERT_TRUE(fs::exists(replica_dir));

    ReplicaPtr replica = manager.get_replica_by_id(replica_id);
    ASSERT_NE(replica, nullptr);

    ASSERT_TRUE(manager.delete_replica(replica_id).ok());
    EXPECT_FALSE(fs::exists(replica_dir));

    Status status = replica->put(PutParam{"after-delete", "value"});
    EXPECT_EQ(status.code(), StatusCode::ERROR);

    Replica::ReplicaStateForTest state{};
    status = replica->get_replica_state_for_test(state);
    EXPECT_EQ(status.code(), StatusCode::ERROR);
}

// recover应扫描磁盘上的replica_meta，并重建内存里的replica索引
TEST_F(ReplicaManagerTest, RecoverScansDiskAndRebuildsReplicaIndexes) {
    ReplicaID replica_id{12, 4, 0};

    {
        ReplicaManager manager(base_dir_.string());
        ASSERT_TRUE(manager.add_replica(make_param(replica_id)).ok());
    }

    ReplicaManager recovered(base_dir_.string());
    recovered.recover();

    ReplicaPtr by_id = recovered.get_replica_by_id(replica_id);
    ASSERT_NE(by_id, nullptr);
    EXPECT_EQ(by_id->get_replica_id(), replica_id);

    ReplicaPtr by_shard = recovered.get_replica_by_shard(
        ShardID{replica_id.table_id, replica_id.shard_index});
    ASSERT_NE(by_shard, nullptr);
    EXPECT_EQ(by_shard->get_replica_id(), replica_id);
    EXPECT_EQ(recovered.get_replicas().size(), 1U);
}

}  // namespace
}  // namespace adviskv::storage