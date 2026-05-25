#include "storage/persist/replica_meta_persist_engine.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "common/type.h"
#include "test_env.h"
namespace fs = std::filesystem;

namespace adviskv::storage {

class ReplicaMetaPersistEngineTest : public ::testing::Test {
   protected:
    void SetUp() override {
        base_dir_ = adviskv::test::make_unique_test_dir("replica_meta_persist",
                                                        sequence_++);
        ASSERT_TRUE(fs::create_directories(base_dir_)) << base_dir_.string();
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(base_dir_, ec);
    }

    fs::path replica_dir(const ReplicaID& replica_id) const {
        return std::filesystem::path(base_dir_) /
               fmt::format("{}-{}", replica_id.table_id,
                           replica_id.shard_index);
    }

    fs::path meta_path(const ReplicaID& replica_id) const {
        return replica_dir(replica_id) / ReplicaMetaPersistEngine::kFileName;
    }

    static PeerMember make_member(std::string node_id, ReplicaID replica_id,
                                  std::string ip, int32_t port) {
        return PeerMember{std::move(node_id), replica_id,
                          Endpoint{std::move(ip), port}};
    }

    ReplicaMetaPayload make_payload(const ReplicaID& replica_id) const {
        return ReplicaMetaPayload{
            ReplicaInitParam{
                replica_id,
                EngineType::MAP,
                endpoint_,
                members_,
                base_dir_.string(),
            },
        };
    }

    ReplicaID replica_id_{101, 7, 2};

    Endpoint endpoint_{"127.0.0.1", 1000};
    std::vector<PeerMember> members_{
        make_member(
            "node-1",
            ReplicaID{101, 7, 0},
            "127.0.0.1", 1000),
        make_member(
            "node-2",
            ReplicaID{101, 7, 1},
            "127.0.0.1", 1001),
        make_member("node-3", replica_id_, "127.0.0.1", 1002),
    };

    static inline int sequence_{0};
    fs::path base_dir_;
};

// 检测正常的load和save函数是否可以用
TEST_F(ReplicaMetaPersistEngineTest, SaveAndLoadReplicaMetaRoundTrip) {
    ReplicaMetaPersistEngine persist{base_dir_.string()};

    ReplicaMetaPayload save_payload = make_payload(replica_id_);

    Status status = persist.save_replica_meta(save_payload);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    {
        ReplicaMetaPayload payload;
        status = persist.load_replica_meta(meta_path(replica_id_), payload);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

        EXPECT_EQ(payload.init_param.replica_id,
                  save_payload.init_param.replica_id);
        EXPECT_EQ(payload.init_param.engine_type,
                  save_payload.init_param.engine_type);
        EXPECT_EQ(payload.init_param.local_endpoint,
                  save_payload.init_param.local_endpoint);
        EXPECT_EQ(payload.init_param.members, save_payload.init_param.members);
        EXPECT_EQ(payload.init_param.data_dir, base_dir_.string());
    }
}

// 检测 std::vector<std::filesystem::path> scan_replica_meta_files() const;
TEST_F(ReplicaMetaPersistEngineTest,
       ScanReplicaMetaFilesReturnsOnlySavedMetas) {
    ReplicaMetaPersistEngine persist{base_dir_.string()};

    ReplicaID first{101, 7, 0};
    ReplicaID second{102, 8, 0};
    Status status = persist.save_replica_meta(make_payload(first));
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    status = persist.save_replica_meta(make_payload(second));
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    // 这里是一个干扰的，代表我们过滤掉这种了
    ASSERT_TRUE(fs::create_directories(base_dir_ / "empty-replica-dir"));
    {
        std::ofstream stray_file(base_dir_ / "not-a-replica-dir");
        ASSERT_TRUE(stray_file.is_open());
        stray_file << "not a replica meta";
    }

    std::vector<fs::path> paths = persist.scan_replica_meta_files();
    ASSERT_EQ(paths.size(), 2U);
    EXPECT_EQ(paths[0].filename(), ReplicaMetaPersistEngine::kFileName);
    EXPECT_EQ(paths[1].filename(), ReplicaMetaPersistEngine::kFileName);
    EXPECT_NE(std::find(paths.begin(), paths.end(), meta_path(first)),
              paths.end());
    EXPECT_NE(std::find(paths.begin(), paths.end(), meta_path(second)),
              paths.end());
}

// 检测空目录扫描和不存在的meta文件读取。
TEST_F(ReplicaMetaPersistEngineTest, EmptyOrMissingMetaReturnsNoDataOrError) {
    ReplicaMetaPersistEngine persist{base_dir_.string()};

    EXPECT_TRUE(persist.scan_replica_meta_files().empty());

    ReplicaMetaPayload payload;
    Status status = persist.load_replica_meta(meta_path(replica_id_), payload);
    EXPECT_TRUE(status.fail());
}

// 检测meta文件内容损坏时load失败。
TEST_F(ReplicaMetaPersistEngineTest, LoadCorruptedReplicaMetaFails) {
    ASSERT_TRUE(fs::create_directories(replica_dir(replica_id_)));
    {
        std::ofstream out(meta_path(replica_id_), std::ios::binary);
        ASSERT_TRUE(out.is_open());
        out << "corrupted replica meta"; // 覆盖写
    }

    ReplicaMetaPersistEngine persist{base_dir_.string()};
    ReplicaMetaPayload payload;
    Status status = persist.load_replica_meta(meta_path(replica_id_), payload);
    EXPECT_TRUE(status.fail());
}

}  // namespace adviskv::storage