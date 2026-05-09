#include "storage/raft/raft_node.h"

#include <gtest/gtest.h>

#include "common/status.h"
#include "common/type.h"
#include "storage/model/param.h"
#include "storage/persist/persist_engine.h"
#include "test_env.h"

namespace adviskv::storage {
namespace fs = std::filesystem;

class RaftNodeTest : public ::testing::Test {
   protected:
    void SetUp() override {
        base_dir_ = adviskv::test::make_unique_test_dir("raft_node", sequence_++);
        ASSERT_TRUE(fs::create_directories(base_dir_)) << base_dir_.string();
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(base_dir_, ec);
    }

    PersistEngine make_engine() const {
        return PersistEngine(base_dir_.string(), replica_id_);
    }

    static LogEntry make_entry(Term term, LogIndex index, WriteOpType op_type,
                               std::string key, std::string value) {
        return LogEntry{
            .term = term,
            .index = index,
            .op_type = op_type,
            .key = std::move(key),
            .value = std::move(value),
        };
    }
    ReplicaID replica_id_{
        .table_id = 101, .shard_index = 7, .replica_index = 2};
    std::vector<PeerMember> members_{PeerMember{"", replica_id_, {}}};

   private:
    static inline int sequence_{0};

    fs::path base_dir_;
};

// 测试member只有自己一个的时候选举是否成功
TEST_F(RaftNodeTest, test_1) {
    PersistEngine persist = make_engine();
    {
        Status status = persist.init();
        ASSERT_EQ(status, Status::OK());
    }
    RaftNode node{replica_id_, members_, &persist};
    for (int i = 1; i <= 30; i++) {
        node.tick();
    }
    ASSERT_EQ(node.role(), ReplicaRole::LEADER);
}

// 测试当memeber_size=1，自己是leader之后，更新commit_idx，wal和meta信息的更新是否成功
TEST_F(RaftNodeTest, test_2) {
    PersistEngine persist = make_engine();
    {
        Status status = persist.init();
        ASSERT_EQ(status, Status::OK());
    }

    RaftNode node{replica_id_, members_, &persist};
    for (int i = 1; i <= 30; i++) {
        node.tick();
    }
    ASSERT_EQ(node.role(), ReplicaRole::LEADER);
    auto [status, new_index] = node.propose(WriteOpType::PUT, "1", "1");
    ASSERT_EQ(status, Status::OK());
    std::vector<LogEntry> entries;
    status = persist.read_wal_batch(entries);
    ASSERT_EQ(status, Status::OK());
    ASSERT_EQ((int)entries.size(),
              2);  // 一个是选举发的no-op ， 一个是put放进去的
    {
        LogEntry entry{.key = "1",
                       .value = "1",
                       .index = 2,
                       .op_type = WriteOpType::PUT,
                       .term = 1};
        ASSERT_EQ(entries.back(), entry);
    }
    {
        RaftMeta meta;
        persist.load_raft_meta(meta);
        RaftMeta real_meta{
            .commit_index = 2, .current_term = 1, .voted_for = replica_id_};
        ASSERT_EQ(meta, real_meta);
    }
}

// 测试一下

}  // namespace adviskv::storage