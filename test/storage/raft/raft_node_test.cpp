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
        base_dir_ =
            adviskv::test::make_unique_test_dir("raft_node", sequence_++);
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
        return LogEntry{term, index, op_type, std::move(key), std::move(value)};
    }
    ReplicaID replica_id_{101, 7, 2};
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
        LogEntry entry{1, 2, WriteOpType::PUT, "1", "1"};
        ASSERT_EQ(entries.back(), entry);
    }
    {
        RaftMeta meta;
        persist.load_raft_meta(meta);
        RaftMeta real_meta{1, replica_id_};
        ASSERT_EQ(meta, real_meta);
    }
}

// 进入recovering后，tick不应触发选举，RequestVote不应授票，propose应失败；
// 收到AppendEntries补齐entries后应退出recovering
TEST_F(RaftNodeTest, RecoveringBlocksElectionVoteAndProposeUntilCatchUp) {
    ReplicaID leader_id{101, 7, 1};
    std::vector<PeerMember> members{
        PeerMember{"leader", leader_id, {}},
        PeerMember{"self", replica_id_, {}},
    };
    RaftNode node{replica_id_, members, nullptr};

    node.enter_recovering();
    ASSERT_TRUE(node.is_recovering());

    for (int i = 1; i <= 30; i++) {
        node.tick();
    }
    ASSERT_EQ(node.role(), ReplicaRole::FOLLOWER);

    RequestVoteResult vote_result;
    node.handle_request_vote(RequestVoteParam{leader_id, replica_id_, 1, 0, 0},
                             vote_result);
    ASSERT_FALSE(vote_result.vote_granted);

    auto [status, new_index] = node.propose(WriteOpType::PUT, "k", "v");
    ASSERT_TRUE(status.fail());
    ASSERT_EQ(new_index, -1);

    AppendEntriesResult append_result;
    AppendEntriesParam append_param{leader_id,
                                    replica_id_,
                                    1,
                                    {make_entry(1, 1, WriteOpType::PUT, "k1", "v1"),
                                     make_entry(1, 2, WriteOpType::PUT, "k2", "v2")},
                                    0,
                                    0,
                                    2};
    node.handle_append_entries(append_param, append_result);

    ASSERT_TRUE(append_result.success);
    ASSERT_EQ(node.commit_index(), 2);
    ASSERT_FALSE(node.is_recovering());
}

// 进入recovering后，收到有效install_snapshot时应退出recovering
TEST_F(RaftNodeTest, RecoveringFinishesWhenSnapshotCoversTarget) {
    RaftNode node{replica_id_, members_, nullptr};

    node.enter_recovering();
    ASSERT_TRUE(node.is_recovering());

    node.install_snapshot(5, 2, 2);

    ASSERT_EQ(node.snapshot_index(), 5);
    ASSERT_EQ(node.commit_index(), 5);
    ASSERT_FALSE(node.is_recovering());
}

}  // namespace adviskv::storage