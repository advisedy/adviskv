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

    fs::path persist_dir() const {
        return base_dir_ / (std::to_string(replica_id_.table_id) + "-" +
                            std::to_string(replica_id_.shard_index));
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
    PersistEngine persist = make_engine();
    Status persist_status = persist.init();
    ASSERT_EQ(persist_status, Status::OK());

    RaftNode node{replica_id_, members, &persist};

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
    PersistEngine persist = make_engine();
    Status status = persist.init();
    ASSERT_EQ(status, Status::OK());

    RaftNode node{replica_id_, members_, &persist};

    node.enter_recovering();
    ASSERT_TRUE(node.is_recovering());

    node.install_leader_snapshot(5, 2, 2);

    ASSERT_EQ(node.snapshot_index(), 5);
    ASSERT_EQ(node.commit_index(), 5);
    ASSERT_FALSE(node.is_recovering());
}

TEST_F(RaftNodeTest, HandleAppendEntriesRewritesWalWhenLeaderOverwritesConflict) {
    PersistEngine persist = make_engine();
    Status status = persist.init();
    ASSERT_EQ(status, Status::OK());

    std::vector<LogEntry> initial_entries{
        make_entry(1, 1, WriteOpType::PUT, "k1", "old1"),
        make_entry(1, 2, WriteOpType::PUT, "k2", "old2"),
        make_entry(1, 3, WriteOpType::PUT, "k3", "old3"),
    };
    status = persist.append_wal_batch(initial_entries);
    ASSERT_EQ(status, Status::OK());

    RaftNode node{replica_id_, members_, &persist};
    node.update_raft_meta(RaftMeta{2, std::nullopt});
    node.update_log_entries(initial_entries);

    std::vector<LogEntry> leader_entries{
        make_entry(2, 2, WriteOpType::PUT, "k2", "new2"),
        make_entry(2, 3, WriteOpType::PUT, "k3", "new3"),
    };
    ReplicaID leader_id{101, 7, 1};
    AppendEntriesResult result;
    node.handle_append_entries(
        AppendEntriesParam{leader_id, replica_id_, 2, leader_entries, 1, 1, 3},
        result);

    ASSERT_TRUE(result.success);

    std::vector<LogEntry> actual_entries;
    status = persist.read_wal_batch(actual_entries);
    ASSERT_EQ(status, Status::OK()) << status.to_string();

    std::vector<LogEntry> expected_entries{
        initial_entries[0],
        leader_entries[0],
        leader_entries[1],
    };
    ASSERT_EQ(actual_entries, expected_entries);
}

TEST_F(RaftNodeTest, BecomeLeaderFaultsWhenNoopWalAppendFails) {
    PersistEngine persist = make_engine();
    Status status = persist.init();
    ASSERT_EQ(status, Status::OK());
    status = persist.close();
    ASSERT_EQ(status, Status::OK());

    RaftNode node{replica_id_, members_, &persist};
    for (int i = 1; i <= 30; i++) {
        node.tick();
    }

    ASSERT_TRUE(node.is_faulted());
    ASSERT_EQ(node.role(), ReplicaRole::FOLLOWER);
    ASSERT_TRUE(node.extract_messages().empty());
}

TEST_F(RaftNodeTest, InstallLocalSnapshotDoesNotSaveRaftMeta) {
    PersistEngine persist = make_engine();
    Status status = persist.init();
    ASSERT_EQ(status, Status::OK());

    RaftNode node{replica_id_, members_, &persist};
    node.update_raft_meta(RaftMeta{5, std::nullopt});

    std::error_code ec;
    ASSERT_GT(fs::remove_all(persist_dir(), ec), 0U);
    ASSERT_FALSE(ec) << ec.message();

    node.install_local_snapshot(10, 5);

    ASSERT_FALSE(node.is_faulted());
    ASSERT_EQ(node.snapshot_index(), 10);
    ASSERT_EQ(node.commit_index(), 10);
}

// 验证关于install leader snapshot的流程都走过了一遍，save raft meta也没有问题
TEST_F(RaftNodeTest, InstallLeaderSnapshotStepsDownAndPersistsHigherTerm) {
    PersistEngine persist = make_engine();
    Status status = persist.init();
    ASSERT_EQ(status, Status::OK());

    RaftNode node{replica_id_, members_, &persist};
    node.update_raft_meta(RaftMeta{3, replica_id_});

    node.install_leader_snapshot(10, 5, 4);

    ASSERT_FALSE(node.is_faulted());
    ASSERT_EQ(node.role(), ReplicaRole::FOLLOWER);
    ASSERT_EQ(node.current_term(), 4);
    ASSERT_EQ(node.snapshot_index(), 10);
    ASSERT_EQ(node.commit_index(), 10);

    RaftMeta meta;
    status = persist.load_raft_meta(meta);
    ASSERT_EQ(status, Status::OK());
    ASSERT_EQ(meta, (RaftMeta{4, std::nullopt}));
}

}  // namespace adviskv::storage