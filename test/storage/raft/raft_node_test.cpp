#include "storage/raft/core/raft_core.h"

#include <gtest/gtest.h>

#include "common/status.h"
#include "common/type.h"
#include "storage/model/param.h"
#include "storage/persist/persist_engine.h"
#include "test_env.h"

namespace adviskv::storage {
namespace fs = std::filesystem;

class RaftCoreTest : public ::testing::Test {
   protected:
    void SetUp() override {
        base_dir_ =
            adviskv::test::make_unique_test_dir("raft_core", sequence_++);
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

    InstallSnapshotParam make_install_snapshot_param(Term term,
                                                     LogIndex snapshot_index,
                                                     Term snapshot_term) const {
        InstallSnapshotParam param;
        param.from_replica_id = replica_id_;
        param.to_replica_id = replica_id_;
        param.term = term;
        param.snapshot_index = snapshot_index;
        param.snapshot_term = snapshot_term;
        return param;
    }

    static InstallSnapshotContext make_install_snapshot_context(
        LogIndex snapshot_index, Term snapshot_term,
        std::vector<RaftMember> snapshot_members = {}) {
        InstallSnapshotContext context;
        context.snapshot_index = snapshot_index;
        context.snapshot_term = snapshot_term;
        context.snapshot_members = std::move(snapshot_members);
        return context;
    }

    static Status drive_raft_effects(PersistEngine& persist,
                                     const RaftEffects& effects) {
        if (effects.entries_to_rewrite.has_value() &&
            !effects.entries_to_append.empty()) {
            return Status::INVALID_ARGUMENT(
                "cannot append and rewrite raft log in one effects batch");
        }
        if (effects.hard_state.has_value()) {
            RETURN_IF_INVALID_STATUS(
                persist.save_raft_meta(*effects.hard_state))
        }
        if (effects.entries_to_rewrite.has_value()) {
            RETURN_IF_INVALID_STATUS(
                persist.rewrite_wal(*effects.entries_to_rewrite))
        }
        if (!effects.entries_to_append.empty()) {
            RETURN_IF_INVALID_STATUS(
                persist.append_wal_batch(effects.entries_to_append))
        }
        return Status::OK();
    }

    static Status tick(PersistEngine& persist, RaftCore& core) {
        RaftEffects effects;
        core.tick(effects);
        return drive_raft_effects(persist, effects);
    }

    ReplicaID replica_id_{101, 7, 2};
    std::vector<PeerMember> members_{PeerMember{"", replica_id_, {}}};

   private:
    static inline int sequence_{0};

    fs::path base_dir_;
};

TEST_F(RaftCoreTest, test_1) {
    PersistEngine persist = make_engine();
    {
        Status status = persist.init();
        ASSERT_EQ(status, Status::OK());
    }
    RaftCore core{replica_id_, members_};
    for (int i = 1; i <= 30; i++) {
        Status status = tick(persist, core);
        ASSERT_EQ(status, Status::OK()) << status.to_string();
    }
    ASSERT_EQ(core.role(), ReplicaRole::LEADER);
}

TEST_F(RaftCoreTest, test_2) {
    PersistEngine persist = make_engine();
    {
        Status status = persist.init();
        ASSERT_EQ(status, Status::OK());
    }

    RaftCore core{replica_id_, members_};
    for (int i = 1; i <= 30; i++) {
        Status status = tick(persist, core);
        ASSERT_EQ(status, Status::OK()) << status.to_string();
    }
    ASSERT_EQ(core.role(), ReplicaRole::LEADER);
    RaftEffects effects;
    auto [status, new_index] =
        core.propose(ProposeParam::write(WriteOpType::PUT, "1", "1"), effects);
    ASSERT_EQ(status, Status::OK());
    status = drive_raft_effects(persist, effects);
    ASSERT_EQ(status, Status::OK()) << status.to_string();
    std::vector<LogEntry> entries;
    status = persist.read_wal_batch(entries);
    ASSERT_EQ(status, Status::OK());
    ASSERT_EQ((int)entries.size(), 2);
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

TEST_F(RaftCoreTest, RecoveringBlocksElectionVoteAndProposeUntilCatchUp) {
    ReplicaID leader_id{101, 7, 1};
    std::vector<PeerMember> members{
        PeerMember{"leader", leader_id, {}},
        PeerMember{"self", replica_id_, {}},
    };
    PersistEngine persist = make_engine();
    Status persist_status = persist.init();
    ASSERT_EQ(persist_status, Status::OK());

    RaftCore core{replica_id_, members};

    core.enter_recovering();
    ASSERT_TRUE(core.is_recovering());

    for (int i = 1; i <= 30; i++) {
        Status tick_status = tick(persist, core);
        ASSERT_EQ(tick_status, Status::OK()) << tick_status.to_string();
    }
    ASSERT_EQ(core.role(), ReplicaRole::FOLLOWER);

    RequestVoteResult vote_result;
    RaftEffects vote_effects;
    core.handle_request_vote(RequestVoteParam{leader_id, replica_id_, 1, 0, 0},
                             vote_result, vote_effects);
    Status vote_status = drive_raft_effects(persist, vote_effects);
    ASSERT_EQ(vote_status, Status::OK()) << vote_status.to_string();
    ASSERT_FALSE(vote_result.vote_granted);

    RaftEffects propose_effects;
    auto [status, new_index] = core.propose(
        ProposeParam::write(WriteOpType::PUT, "k", "v"), propose_effects);
    ASSERT_TRUE(status.fail());
    ASSERT_EQ(new_index, -1);

    AppendEntriesResult append_result;
    AppendEntriesParam append_param{
        leader_id, replica_id_, 1,
        {make_entry(1, 1, WriteOpType::PUT, "k1", "v1"),
         make_entry(1, 2, WriteOpType::PUT, "k2", "v2")},
        0, 0, 2};
    RaftEffects effects;
    core.handle_append_entries(append_param, append_result, effects);
    status = drive_raft_effects(persist, effects);
    ASSERT_EQ(status, Status::OK()) << status.to_string();

    ASSERT_TRUE(append_result.success);
    ASSERT_EQ(core.commit_index(), 2);
    ASSERT_FALSE(core.is_recovering());
}

TEST_F(RaftCoreTest, RecoveringFinishesWhenSnapshotCoversTarget) {
    PersistEngine persist = make_engine();
    Status status = persist.init();
    ASSERT_EQ(status, Status::OK());

    ReplicaID leader_id{101, 7, 1};
    std::vector<PeerMember> members{
        PeerMember{"leader", leader_id, {}},
        PeerMember{"self", replica_id_, {}},
    };
    RaftCore core{replica_id_, members};

    core.enter_recovering();
    ASSERT_TRUE(core.is_recovering());

    RaftEffects prepare_effects;
    status = core.prepare_install_snapshot(
        make_install_snapshot_param(2, 5, 2), prepare_effects);
    ASSERT_EQ(status, Status::OK()) << status.to_string();
    status = drive_raft_effects(persist, prepare_effects);
    ASSERT_EQ(status, Status::OK()) << status.to_string();

    RaftEffects effects;
    core.commit_install_snapshot(InstallSnapshotContext{5, 2, {}}, effects);
    status = drive_raft_effects(persist, effects);
    ASSERT_EQ(status, Status::OK()) << status.to_string();
    ASSERT_EQ(core.snapshot_index(), 5);
    ASSERT_EQ(core.commit_index(), 5);
    ASSERT_TRUE(effects.entries_to_rewrite.has_value());
    ASSERT_TRUE(effects.entries_to_rewrite->empty());
    ASSERT_FALSE(core.is_recovering());
}

TEST_F(RaftCoreTest,
       HandleAppendEntriesRewritesWalWhenLeaderOverwritesConflict) {
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

    ReplicaID leader_id{101, 7, 1};
    std::vector<PeerMember> members{
        PeerMember{"leader", leader_id, {}},
        PeerMember{"self", replica_id_, {}},
    };
    RaftCore core{replica_id_, members};
    core.update_raft_meta(RaftMeta{2, std::nullopt});
    core.update_log_entries(initial_entries);

    std::vector<LogEntry> leader_entries{
        make_entry(2, 2, WriteOpType::PUT, "k2", "new2"),
        make_entry(2, 3, WriteOpType::PUT, "k3", "new3"),
    };
    AppendEntriesResult result;
    RaftEffects effects;
    core.handle_append_entries(
        AppendEntriesParam{leader_id, replica_id_, 2, leader_entries, 1, 1, 3},
        result, effects);
    status = drive_raft_effects(persist, effects);
    ASSERT_EQ(status, Status::OK()) << status.to_string();

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

TEST_F(RaftCoreTest, RequestVoteFromNonVoterDoesNotBumpTerm) {
    ReplicaID leader_id{101, 7, 1};
    ReplicaID alien_id{101, 7, 9};
    std::vector<PeerMember> members{
        PeerMember{"leader", leader_id, {}},
        PeerMember{"self", replica_id_, {}},
    };
    RaftCore core{replica_id_, members};
    core.update_raft_meta(RaftMeta{3, std::nullopt});

    RequestVoteResult result;
    RaftEffects effects;
    core.handle_request_vote(RequestVoteParam{alien_id, replica_id_, 10, 0, 0},
                             result, effects);

    EXPECT_FALSE(result.vote_granted);
    EXPECT_EQ(result.term, 3);
    EXPECT_EQ(core.current_term(), 3);
    EXPECT_FALSE(effects.hard_state.has_value());
}

TEST_F(RaftCoreTest, AppendEntriesFromNonMemberDoesNotBumpTerm) {
    ReplicaID leader_id{101, 7, 1};
    ReplicaID alien_id{101, 7, 9};
    std::vector<PeerMember> members{
        PeerMember{"leader", leader_id, {}},
        PeerMember{"self", replica_id_, {}},
    };
    RaftCore core{replica_id_, members};
    core.update_raft_meta(RaftMeta{3, std::nullopt});

    AppendEntriesResult result;
    RaftEffects effects;
    core.handle_append_entries(
        AppendEntriesParam{alien_id, replica_id_, 10,
                           {make_entry(10, 1, WriteOpType::PUT, "k", "v")},
                           0, 0, 1},
        result, effects);

    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.term, 3);
    EXPECT_EQ(core.current_term(), 3);
    EXPECT_EQ(core.last_log_index(), 0);
    EXPECT_EQ(core.commit_index(), 0);
    EXPECT_FALSE(effects.hard_state.has_value());
    EXPECT_TRUE(effects.entries_to_append.empty());
    EXPECT_FALSE(effects.entries_to_rewrite.has_value());
}

TEST_F(RaftCoreTest, InstallSnapshotFromNonMemberDoesNotBumpTerm) {
    ReplicaID leader_id{101, 7, 1};
    ReplicaID alien_id{101, 7, 9};
    std::vector<PeerMember> members{
        PeerMember{"leader", leader_id, {}},
        PeerMember{"self", replica_id_, {}},
    };
    RaftCore core{replica_id_, members};
    core.update_raft_meta(RaftMeta{3, std::nullopt});

    InstallSnapshotParam param = make_install_snapshot_param(10, 8, 10);
    param.from_replica_id = alien_id;
    RaftEffects effects;
    Status status = core.prepare_install_snapshot(param, effects);

    EXPECT_EQ(status.code(), StatusCode::INVALID_ARGUMENT) << status.to_string();
    EXPECT_EQ(core.current_term(), 3);
    EXPECT_EQ(core.snapshot_index(), 0);
    EXPECT_EQ(core.commit_index(), 0);
    EXPECT_FALSE(effects.hard_state.has_value());
}

TEST_F(RaftCoreTest, JoiningReplicaAcceptsAppendEntriesFromKnownLeader) {
    ReplicaID leader_id{101, 7, 1};
    std::vector<PeerMember> initial_members{
        PeerMember{"leader", leader_id, {}},
    };
    RaftCore core{replica_id_, initial_members};
    core.update_raft_meta(RaftMeta{3, std::nullopt});

    AppendEntriesResult result;
    RaftEffects effects;
    core.handle_append_entries(
        AppendEntriesParam{leader_id, replica_id_, 3,
                           {make_entry(3, 1, WriteOpType::PUT, "k", "v")},
                           0, 0, 1},
        result, effects);

    EXPECT_TRUE(result.success);
    EXPECT_EQ(core.current_term(), 3);
    EXPECT_EQ(core.commit_index(), 1);
    EXPECT_EQ(effects.entries_to_append.size(), 1U);
}

TEST_F(RaftCoreTest, JoiningReplicaAcceptsSnapshotFromKnownLeader) {
    ReplicaID leader_id{101, 7, 1};
    std::vector<PeerMember> initial_members{
        PeerMember{"leader", leader_id, {}},
    };
    RaftCore core{replica_id_, initial_members};
    core.update_raft_meta(RaftMeta{3, std::nullopt});

    InstallSnapshotParam param = make_install_snapshot_param(3, 8, 3);
    param.from_replica_id = leader_id;
    RaftEffects effects;
    Status status = core.prepare_install_snapshot(param, effects);

    EXPECT_EQ(status, Status::OK()) << status.to_string();
    EXPECT_EQ(core.current_term(), 3);
}

TEST_F(RaftCoreTest,
       BecomeLeaderNoopWalAppendFailureIsReturnedByEffectsDriver) {
    PersistEngine persist = make_engine();
    Status status = persist.init();
    ASSERT_EQ(status, Status::OK());

    RaftCore core{replica_id_, members_};
    RaftEffects effects;
    for (int i = 1; i <= 30; i++) {
        core.tick(effects);
        if (!effects.entries_to_append.empty()) break;
    }
    ASSERT_EQ(core.role(), ReplicaRole::LEADER);
    ASSERT_EQ(effects.entries_to_append.size(), 1U);
    ASSERT_EQ(effects.entries_to_append[0].op_type, WriteOpType::NONE);
    ASSERT_TRUE(effects.messages.empty());

    status = persist.close();
    ASSERT_EQ(status, Status::OK());

    status = drive_raft_effects(persist, effects);
    ASSERT_TRUE(status.fail());
}

TEST_F(RaftCoreTest, InstallLocalSnapshotDoesNotSaveRaftMeta) {
    PersistEngine persist = make_engine();
    Status status = persist.init();
    ASSERT_EQ(status, Status::OK());

    RaftCore core{replica_id_, members_};
    core.update_raft_meta(RaftMeta{5, std::nullopt});

    std::error_code ec;
    ASSERT_GT(fs::remove_all(persist_dir(), ec), 0U);
    ASSERT_FALSE(ec) << ec.message();

    core.install_local_snapshot(InstallSnapshotContext{10, 5, {}});

    ASSERT_EQ(core.snapshot_index(), 10);
    ASSERT_EQ(core.commit_index(), 10);
}

TEST_F(RaftCoreTest, InstallLeaderSnapshotStepsDownAndPersistsHigherTerm) {
    PersistEngine persist = make_engine();
    Status status = persist.init();
    ASSERT_EQ(status, Status::OK());

    RaftCore core{replica_id_, members_};
    core.update_raft_meta(RaftMeta{3, replica_id_});

    RaftEffects prepare_effects;
    status = core.prepare_install_snapshot(
        make_install_snapshot_param(4, 10, 5), prepare_effects);
    ASSERT_EQ(status, Status::OK()) << status.to_string();
    status = drive_raft_effects(persist, prepare_effects);
    ASSERT_EQ(status, Status::OK()) << status.to_string();

    RaftEffects effects;
    core.commit_install_snapshot(InstallSnapshotContext{10, 5, {}}, effects);
    status = drive_raft_effects(persist, effects);
    ASSERT_EQ(status, Status::OK()) << status.to_string();
    ASSERT_EQ(core.role(), ReplicaRole::FOLLOWER);
    ASSERT_EQ(core.current_term(), 4);
    ASSERT_EQ(core.snapshot_index(), 10);
    ASSERT_EQ(core.commit_index(), 10);
    ASSERT_TRUE(effects.entries_to_rewrite.has_value());
    ASSERT_TRUE(effects.entries_to_rewrite->empty());

    RaftMeta meta;
    status = persist.load_raft_meta(meta);
    ASSERT_EQ(status, Status::OK());
    ASSERT_EQ(meta, (RaftMeta{4, std::nullopt}));
}

TEST_F(RaftCoreTest, PrepareInstallSnapshotRejectsCoveredLogBoundary) {
    ReplicaID leader_id{101, 7, 1};
    std::vector<PeerMember> members{
        PeerMember{"leader", leader_id, {}},
        PeerMember{"self", replica_id_, {}},
    };
    RaftCore core{replica_id_, members};
    core.update_log_entries({
        make_entry(1, 1, WriteOpType::PUT, "k1", "v1"),
        make_entry(1, 2, WriteOpType::PUT, "k2", "v2"),
        make_entry(2, 3, WriteOpType::PUT, "k3", "v3"),
    });

    RaftEffects effects;
    InstallSnapshotParam param = make_install_snapshot_param(3, 2, 1);
    param.from_replica_id = leader_id;
    Status status = core.prepare_install_snapshot(param, effects);

    ASSERT_EQ(status.code(), StatusCode::ALREADY_EXIST) << status.to_string();
    ASSERT_EQ(core.current_term(), 3);
    ASSERT_EQ(core.snapshot_index(), 0);
    ASSERT_EQ(core.last_log_index(), 3);
    ASSERT_TRUE(effects.hard_state.has_value());
}

TEST_F(RaftCoreTest, PrepareInstallSnapshotRejectsCommittedSnapshot) {
    ReplicaID leader_id{101, 7, 1};
    std::vector<PeerMember> members{
        PeerMember{"leader", leader_id, {}},
        PeerMember{"self", replica_id_, {}},
    };
    RaftCore core{replica_id_, members};

    AppendEntriesResult append_result;
    RaftEffects append_effects;
    core.handle_append_entries(
        AppendEntriesParam{
            leader_id, replica_id_, 2,
            {
                make_entry(2, 1, WriteOpType::PUT, "k1", "v1"),
                make_entry(2, 2, WriteOpType::PUT, "k2", "v2"),
                make_entry(2, 3, WriteOpType::PUT, "k3", "v3"),
            },
            0, 0, 3,
        },
        append_result, append_effects);
    ASSERT_TRUE(append_result.success);
    ASSERT_EQ(core.commit_index(), 3);

    RaftEffects effects;
    InstallSnapshotParam param = make_install_snapshot_param(2, 2, 2);
    param.from_replica_id = leader_id;
    Status status = core.prepare_install_snapshot(param, effects);

    ASSERT_EQ(status.code(), StatusCode::ALREADY_EXIST) << status.to_string();
    ASSERT_EQ(core.snapshot_index(), 0);
    ASSERT_EQ(core.last_log_index(), 3);
}

}  // namespace adviskv::storage
