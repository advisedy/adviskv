#pragma once

#include <cassert>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/define.h"
#include "common/model/storage_replica_status.h"
#include "common/status.h"
#include "common/tick_trigger.h"
#include "common/type.h"
#include "storage/model/param.h"
#include "storage/raft/raft_apply.h"
#include "storage/raft/raft_election.h"
#include "storage/raft/raft_log.h"
#include "storage/raft/raft_membership.h"
#include "storage/raft/raft_replication.h"
namespace adviskv::storage {

class RaftNode {
   public:
    RaftNode(const ReplicaID& self_id, const std::vector<PeerMember>& members);

    void tick(RaftEffects& effects);

    // 处理外层的写请求
    // 这里返回值第一个是Status，
    // 第二个是commit之后，新的commit_index应该对应是多少
    std::pair<Status, LogIndex> propose(WriteOpType op, const Key& key,
                                        const Value& value,
                                        RaftEffects& effects);

    // 处理来自storage_service_impl的RPC的请求
    void handle_request_vote(const RequestVoteParam& param,
                             RequestVoteResult& result, RaftEffects& effects);
                             
    void handle_append_entries(const AppendEntriesParam& param,
                               AppendEntriesResult& result,
                               RaftEffects& effects);

    // Replica 发送 RaftEffects 中的消息后，把 response 再交回 RaftNode。
    void handle_vote_response(const ReplicaID& from,
                              const RequestVoteResult& result,
                              RaftEffects& effects);

    Status handle_append_response(const ReplicaID& from,
                                  const AppendEntriesParam& sent_param,
                                  const AppendEntriesResult& result,
                                  RaftEffects& effects);

    std::vector<LogEntry>
    extract_committed_entries();  // 提取那些已提交但是还未 apply 的日志

    ReplicaRole role() const;

    Term current_term() const;

    LogIndex commit_index() const;

    LogIndex last_applied() const;

    LogIndex last_log_index() const;

    Term last_log_term() const;

    LogIndex snapshot_index() const;

    Term snapshot_term() const;

    int quorum_size() const;

    bool is_leader() const;

    // 外部更新 last_applied（apply 完成后调用）
    void advance_last_applied(LogIndex applied);

    // 外部用来执行完快照直接要截断log
    Status truncate_log(LogIndex index);

    // Snapshot 支持
    void install_local_snapshot(LogIndex snapshot_index, Term snapshot_term);
    Status install_leader_snapshot(LogIndex snapshot_index, Term snapshot_term,
                                   Term leader_term, RaftEffects& effects);
    Status build_install_snapshot_plan(Term leader_term, LogIndex snapshot_index,
                                       Term snapshot_term,
                                       SnapshotInstallPlan& plan,
                                       RaftEffects& effects);
    void commit_install_snapshot(const SnapshotInstallPlan& plan,
                                 RaftEffects& effects);

    // InstallSnapshot 回调
    void handle_install_snapshot_response(
        const ReplicaID& from, const InstallSnapshotParam& sent_param,
        const InstallSnapshotResult& result, RaftEffects& effects);
    void handle_install_snapshot_send_failed(
        const ReplicaID& from, const InstallSnapshotParam& sent_param,
        const Status& status);

    Status prepare_install_snapshot(Term leader_term, LogIndex snapshot_index,
                                    Term snapshot_term, RaftEffects& effects);

    // revocer 的时候更新用的
    void update_raft_meta(const RaftMeta& meta);

    void update_log_entries(const std::vector<LogEntry>& entries);

    void enter_recovering();

    bool is_recovering() const {
        std::lock_guard lock(mutex_);
        return state_ == RaftNodeState::RECOVERING;
    }

    bool is_ready() const {
        std::lock_guard lock(mutex_);
        return state_ == RaftNodeState::READY;
    }

    // 读一致性准备心跳
    Status build_append_entries_for_read(RaftEffects& effects,
                                         LogIndex& read_index, Term& read_term);

   private:
    RaftMeta get_rafe_meta() const;
    LogIndex last_log_index_unlocked() const;
    Term last_log_term_unlocked() const;
    LogIndex snapshot_index_unlocked() const;
    Term snapshot_term_unlocked() const;

    Status ensure_ready_unlocked() const;

    int quorum_size_unlocked() const;
    bool has_quorum_unlocked(int ack_count) const;

    void become_follower(Term later_term, RaftEffects& effects);
    void become_leader(RaftEffects& effects);
    void become_candidate(RaftEffects& effects);

    LogIndex append_new_entry_unlocked(WriteOpType op, const Key& key,
                                       const Value& value,
                                       RaftEffects& effects);
    void try_update_commit_index();
    bool later_than_other(Term other_term, LogIndex other_index) const;
    RaftLog::InstallSnapshotResult install_snapshot_unlocked(
        LogIndex snapshot_index, Term snapshot_term);
    void commit_install_snapshot_unlocked(const SnapshotInstallPlan& plan,
                                          RaftEffects& effects);
    Status prepare_install_snapshot_unlocked(Term leader_term,
                                             LogIndex snapshot_index,
                                             Term snapshot_term,
                                             RaftEffects& effects);
    void finish_recovering_unlocked();
    bool has_committed_current_term_entry_unlocked() const;
    void record_hard_state_unlocked(RaftEffects& effects) const;

    // RaftNode 只生成消息；Replica 负责把 RaftEffects.messages 发送出去。
    void send_request_vote_to(const PeerMember& member, RaftEffects& effects);
    void broadcast_append_entries(RaftEffects& effects);

    ReplicaID self_id_;
    RaftElection election_;
    RaftLog raft_log_;
    RaftApply raft_apply_;
    RaftMembership membership_;
    RaftReplication replication_;

    TickTrigger election_tick_trigger_;
    TickTrigger heartbeat_tick_trigger_;

    mutable std::mutex mutex_;

    enum class RaftNodeState {
        READY,
        RECOVERING,
    };
    RaftNodeState state_{RaftNodeState::READY};

    //////////// TEST
    const std::vector<LogEntry>& log_entries_for_test() const {
        return raft_log_.entries();
    }

    std::pair<LogIndex, Term> snapshot_for_test() const {
        return {raft_log_.snapshot_index(), raft_log_.snapshot_term()};
    }

    void set_next_index_for_test(ReplicaID target, LogIndex index) {
        replication_.set_next_index_for_test(target, index);
    }

    friend class RaftClusterTest;
};

}  // namespace adviskv::storage