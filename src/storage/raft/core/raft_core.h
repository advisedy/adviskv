#pragma once

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

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

// RaftCore 是不带锁的 Raft 协议核心。
// 外面的 RaftNode 负责加锁，然后把具体逻辑委托到这里。
class RaftCore {
   public:
    RaftCore(const ReplicaID& self_id, const std::vector<PeerMember>& members);

    // tick 和外层写请求
    void tick(RaftEffects& effects);
    std::pair<Status, LogIndex> propose(const ProposeParam& param,
                                        RaftEffects& effects);

    // 处理别的 replica 发过来的 Raft RPC 请求
    void handle_request_vote(const RequestVoteParam& param,
                             RequestVoteResult& result, RaftEffects& effects);
    void handle_append_entries(const AppendEntriesParam& param,
                               AppendEntriesResult& result,
                               RaftEffects& effects);

    // 处理自己发出去的 Raft RPC 的 response
    void handle_vote_response(const ReplicaID& from,
                              const RequestVoteResult& result,
                              RaftEffects& effects);
    Status handle_append_response(const ReplicaID& from,
                                  const AppendEntriesParam& sent_param,
                                  const AppendEntriesResult& result,
                                  RaftEffects& effects);
    void handle_append_send_failed(const ReplicaID& from,
                                   const AppendEntriesParam& sent_param,
                                   const Status& status);

    // 查询当前 raft 的状态
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

    bool is_recovering() const;
    bool is_ready() const;

    // apply 相关的推进
    std::vector<LogEntry> extract_committed_entries();
    void advance_last_applied(LogIndex applied);

    // 快照相关
    Status truncate_log(LogIndex index);
    void install_local_snapshot(LogIndex snapshot_index, Term snapshot_term);
    Status build_install_snapshot_plan(const InstallSnapshotParam& param,
                                       SnapshotInstallPlan& plan,
                                       RaftEffects& effects);
    void commit_install_snapshot(const SnapshotInstallPlan& plan,
                                 RaftEffects& effects);

    void handle_install_snapshot_response(
        const ReplicaID& from, const InstallSnapshotParam& sent_param,
        const InstallSnapshotResult& result, RaftEffects& effects);
    void handle_install_snapshot_send_failed(
        const ReplicaID& from, const InstallSnapshotParam& sent_param,
        const Status& status);

    Status prepare_install_snapshot(const InstallSnapshotParam& param,
                                    RaftEffects& effects);

    // recovery 的时候会用到的更新
    void update_raft_meta(const RaftMeta& meta);
    void update_log_entries(const std::vector<LogEntry>& entries);
    void enter_recovering();

    // 读一致性准备心跳
    Status build_append_entries_for_read(RaftEffects& effects,
                                         LogIndex& read_index, Term& read_term);

    // 测试用的入口
    const std::vector<LogEntry>& log_entries_for_test() const {
        return raft_log_.entries();
    }

    std::pair<LogIndex, Term> snapshot_for_test() const {
        return {raft_log_.snapshot_index(), raft_log_.snapshot_term()};
    }

    void set_next_index_for_test(ReplicaID target, LogIndex index) {
        replication_.set_next_index_for_test(target, index);
    }

    void become_candidate_for_test(RaftEffects& effects) {
        become_candidate(effects);
    }

    void reset_heartbeat_tick_for_test(int32 val) {
        heartbeat_tick_trigger_.reset(val);
    }

    LogIndex next_index_for_test(const ReplicaID& target) const {
        return replication_.next_index(target);
    }

    LogIndex snapshot_watermark_for_test(const ReplicaID& target) const {
        return replication_.snapshot_watermark(target);
    }

    LogIndex inflight_snapshot_index_for_test(const ReplicaID& target) const {
        return replication_.inflight_snapshot_index(target);
    }

   private:
    enum class State {
        READY,
        RECOVERING,
    };

    // 基础状态工具函数
    Status ensure_ready() const;
    bool later_than_other(Term other_term, LogIndex other_index) const;
    void record_hard_state(RaftEffects& effects) const;

    // 角色切换和选举相关
    void become_follower(Term later_term, RaftEffects& effects);
    void become_leader(RaftEffects& effects);
    void become_candidate(RaftEffects& effects);
    void send_request_vote_to(const PeerMember& member, RaftEffects& effects);

    // 日志复制相关
    LogIndex append_new_entry(const ProposeParam& param, RaftEffects& effects);

    Status validate_proposal(const ProposeParam& param) const;
    void try_update_commit_index();
    void broadcast_append_entries(RaftEffects& effects);

    // 快照相关
    RaftLog::InstallSnapshotResult install_snapshot_unlocked(
        LogIndex snapshot_index, Term snapshot_term);

    // recovery 和 read index 内部工具
    void finish_recovering();
    bool has_committed_current_term_entry() const;

    ReplicaID self_id_;
    RaftElection election_;
    RaftLog raft_log_;
    RaftApply raft_apply_;
    RaftMembership membership_;
    RaftReplication replication_;

    TickTrigger election_tick_trigger_;
    TickTrigger heartbeat_tick_trigger_;

    State state_{State::READY};
};

}  // namespace adviskv::storage