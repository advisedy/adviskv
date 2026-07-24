#pragma once

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "common/model/type.h"
#include "common/status.h"
#include "common/tick_trigger.h"
#include "storage/model/param.h"
#include "storage/raft/raft_apply.h"
#include "storage/raft/raft_election.h"
#include "storage/raft/raft_log.h"
#include "storage/raft/raft_membership.h"
#include "storage/raft/raft_replication.h"

namespace adviskv::storage {

struct RaftCoreTimingConfig {
    int32 heartbeat_ticks{3};
    TickTrigger::TickLimitFunc next_election_timeout;
};

// 上锁的逻辑都交给了外部，需持有 raft_core_mutex_。
class RaftCore {
public:
    RaftCore(const ReplicaID& self_id, const std::vector<PeerMember>& members);
    RaftCore(const ReplicaID& self_id, const std::vector<PeerMember>& members, RaftCoreTimingConfig timing);

    void tick(RaftEffects& effects);

    // propose
    std::pair<Status, LogIndex> propose(const ProposeParam& param, RaftEffects& effects);
    std::vector<std::pair<Status, LogIndex>> propose_batch(const std::vector<ProposeParam>& params,
                                                           RaftEffects& effects);

    // 处理别的 replica 发过来的 Raft RPC 请求
    void handle_request_vote(const RequestVoteParam& param, RequestVoteResult& result, RaftEffects& effects);
    void handle_append_entries(const AppendEntriesParam& param, AppendEntriesResult& result, RaftEffects& effects);

    // 处理自己发出去的 Raft RPC 的 response
    void handle_vote_response(const ReplicaID& from, const RequestVoteResult& result, RaftEffects& effects);
    Status handle_append_response(const ReplicaID& from, const AppendEntriesParam& sent_param,
                                  const AppendEntriesResult& result, RaftEffects& effects);
    void handle_append_send_failed(const ReplicaID& from, const AppendEntriesParam& sent_param, const Status& status);

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
    RaftMemberType member_type(const ReplicaID& replica_id) const;
    std::vector<RaftMember> raft_members() const;

    bool is_recovering() const;
    bool is_ready() const;

    // apply 相关的推进
    std::vector<LogEntry> extract_committed_entries();
    void advance_last_applied(LogIndex applied);
    Status apply_config_entry(const LogEntry& entry);

    // 快照相关
    Status truncate_log(LogIndex index);
    void install_local_snapshot(const InstallSnapshotContext& context);
    void commit_install_snapshot(const InstallSnapshotContext& context, RaftEffects& effects);

    void handle_install_snapshot_response(const ReplicaID& from, const InstallSnapshotParam& sent_param,
                                          const InstallSnapshotResult& result, RaftEffects& effects);
    void handle_install_snapshot_send_failed(const ReplicaID& from, const InstallSnapshotParam& sent_param,
                                             const Status& status);

    Status prepare_install_snapshot(const InstallSnapshotParam& param, RaftEffects& effects);

    // recovery 的时候会用到的更新
    void update_raft_meta(const RaftMeta& meta);
    void update_log_entries(const std::vector<LogEntry>& entries);
    void update_membership(const std::vector<RaftMember>& members);
    void enter_recovering();

    // 成员变更
    Status ensure_add_learner(const PeerMember& member, RaftEffects& effects);
    Status ensure_remove_member(const ReplicaID& replica_id, RaftEffects& effects);

    // 读一致性准备心跳
    Status build_append_entries_for_read(RaftEffects& effects, LogIndex& read_index, Term& read_term);

    ///////////////////////////////////////
    // 测试用的入口
    const std::vector<LogEntry>& log_entries_for_test() const { return raft_log_.entries(); }

    LogIndex next_index_for_test(const ReplicaID& target) const { return replication_.next_index(target); }

    LogIndex snapshot_watermark_for_test(const ReplicaID& target) const {
        return replication_.snapshot_watermark(target);
    }

    LogIndex inflight_snapshot_index_for_test(const ReplicaID& target) const {
        return replication_.inflight_snapshot_index(target);
    }
    ///////////////////////////////////////
private:
    enum class State {
        READY,
        RECOVERING,
    };

    Status ensure_ready() const;
    bool later_than_other(Term other_term, LogIndex other_index) const;
    void record_hard_state(RaftEffects& effects) const;
    const LogEntry* first_unapplied_config_entry() const;
    bool has_unapplied_config_entry() const;
    void maybe_promote_ready_learner(RaftEffects& effects);

    // 角色
    void become_follower(Term later_term, RaftEffects& effects);
    void become_leader(RaftEffects& effects);
    void become_candidate(RaftEffects& effects);
    void send_request_vote_to(const PeerMember& member, RaftEffects& effects);
    void step_down_if_become_non_member();

    // 日志复制相关
    LogIndex append_new_entry(const ProposeParam& param, RaftEffects& effects);

    Status validate_proposal(const ProposeParam& param) const;
    void try_update_commit_index(RaftEffects& effects);
    void broadcast_append_entries(RaftEffects& effects);

    RaftLog::InstallSnapshotResult install_snapshot(const InstallSnapshotContext& context);

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
