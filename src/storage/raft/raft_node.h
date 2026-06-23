#pragma once

#include <mutex>
#include <utility>
#include <vector>

#include "common/status.h"
#include "common/type.h"
#include "storage/model/param.h"
#include "storage/raft/core/raft_core.h"

namespace adviskv::storage {

class RaftNode {
   public:
    RaftNode(const ReplicaID& self_id, const std::vector<PeerMember>& members);

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

    // 查询状态
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

   private:
    RaftCore core_;
    mutable std::mutex mutex_;

    //============================================================
    // 测试用的入口
    //============================================================


    const std::vector<LogEntry>& log_entries_for_test() const {
        std::lock_guard lock(mutex_);
        return core_.log_entries_for_test();
    }

    std::pair<LogIndex, Term> snapshot_for_test() const {
        std::lock_guard lock(mutex_);
        return core_.snapshot_for_test();
    }

    void set_next_index_for_test(ReplicaID target, LogIndex index) {
        std::lock_guard lock(mutex_);
        core_.set_next_index_for_test(target, index);
    }

    void become_candidate_for_test(RaftEffects& effects) {
        std::lock_guard lock(mutex_);
        effects = RaftEffects{};
        core_.become_candidate_for_test(effects);
    }

    void reset_heartbeat_tick_for_test(int32 val) {
        std::lock_guard lock(mutex_);
        core_.reset_heartbeat_tick_for_test(val);
    }

    LogIndex next_index_for_test(const ReplicaID& target) const {
        std::lock_guard lock(mutex_);
        return core_.next_index_for_test(target);
    }

    LogIndex snapshot_watermark_for_test(const ReplicaID& target) const {
        std::lock_guard lock(mutex_);
        return core_.snapshot_watermark_for_test(target);
    }

    LogIndex inflight_snapshot_index_for_test(const ReplicaID& target) const {
        std::lock_guard lock(mutex_);
        return core_.inflight_snapshot_index_for_test(target);
    }

    friend class RaftClusterTest;
};

}  // namespace adviskv::storage