#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/define.h"
#include "common/model/storage_replica_status.h"
#include "common/status.h"
#include "common/type.h"
#include "storage/model/param.h"
#include "storage/raft/raft_log.h"
namespace adviskv::storage {

// 这个就是一个计数触发器
// 手动在外面保证传进来的limit_cnt是非负吧。
class TickTrigger {
   public:
    explicit TickTrigger(int32_t limit_cnt) : limit_cnt_(limit_cnt) {}

    bool tick() {
        if (stop_flag_) return false;
        cur_cnt_++;
        if (cur_cnt_ >= limit_cnt_) {
            cur_cnt_ = 0;
            return true;
        }
        return false;
    }

    bool reset(int32_t limit_cnt) {
        stop_flag_ = false;
        cur_cnt_ = 0;
        limit_cnt_ = limit_cnt;
        if (cur_cnt_ >= limit_cnt_) {
            cur_cnt_ = 0;
            return true;
        }
        return false;
    }

    void clear() { cur_cnt_ = 0; }

    void stop() { stop_flag_ = true; }

   private:
    bool stop_flag_{false};
    int32_t cur_cnt_{0};
    int32_t limit_cnt_;
};

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

    bool is_leader() const;

    // 外部更新 last_applied（apply 完成后调用）
    void advance_last_applied(LogIndex applied);

    // 外部用来执行完快照直接要截断log
    Status truncate_log(LogIndex index);

    // Snapshot 支持
    void install_local_snapshot(LogIndex snapshot_index, Term snapshot_term);
    Status install_leader_snapshot(LogIndex snapshot_index, Term snapshot_term,
                                   Term leader_term, RaftEffects& effects);

    // InstallSnapshot 回调
    void handle_install_snapshot_response(const ReplicaID& from,
                                          const InstallSnapshotResult& result,
                                          RaftEffects& effects);

    Status prepare_install_snapshot(Term leader_term, LogIndex snapshot_index,
                                    RaftEffects& effects);

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
    void install_snapshot_unlocked(LogIndex snapshot_index, Term snapshot_term);
    void finish_recovering_unlocked();
    bool has_committed_current_term_entry_unlocked() const;
    void record_hard_state_unlocked(RaftEffects& effects) const;
    RaftMessage build_append_entries_message_unlocked(const PeerMember& member,
                                                      LogIndex next_index);

    // RaftNode 只生成消息；Replica 负责把 RaftEffects.messages 发送出去。
    void send_request_vote_to(const PeerMember& member, RaftEffects& effects);
    void send_append_entries_to(const PeerMember& member, LogIndex next_index,
                                RaftEffects& effects);
    void broadcast_append_entries(RaftEffects& effects);

    enum class RaftNodeState {
        READY,
        RECOVERING,
    };

    ReplicaID self_id_;
    ReplicaRole role_{ReplicaRole::FOLLOWER};
    Term current_term_{0};
    std::optional<ReplicaID> voted_for_;

    // 选举
    int32_t election_generation_{0};
    int32_t granted_vote_count_{0};

    RaftLog raft_log_;
    std::vector<PeerMember> members_;

    std::unordered_map<ReplicaID, LogIndex, ReplicaIDHash> next_index_;
    std::unordered_map<ReplicaID, LogIndex, ReplicaIDHash> match_index_;

    TickTrigger election_tick_trigger_;
    TickTrigger heartbeat_tick_trigger_;

    mutable std::mutex mutex_;

    RaftNodeState state_{RaftNodeState::READY};

    //////////// TEST
    const std::vector<LogEntry>& log_entries_for_test() const {
        return raft_log_.entries();
    }

    std::pair<LogIndex, Term> snapshot_for_test() const {
        return {raft_log_.snapshot_index(), raft_log_.snapshot_term()};
    }

    friend class RaftClusterTest;
};

}  // namespace adviskv::storage
