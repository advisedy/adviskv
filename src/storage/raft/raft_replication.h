#pragma once

#include <cstddef>
#include <optional>
#include <unordered_map>
#include <vector>

#include "common/model/model.h"
#include "storage/model/model.h"
#include "storage/raft/raft_apply.h"
#include "storage/raft/raft_log.h"
#include "storage/raft/raft_membership.h"

namespace adviskv::storage {

class RaftReplication {
public:
    struct CommitAdvanceResult {
        bool advanced{false};
        LogIndex old_commit_index{0};
        LogIndex new_commit_index{0};
    };

    RaftReplication(const ReplicaID& self_id, const RaftMembership& membership, const RaftLog& raft_log,
                    RaftApply& raft_apply);
    void reset_for_leader();
    void broadcast_append_entries(Term current_term, RaftEffects& effects);

    bool is_stale_append_response(const ReplicaID& replica_id, const AppendEntriesParam& sent_param) const;
    void handle_append_ok(const ReplicaID& replica_id, LogIndex prev_log_index, size_t entries_size);
    void handle_append_failed(const ReplicaID& replica_id, LogIndex follower_last_log_index);
    CommitAdvanceResult try_advance_commit_index(Term current_term);

    void update_snapshot_watermark(const ReplicaID& replica_id, LogIndex snapshot_watermark);
    void clear_snapshot_inflight(const ReplicaID& replica_id, LogIndex snapshot_index);
    LogIndex next_index(const ReplicaID& replica_id) const;
    LogIndex snapshot_watermark(const ReplicaID& replica_id) const;
    LogIndex inflight_snapshot_index(const ReplicaID& replica_id) const;
    bool match_index_at_least(const ReplicaID& replica_id, LogIndex log_index) const;
    void set_next_index_for_test(ReplicaID replica_id, LogIndex index);

private:
    RaftMessageOr build_append_entries_message(const PeerMember& member, LogIndex next_index, Term current_term);

    LogIndex get_next_index(ReplicaID replica_id) const;
    void update_next_index(ReplicaID replica_id, LogIndex log_index);
    LogIndex get_match_index(ReplicaID replica_id) const;
    void update_match_index(ReplicaID replica_id, LogIndex log_index);
    void reset_peer_progress_for_leader(const RaftMembership& membership, LogIndex last_log_index);
    LogIndex get_snapshot_watermark(ReplicaID replica_id) const;
    LogIndex get_inflight_snapshot_index(ReplicaID replica_id) const;
    bool mark_snapshot_inflight(ReplicaID replica_id, LogIndex snapshot_index);
    void handle_peer_append_ok(ReplicaID replica_id, LogIndex prev_log_index, size_t entries_size);
    void handle_peer_append_failed(ReplicaID replica_id, LogIndex follower_last_log_index,
                                   LogIndex leader_last_log_index);

    ReplicaID self_id_;
    const RaftMembership& membership_;
    const RaftLog& raft_log_;
    RaftApply& raft_apply_;

    std::unordered_map<ReplicaID, LogIndex, ReplicaIDHash> next_index_;
    std::unordered_map<ReplicaID, LogIndex, ReplicaIDHash> match_index_;
    std::unordered_map<ReplicaID, LogIndex, ReplicaIDHash> snapshot_watermark_;
    std::unordered_map<ReplicaID, LogIndex, ReplicaIDHash> inflight_snapshot_index_;
};

}  // namespace adviskv::storage
