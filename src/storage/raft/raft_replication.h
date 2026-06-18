#pragma once

#include <cstddef>
#include <optional>

#include "common/type.h"
#include "storage/model/param.h"
#include "storage/raft/raft_apply.h"
#include "storage/raft/raft_log.h"
#include "storage/raft/raft_membership.h"
#include "storage/raft/raft_peer_progress.h"

namespace adviskv::storage {

// 负责Raft的复制相关内容
class RaftReplication {
   public:

   // 这个类其实只是给外部的RaftNode提供打点用的
    struct CommitAdvanceResult {
        bool advanced{false};
        LogIndex old_commit_index{0};
        LogIndex new_commit_index{0};
    };

    RaftReplication(const ReplicaID& self_id,
                    const RaftMembership& membership, const RaftLog& raft_log,
                    RaftApply& raft_apply);

    void reset_for_leader();
    void broadcast_append_entries(Term current_term, RaftEffects& effects);

    bool is_stale_append_response(const ReplicaID& replica_id,
                                  const AppendEntriesParam& sent_param) const;
    void handle_append_ok(const ReplicaID& replica_id, LogIndex prev_log_index,
                          size_t entries_size);
    void handle_append_failed(const ReplicaID& replica_id,
                              LogIndex follower_last_log_index);
    CommitAdvanceResult try_advance_commit_index(Term current_term);

    void update_snapshot_watermark(const ReplicaID& replica_id,
                                   LogIndex snapshot_watermark);
    void clear_snapshot_inflight(const ReplicaID& replica_id,
                                 LogIndex snapshot_index);
    LogIndex next_index(const ReplicaID& replica_id) const;
    LogIndex snapshot_watermark(const ReplicaID& replica_id) const;
    LogIndex inflight_snapshot_index(const ReplicaID& replica_id) const;
    void set_next_index_for_test(ReplicaID replica_id, LogIndex index);

   private:
    RaftMessageOr build_append_entries_message(
        const PeerMember& member, LogIndex next_index, Term current_term);

    ReplicaID self_id_;
    const RaftMembership& membership_;
    const RaftLog& raft_log_;
    RaftApply& raft_apply_;
    RaftPeerProgress peer_progress_;
};

}  // namespace adviskv::storage