#pragma once

#include <cstddef>
#include <optional>
#include <unordered_map>
#include <vector>

#include "common/type.h"
#include "storage/model/param.h"
#include "storage/raft/raft_membership.h"

namespace adviskv::storage {

// 负责记录peer们的next_index和match_index都到哪里了
class RaftPeerProgress {
   public:
    RaftPeerProgress(ReplicaID replica_id, const RaftMembership& membership);

    LogIndex get_next_index(ReplicaID replica_id) const;
    void update_next_index(ReplicaID replica_id, LogIndex log_index);
    LogIndex get_match_index(ReplicaID replica_id) const;
    void update_match_index(ReplicaID replica_id, LogIndex log_index);

    void reset_for_leader(const RaftMembership& membership,
                          LogIndex last_log_index);
    void update_snapshot_watermark(ReplicaID replica_id,
                                   LogIndex snapshot_watermark);
    LogIndex get_snapshot_watermark(ReplicaID replica_id) const;
    LogIndex get_inflight_snapshot_index(ReplicaID replica_id) const;
    bool mark_snapshot_inflight(ReplicaID replica_id, LogIndex snapshot_index);
    void clear_snapshot_inflight(ReplicaID replica_id, LogIndex snapshot_index);
    void handle_append_ok(ReplicaID replica_id, LogIndex prev_log_index,
                          size_t entries_size);
    void handle_append_failed(ReplicaID replica_id,
                              LogIndex follower_last_log_index,
                              LogIndex leader_last_log_index);
    bool match_index_at_least(ReplicaID replica_id, LogIndex log_index) const;

   private:
    ReplicaID self_id_;
    std::unordered_map<ReplicaID, LogIndex, ReplicaIDHash> next_index_;
    std::unordered_map<ReplicaID, LogIndex, ReplicaIDHash> match_index_;
    std::unordered_map<ReplicaID, LogIndex, ReplicaIDHash> snapshot_watermark_;
    std::unordered_map<ReplicaID, LogIndex, ReplicaIDHash>
        inflight_snapshot_index_;
};

}  // namespace adviskv::storage