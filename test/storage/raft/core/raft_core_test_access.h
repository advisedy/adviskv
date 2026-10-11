#pragma once

#include <utility>
#include <vector>

#include "common/model/model.h"
#include "storage/raft/core/raft_core.h"

namespace adviskv::storage {

class RaftCoreTestPeer {
public:
    static const std::vector<LogEntry>& log_entries(const RaftCore& core) { return core.raft_log_.entries(); }

    static std::pair<LogIndex, Term> snapshot(const RaftCore& core) {
        return {core.raft_log_.snapshot_index(), core.raft_log_.snapshot_term()};
    }

    static void set_next_index(RaftCore& core, const ReplicaID& target, LogIndex index) {
        core.replication_.set_next_index_for_test(target, index);
    }

    static void become_candidate(RaftCore& core, RaftEffects& effects) { core.become_candidate(effects); }

    static void become_pre_candidate(RaftCore& core, RaftEffects& effects) { core.become_pre_candidate(effects); }

    static bool is_pre_candidate(const RaftCore& core) { return core.election_.is_pre_candidate(); }

    static Term pre_vote_term(const RaftCore& core) { return core.election_.pre_vote_term(); }

    static void reset_heartbeat_tick(RaftCore& core, int32 val) { core.heartbeat_tick_trigger_.reset(val); }

    static void reset_election_tick(RaftCore& core, int32 val) { core.election_tick_trigger_.reset(val); }

    static LogIndex next_index(const RaftCore& core, const ReplicaID& target) {
        return core.replication_.next_index(target);
    }

    static LogIndex snapshot_watermark(const RaftCore& core, const ReplicaID& target) {
        return core.replication_.snapshot_watermark(target);
    }

    static LogIndex inflight_snapshot_index(const RaftCore& core, const ReplicaID& target) {
        return core.replication_.inflight_snapshot_index(target);
    }
};

}  // namespace adviskv::storage
