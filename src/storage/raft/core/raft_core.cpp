#include "storage/raft/core/raft_core.h"

#include <functional>

#include "common/func.h"

namespace adviskv::storage {

namespace {

constexpr int32_t HEARTBEAT_INTERVAL = 3;

const TickTrigger::TickLimitFunc ELECTION_TIMEOUT_FUNC =
    std::bind(func::get_random_int32, 15, 30);

}  // namespace

RaftCore::RaftCore(const ReplicaID& self_id,
                   const std::vector<PeerMember>& members)
    : self_id_(self_id),
      election_(self_id_),
      raft_log_(),
      raft_apply_(raft_log_),
      membership_(self_id_, members),
      replication_(self_id_, membership_, raft_log_, raft_apply_),
      election_tick_trigger_(ELECTION_TIMEOUT_FUNC),
      heartbeat_tick_trigger_(HEARTBEAT_INTERVAL) {}

LogIndex RaftCore::last_log_index() const { return raft_log_.last_log_index(); }

Term RaftCore::last_log_term() const { return raft_log_.last_log_term(); }


int RaftCore::quorum_size() const { return membership_.quorum_size_unlocked(); }

ReplicaRole RaftCore::role() const { return election_.role(); }

Term RaftCore::current_term() const { return election_.current_term(); }

LogIndex RaftCore::commit_index() const { return raft_apply_.commit_index(); }

LogIndex RaftCore::last_applied() const { return raft_apply_.last_applied(); }

LogIndex RaftCore::snapshot_index() const { return raft_log_.snapshot_index(); }

Term RaftCore::snapshot_term() const { return raft_log_.snapshot_term(); }

bool RaftCore::is_leader() const { return election_.is_leader(); }

bool RaftCore::is_recovering() const { return state_ == State::RECOVERING; }

bool RaftCore::is_ready() const { return state_ == State::READY; }


bool RaftCore::later_than_other(Term other_term, LogIndex other_index) const {
    if (raft_log_.last_log_term() != other_term) {
        return raft_log_.last_log_term() > other_term;
    }
    return raft_log_.last_log_index() > other_index;
}

Status RaftCore::ensure_ready_unlocked() const {
    if (state_ == State::RECOVERING) {
        return Status::IS_RECOVERING("raft node is recovering");
    }
    if (state_ != State::READY) {
        return Status::NOT_INIT("raft node is not ready");
    }
    return Status::OK();
}

void RaftCore::record_hard_state_unlocked(RaftEffects& effects) const {
    effects.hard_state = election_.hard_state();
}

std::vector<LogEntry> RaftCore::extract_committed_entries() {
    return raft_apply_.extract_committed_entries();
}

void RaftCore::advance_last_applied(LogIndex applied) {
    raft_apply_.advance_last_applied(applied);
}

}  // namespace adviskv::storage
