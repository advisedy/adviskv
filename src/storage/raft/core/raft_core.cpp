#include "storage/raft/core/raft_core.h"

#include <functional>

#include "common/func.h"

namespace adviskv::storage {

namespace {

constexpr int32 K_HEARTBEAT_INTERVAL = 3;

const TickTrigger::TickLimitFunc K_ELECTION_TIMEOUT_FUNC = std::bind(func::get_random_int32, 15, 30);

}  // namespace

RaftCore::RaftCore(const ReplicaID& self_id, const std::vector<PeerMember>& members)
        : self_id_(self_id),
          election_(self_id_),
          raft_log_(),
          raft_apply_(raft_log_),
          membership_(members),
          replication_(self_id_, membership_, raft_log_, raft_apply_),
          election_tick_trigger_(K_ELECTION_TIMEOUT_FUNC),
          heartbeat_tick_trigger_(K_HEARTBEAT_INTERVAL) {}

LogIndex RaftCore::last_log_index() const { return raft_log_.last_log_index(); }

Term RaftCore::last_log_term() const { return raft_log_.last_log_term(); }

int RaftCore::quorum_size() const { return membership_.quorum_size(); }

ReplicaRole RaftCore::role() const { return election_.role(); }

Term RaftCore::current_term() const { return election_.current_term(); }

LogIndex RaftCore::commit_index() const { return raft_apply_.commit_index(); }

LogIndex RaftCore::last_applied() const { return raft_apply_.last_applied(); }

LogIndex RaftCore::snapshot_index() const { return raft_log_.snapshot_index(); }

Term RaftCore::snapshot_term() const { return raft_log_.snapshot_term(); }

bool RaftCore::is_leader() const { return election_.is_leader(); }

RaftMemberType RaftCore::member_type(const ReplicaID& replica_id) const { return membership_.member_type(replica_id); }

std::vector<RaftMember> RaftCore::raft_members() const { return membership_.raft_members(); }

bool RaftCore::is_recovering() const { return state_ == State::RECOVERING; }

bool RaftCore::is_ready() const { return state_ == State::READY; }

bool RaftCore::later_than_other(Term other_term, LogIndex other_index) const {
    if (raft_log_.last_log_term() != other_term) {
        return raft_log_.last_log_term() > other_term;
    }
    return raft_log_.last_log_index() > other_index;
}

Status RaftCore::ensure_ready() const {
    if (state_ == State::RECOVERING) {
        return Status::IS_RECOVERING("raft node is recovering");
    }
    if (state_ != State::READY) {
        return Status::NOT_INIT("raft node is not ready");
    }
    return Status::OK();
}

void RaftCore::record_hard_state(RaftEffects& effects) const { effects.hard_state = election_.hard_state(); }

std::vector<LogEntry> RaftCore::extract_committed_entries() { return raft_apply_.extract_committed_entries(); }

void RaftCore::advance_last_applied(LogIndex applied) { raft_apply_.advance_last_applied(applied); }

void RaftCore::step_down_if_become_non_member() {
    if (membership_.member_type(self_id_) != RaftMemberType::NON_MEMBER) {
        return;
    }
    if (!election_.is_follower()) {
        election_.become_follower(election_.current_term());
    }
    // self 已经不在 raft membership 里
    // 了，不应继续参与心跳或选举节奏
    heartbeat_tick_trigger_.stop();
    election_tick_trigger_.stop();
}

Status RaftCore::apply_config_entry(const LogEntry& entry) {
    switch (entry.op_type) {
        case WriteOpType::ADD_LEARNER:
            RETURN_IF_INVALID_STATUS(membership_.add_learner(entry.config_member))
            break;
        case WriteOpType::PROMOTE_VOTER:
            RETURN_IF_INVALID_STATUS(membership_.promote_voter(entry.config_replica_id))
            break;
        case WriteOpType::REMOVE_MEMBER:
            RETURN_IF_INVALID_STATUS(membership_.remove_member(entry.config_replica_id))
            break;
        default:
            return Status::INVALID_ARGUMENT("not a config entry");
    }

    step_down_if_become_non_member();
    raft_apply_.advance_last_applied(entry.index);
    return Status::OK();
}

}  // namespace adviskv::storage