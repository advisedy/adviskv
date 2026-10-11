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

bool RaftCore::is_recovering() const { return recovering_; }

bool RaftCore::later_than_other(Term other_term, LogIndex other_index) const {
    if (raft_log_.last_log_term() != other_term) {
        return raft_log_.last_log_term() > other_term;
    }
    return raft_log_.last_log_index() > other_index;
}

bool RaftCore::is_recent_leader_active() const {
    return recent_leader_active_;
}

void RaftCore::clear_recent_leader_active() {
    recent_leader_active_ = false;
}

void RaftCore::mark_recent_leader_active() { recent_leader_active_ = true; }

Status RaftCore::ensure_ready() const {
    if (recovering_) {
        return Status::IS_RECOVERING("raft node is recovering");
    }
    return Status::OK();
}

Status RaftCore::ensure_writable_leader() const {
    RETURN_IF_INVALID_STATUS(ensure_ready())
    if (!election_.is_leader()) {
        return Status::NOT_LEADER("not leader");
    }
    if (!membership_.is_voter(self_id_)) {
        return Status::NOT_VOTER("self is not voter");
    }
    return Status::OK();
}

Status RaftCore::ensure_readable_leader() const {
    RETURN_IF_INVALID_STATUS(ensure_ready())
    if (!election_.is_leader()) return Status::NOT_LEADER("not leader");
    if (!has_committed_current_term_entry()) {
        return Status::READ_INDEX_NOT_READY("current term entry is not committed");
    }
    return Status::OK();
}

void RaftCore::record_hard_state(RaftEffects& effects) const { effects.hard_state = election_.hard_state(); }

std::vector<LogEntry> RaftCore::extract_committed_entries(size_t budget_entries, size_t budget_bytes,
                                                          bool* has_more) {
    bool unused_has_more = false;
    bool& has_more_ref = has_more != nullptr ? *has_more : unused_has_more;
    return raft_apply_.extract_committed_entries(budget_entries, budget_bytes, has_more_ref);
}

const LogEntry* RaftCore::find_request(const std::string& request_id) const {
    return raft_log_.find_request(request_id);
}

void RaftCore::advance_last_applied(LogIndex applied) { raft_apply_.advance_last_applied(applied); }

void RaftCore::step_down_if_become_non_member() {
    if (membership_.member_type(self_id_) != RaftMemberType::NON_MEMBER) {
        return;
    }
    clear_recent_leader_active();
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