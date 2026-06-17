#include "storage/raft/raft_election.h"

#include <cassert>

namespace adviskv::storage {

RaftElection::RaftElection(const ReplicaID& self_id) : self_id_(self_id) {}

ReplicaRole RaftElection::role() const {
    return role_;
}

Term RaftElection::current_term() const {
    return current_term_;
}

const std::optional<ReplicaID>& RaftElection::voted_for() const {
    return voted_for_;
}

bool RaftElection::is_follower() const {
    return role_ == ReplicaRole::FOLLOWER;
}

bool RaftElection::is_candidate() const {
    return role_ == ReplicaRole::CANDIDATE;
}

bool RaftElection::is_leader() const {
    return role_ == ReplicaRole::LEADER;
}

int32_t RaftElection::granted_vote_count() const {
    return static_cast<int32_t>(granted_voters_.size());
}

RaftMeta RaftElection::hard_state() const {
    return RaftMeta{current_term_, voted_for_};
}

void RaftElection::update_meta(const RaftMeta& meta) {
    current_term_ = meta.current_term;
    voted_for_ = meta.voted_for;
}

void RaftElection::become_candidate() {
    role_ = ReplicaRole::CANDIDATE;
    current_term_++;
    voted_for_ = self_id_;
    granted_voters_.clear();
    granted_voters_.insert(self_id_);
}

// 返回值是true，代表自己的term发生了变化
bool RaftElection::become_follower(Term later_term) {
    assert(later_term >= current_term_);

    role_ = ReplicaRole::FOLLOWER;
    granted_voters_.clear();

    if (later_term == current_term_) {
        return false;
    }

    current_term_ = later_term;
    voted_for_.reset();
    return true;
}

void RaftElection::become_leader() {
    role_ = ReplicaRole::LEADER;
    granted_voters_.clear();
}

bool RaftElection::can_grant_vote_to(const ReplicaID& candidate) const {
    return !voted_for_.has_value() || voted_for_.value() == candidate;
}

bool RaftElection::grant_vote_to(const ReplicaID& candidate) {
    if (!can_grant_vote_to(candidate)) {
        return false;
    }
    voted_for_ = candidate;
    return true;
}

bool RaftElection::record_vote_granted_from(const ReplicaID& voter) {
    return granted_voters_.insert(voter).second;
}

}  // namespace adviskv::storage