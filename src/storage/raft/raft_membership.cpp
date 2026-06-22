#include "storage/raft/raft_membership.h"

#include <algorithm>

namespace adviskv::storage {

RaftMembership::RaftMembership(ReplicaID replica_id,
                               const std::vector<PeerMember>& members)
    : self_id_(replica_id), members_(members) {}

const std::vector<PeerMember>& RaftMembership::get_members() const {
    return members_;
}

const std::vector<PeerMember>& RaftMembership::all_members() const {
    return members_;
}

std::vector<PeerMember> RaftMembership::voters() const { return members_; }

const PeerMember* RaftMembership::find_member(
    const ReplicaID& replica_id) const {
    auto it = std::find_if(members_.begin(), members_.end(),
                           [&replica_id](const PeerMember& member) {
                               return member.replica_id == replica_id;
                           });
    if (it == members_.end()) return nullptr;
    return &(*it);
}

bool RaftMembership::contains(const ReplicaID& replica_id) const {
    return find_member(replica_id) != nullptr;
}

bool RaftMembership::is_voter(const ReplicaID& replica_id) const {
    return contains(replica_id);
}

void RaftMembership::update_members(const std::vector<PeerMember>& members) {
    members_ = members;
}

int RaftMembership::quorum_size() const {
    return static_cast<int>(members_.size()) / 2 + 1;
}

bool RaftMembership::has_quorum(int ack_count) const {
    return ack_count >= quorum_size();
}

}  // namespace adviskv::storage