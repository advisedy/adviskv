#include "storage/raft/raft_membership.h"

namespace adviskv::storage {

RaftMembership::RaftMembership(ReplicaID replica_id,
                               const std::vector<PeerMember>& members)
    : self_id_(replica_id), members_(members) {}

const std::vector<PeerMember>& RaftMembership::get_members() const {
    return members_;
}

int RaftMembership::quorum_size_unlocked() const {
    return static_cast<int>(members_.size()) / 2 + 1;
}

bool RaftMembership::has_quorum_unlocked(int ack_count) const {
    return ack_count >= quorum_size_unlocked();
}

}  // namespace adviskv::storage