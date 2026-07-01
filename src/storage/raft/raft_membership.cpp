#include "storage/raft/raft_membership.h"

#include <algorithm>

#include <fmt/format.h>

#include "common/func.h"
#include "common/model/raft_member_type.h"
#include "common/status.h"
#include "storage/model/param.h"

namespace adviskv::storage {

RaftMembership::RaftMembership(const std::vector<PeerMember>& voters) {
    reset_voters(voters);
}

std::vector<PeerMember> RaftMembership::peer_members() const {
    std::vector<PeerMember> peers;
    peers.reserve(members_.size());
    for (const RaftMember& member : members_) {
        peers.push_back(member.peer);
    }
    return peers;
}

std::vector<PeerMember> RaftMembership::voters() const {
    std::vector<PeerMember> peers;
    for (const RaftMember& member : members_) {
        if (member.member_type == RaftMemberType::VOTER) {
            peers.push_back(member.peer);
        }
    }
    return peers;
}

std::vector<PeerMember> RaftMembership::learners() const {
    std::vector<PeerMember> peers;
    for (const RaftMember& member : members_) {
        if (member.member_type == RaftMemberType::LEARNER) {
            peers.push_back(member.peer);
        }
    }
    return peers;
}

const std::vector<RaftMember>& RaftMembership::raft_members() const {
    return members_;
}

const RaftMember* RaftMembership::find_raft_member(const ReplicaID& replica_id) const {
    auto it = std::find_if(members_.begin(), members_.end(),
                           [&replica_id](const RaftMember& member) { return member.peer.replica_id == replica_id; });
    if (it == members_.end())
        return nullptr;
    return &(*it);
}

bool RaftMembership::contains(const ReplicaID& replica_id) const {
    return find_raft_member(replica_id) != nullptr;
}

bool RaftMembership::is_voter(const ReplicaID& replica_id) const {
    return member_type(replica_id) == RaftMemberType::VOTER;
}

RaftMemberType RaftMembership::member_type(const ReplicaID& replica_id) const {
    const RaftMember* member = find_raft_member(replica_id);
    if (member == nullptr) {
        return RaftMemberType::NON_MEMBER;
    }
    return member->member_type;
}

void RaftMembership::reset_voters(const std::vector<PeerMember>& voters) {
    members_.clear();
    members_.reserve(voters.size());
    for (const PeerMember& voter : voters) {
        members_.push_back(RaftMember{voter, RaftMemberType::VOTER});
    }
}

void RaftMembership::update_raft_members(const std::vector<RaftMember>& members) {
    members_ = members;
}

Status RaftMembership::add_learner(const PeerMember& member) {
    if (const RaftMember* existing = find_raft_member(member.replica_id); existing != nullptr) {
        return Status::OK();
    }
    members_.push_back(RaftMember{member, RaftMemberType::LEARNER});
    return Status::OK();
}

Status RaftMembership::promote_voter(const ReplicaID& replica_id) {
    if (auto&& it =
                std::find_if(members_.begin(), members_.end(),
                             [&replica_id](const RaftMember& member) { return member.peer.replica_id == replica_id; });
        it != members_.end()) {
        it->member_type = RaftMemberType::VOTER;
        return Status::OK();
    }
    return Status::INVALID_ARGUMENT(fmt::format("member {} not found", replica_id.to_string()));
}

Status RaftMembership::remove_member(const ReplicaID& replica_id) {
    func::ad_erase_if(members_,
                      [&replica_id](const RaftMember& member) { return member.peer.replica_id == replica_id; });
    return Status::OK();
}

int RaftMembership::quorum_size() const {
    int voter_count = to<int>(voters().size());
    return voter_count / 2 + 1;
}

bool RaftMembership::has_quorum(int ack_count) const {
    return ack_count >= quorum_size();
}

}  // namespace adviskv::storage