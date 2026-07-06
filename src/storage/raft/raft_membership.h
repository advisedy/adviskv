#pragma once

#include <vector>

#include "common/model/type.h"
#include "common/status.h"
#include "storage/model/model.h"

namespace adviskv::storage {

class RaftMembership {
public:
    // 初始化的时候，全当成voters去看待
    explicit RaftMembership(const std::vector<PeerMember>& voters);

    std::vector<PeerMember> peer_members() const;
    std::vector<PeerMember> voters() const;
    std::vector<PeerMember> learners() const;
    const std::vector<RaftMember>& raft_members() const;
    bool contains(const ReplicaID& replica_id) const;
    bool is_voter(const ReplicaID& replica_id) const;
    RaftMemberType member_type(const ReplicaID& replica_id) const;
    void update_raft_members(const std::vector<RaftMember>& members);
    Status add_learner(const PeerMember& member);
    Status promote_voter(const ReplicaID& replica_id);
    Status remove_member(const ReplicaID& replica_id);
    int quorum_size() const;
    bool has_quorum(int ack_count) const;

private:
    void reset_voters(const std::vector<PeerMember>& voters);
    const RaftMember* find_raft_member(const ReplicaID& replica_id) const;

    std::vector<RaftMember> members_;
};

}  // namespace adviskv::storage
