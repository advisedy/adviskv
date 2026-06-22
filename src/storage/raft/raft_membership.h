#pragma once

#include <optional>
#include <vector>

#include "common/type.h"
#include "storage/model/param.h"

namespace adviskv::storage {

class RaftMembership {
   public:
    RaftMembership(ReplicaID replica_id,
                   const std::vector<PeerMember>& members);

    const std::vector<PeerMember>& get_members() const;
    const std::vector<PeerMember>& all_members() const;
    std::vector<PeerMember> voters() const;
    const PeerMember* find_member(const ReplicaID& replica_id) const;
    bool contains(const ReplicaID& replica_id) const;
    bool is_voter(const ReplicaID& replica_id) const;
    void update_members(const std::vector<PeerMember>& members);
    int quorum_size() const;
    bool has_quorum(int ack_count) const;

   private:
    ReplicaID self_id_;
    std::vector<PeerMember> members_;
};

}  // namespace adviskv::storage
