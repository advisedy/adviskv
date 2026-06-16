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
    int quorum_size_unlocked() const;
    bool has_quorum_unlocked(int ack_count) const;

   private:
    ReplicaID self_id_;
    std::vector<PeerMember> members_;
};

}  // namespace adviskv::storage
