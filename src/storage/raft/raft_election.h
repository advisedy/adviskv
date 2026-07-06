#pragma once

#include <cstdint>
#include <optional>
#include <unordered_set>

#include "common/model/type.h"
#include "storage/model/model.h"

namespace adviskv::storage {

class RaftElection {
   public:
    explicit RaftElection(const ReplicaID& self_id);

    ReplicaRole role() const;
    Term current_term() const;
    const std::optional<ReplicaID>& voted_for() const;

    bool is_follower() const;
    bool is_candidate() const;
    bool is_leader() const;

    int32_t granted_vote_count() const;
    RaftMeta hard_state() const;

    void update_meta(const RaftMeta& meta);
    void become_candidate();
    bool become_follower(Term later_term);
    void become_leader();

    bool can_grant_vote_to(const ReplicaID& candidate) const;
    bool grant_vote_to(const ReplicaID& candidate);
    bool record_vote_granted_from(const ReplicaID& voter);

   private:
    ReplicaID self_id_;
    ReplicaRole role_{ReplicaRole::FOLLOWER};

    Term current_term_{0};
    std::optional<ReplicaID> voted_for_;

    std::unordered_set<ReplicaID, ReplicaIDHash> granted_voters_;
};

}  // namespace adviskv::storage
