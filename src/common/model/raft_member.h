#pragma once

#include <fmt/format.h>

#include "common/define.h"
#include "common/model/peer_member.h"
#include "common/model/raft_member_type.h"
#include "common/types.h"

namespace adviskv {

struct RaftMember {
    PeerMember peer;
    RaftMemberType member_type{RaftMemberType::NON_MEMBER};

    bool operator==(const RaftMember& other) const { return peer == other.peer && member_type == other.member_type; }

    DEFINE_OPERATOR_NOT_EQUAL(RaftMember)

    std::string to_string() const {
        return fmt::format("peer:[{}], member_type:{}", peer.to_string(), to<int32>(member_type));
    }
};

}  // namespace adviskv