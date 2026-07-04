#pragma once

#include "common/define.h"
#include "common/model/raft_member_type.h"
#include "common/type.h"

namespace adviskv {

struct RaftMember {
    PeerMember peer;
    RaftMemberType member_type{RaftMemberType::NON_MEMBER};

    bool operator==(const RaftMember& other) const {
        return peer == other.peer && member_type == other.member_type;
    }

    DEFINE_OPERATOR_NOT_EQUAL(RaftMember)
};

}  // namespace adviskv