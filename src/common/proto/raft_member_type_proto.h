#pragma once

#include "common.pb.h"
#include "common/model/raft_member_type.h"

namespace adviskv {

inline bool decode_pb_raft_member_type(pb::RaftMemberType in,
                                       RaftMemberType& out) {
    switch (in) {
        case pb::RaftMemberType::RAFT_MEMBER_TYPE_NON_MEMBER:
            out = RaftMemberType::NON_MEMBER;
            return true;
        case pb::RaftMemberType::RAFT_MEMBER_TYPE_VOTER:
            out = RaftMemberType::VOTER;
            return true;
        case pb::RaftMemberType::RAFT_MEMBER_TYPE_LEARNER:
            out = RaftMemberType::LEARNER;
            return true;
        default:
            return false;
    }
}

inline bool encode_pb_raft_member_type(RaftMemberType in,
                                       pb::RaftMemberType& out) {
    switch (in) {
        case RaftMemberType::NON_MEMBER:
            out = pb::RaftMemberType::RAFT_MEMBER_TYPE_NON_MEMBER;
            return true;
        case RaftMemberType::VOTER:
            out = pb::RaftMemberType::RAFT_MEMBER_TYPE_VOTER;
            return true;
        case RaftMemberType::LEARNER:
            out = pb::RaftMemberType::RAFT_MEMBER_TYPE_LEARNER;
            return true;
        default:
            return false;
    }
}

}  // namespace adviskv
