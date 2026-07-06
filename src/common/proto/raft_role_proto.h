#pragma once

#include "common.pb.h"
#include "common/model/replica_role.h"

namespace adviskv {

inline bool decode_pb_raft_role(pb::RaftRole in, ReplicaRole& out) {
    switch (in) {
        case pb::RaftRole::RAFT_ROLE_FOLLOWER:
            out = ReplicaRole::FOLLOWER;
            return true;
        case pb::RaftRole::RAFT_ROLE_LEADER:
            out = ReplicaRole::LEADER;
            return true;
        case pb::RaftRole::RAFT_ROLE_CANDIDATE:
            out = ReplicaRole::CANDIDATE;
            return true;
        case pb::RaftRole::RAFT_ROLE_UNSPECIFIED:
        default:
            return false;
    }
}

inline bool encode_pb_raft_role(ReplicaRole in, pb::RaftRole& out) {
    switch (in) {
        case ReplicaRole::FOLLOWER:
            out = pb::RaftRole::RAFT_ROLE_FOLLOWER;
            return true;
        case ReplicaRole::LEADER:
            out = pb::RaftRole::RAFT_ROLE_LEADER;
            return true;
        case ReplicaRole::CANDIDATE:
            out = pb::RaftRole::RAFT_ROLE_CANDIDATE;
            return true;
        default:
            return false;
    }
}

}  // namespace adviskv
