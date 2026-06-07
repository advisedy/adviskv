#pragma once

#include <cstdint>

namespace adviskv {

enum class ReplicaRole : int8_t {
    FOLLOWER = 0,
    LEADER = 1,
    CANDIDATE = 2,
};

inline bool decode_replica_role(int32_t value, ReplicaRole& out) {
    switch (value) {
        case 0:
            out = ReplicaRole::FOLLOWER;
            return true;
        case 1:
            out = ReplicaRole::LEADER;
            return true;
        case 2:
            out = ReplicaRole::CANDIDATE;
            return true;
        default:
            return false;
    }
}

inline bool is_valid_replica_role(int32_t value) {
    ReplicaRole out = ReplicaRole::FOLLOWER;
    return decode_replica_role(value, out);
}
}  // namespace adviskv