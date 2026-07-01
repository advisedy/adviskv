#pragma once

#include <cstdint>

namespace adviskv {

enum class RaftMemberType : int8_t {
    NON_MEMBER =
            0,  // 这个字段，在SDM那边是给LEARNER做缓冲的，算是一个短暂的窗口；在storage那边代表group里没有这个member
    VOTER = 1,
    LEARNER = 2,
};

inline bool decode_raft_member_type(int32_t value, RaftMemberType& out) {
    switch (value) {
        case 0:
            out = RaftMemberType::NON_MEMBER;
            return true;
        case 1:
            out = RaftMemberType::VOTER;
            return true;
        case 2:
            out = RaftMemberType::LEARNER;
            return true;
        default:
            return false;
    }
}

inline bool is_raft_voter(RaftMemberType type) {
    return type == RaftMemberType::VOTER;
}

}  // namespace adviskv