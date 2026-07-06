#pragma once

#include <string_view>

#include "common/model/type.h"

namespace adviskv {

inline constexpr uint64 kFNV1a64OffsetBasis = 0xcbf29ce484222325ULL;
inline constexpr uint64 kFNV1a64Prime = 0x100000001b3ULL;

inline uint64 stable_hash_64(std::string_view data) {
    uint64 hash = kFNV1a64OffsetBasis;
    for (unsigned char ch : data) {
        hash ^= static_cast<uint64>(ch);
        hash *= kFNV1a64Prime;
    }
    return hash;
}

inline ShardIndex stable_shard_index(std::string_view key,
                                     int32_t shard_count) {
    if (shard_count <= 0) {
        return -1;
    }
    return static_cast<ShardIndex>(stable_hash_64(key) %
                                   static_cast<uint64>(shard_count));
}

}  // namespace adviskv