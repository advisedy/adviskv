#pragma once

#include <string_view>

#include "common/model/type.h"

namespace adviskv {

inline constexpr uint64 K_FNV1A64_OFFSET_BASIS = 0xcbf29ce484222325ULL;
inline constexpr uint64 K_FNV1A64_PRIME = 0x100000001b3ULL;

class StableHash64 {
public:
    void append(std::string_view data) {
        for (unsigned char ch : data) {
            append_byte(ch);
        }
    }

    void append_uint64(uint64 value) {
        for (int shift = 0; shift < 64; shift += 8) {
            append_byte(static_cast<uint8>(value >> shift));
        }
    }

    uint64 get_hash() const { return hash_; }

    void clear() { hash_ = K_FNV1A64_OFFSET_BASIS; }

private:
    void append_byte(uint8 byte) {
        hash_ ^= static_cast<uint64>(byte);
        hash_ *= K_FNV1A64_PRIME;
    }

    uint64 hash_{K_FNV1A64_OFFSET_BASIS};
};

inline ShardIndex stable_shard_index(std::string_view key, int32_t shard_count) {
    if (shard_count <= 0) {
        return -1;
    }
    StableHash64 hash;
    hash.append(key);
    return static_cast<ShardIndex>(hash.get_hash() % static_cast<uint64>(shard_count));
}

}  // namespace adviskv
