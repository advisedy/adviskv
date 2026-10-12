#pragma once

#include <algorithm>
#include <unordered_set>
#include <vector>

#include "common/constants.h"
#include "common/define.h"
#include "common/stable_hash.h"
#include "common/status.h"
#include "common/types.h"

namespace adviskv {

struct HashRange {
    uint32 start{0};
    uint32 end{0};  // 设定是左闭右开

    bool contains(uint32 bucket) const { return bucket >= start && bucket < end; }
    bool operator==(const HashRange& rhs) const { return start == rhs.start && end == rhs.end; }
    DEFINE_OPERATOR_NOT_EQUAL(HashRange)
};

struct ShardRange {
    ShardIndex shard_id{-1};
    HashRange range;
};

inline uint32 hash_bucket_for_key(std::string_view key) {
    StableHash64 hash;
    hash.append(key);
    return static_cast<uint32>(hash.get_mixed_hash() % K_HASH_BUCKET_COUNT);
}

inline std::vector<ShardRange> make_shard_ranges(int32 count) {
    std::vector<ShardRange> ranges;
    if (count <= 0 || count > (int32)K_HASH_BUCKET_COUNT) return ranges;
    for (int32 i = 0; i < count; ++i) {
        uint32 l = (to<uint64>(K_HASH_BUCKET_COUNT) * i) / count;
        uint32 r = (to<uint64>(K_HASH_BUCKET_COUNT) * (i + 1)) / count;
        ranges.push_back({i, {l, r}});
    }
    return ranges;
}

/*
检查ranges是否合法，需要满足：
- count 合法
- shard_id 合法不重复
- [0, K_HASH_BUCKET_COUNT) 的范围是被ranges铺满的
*/
inline Status check_shard_ranges_is_valid(int32 count, const std::vector<ShardRange>& ranges) {
    if (count <= 0 || count > to<int32>(K_HASH_BUCKET_COUNT)) {
        return Status::INVALID_ARGUMENT("invalid shard count");
    }
    if (ranges.size() != (size_t)(count)) {
        return Status::INVALID_ARGUMENT("invalid hash range count");
    }
    std::unordered_set<ShardIndex> ids;
    uint32 expected_start = 0;
    for (size_t i = 0; i < ranges.size(); ++i) {
        auto& shard_range = ranges[i];
        if (shard_range.shard_id < 0) {
            return Status::INVALID_ARGUMENT("shard_range.shard_id < 0");
        }
        if (auto it = ids.find(shard_range.shard_id); it != ids.end()) {
            return Status::INVALID_ARGUMENT("shard_range.shard_id is not unqiue");
        }
        if (shard_range.range.start != expected_start || shard_range.range.end <= shard_range.range.start ||
            shard_range.range.end > K_HASH_BUCKET_COUNT ||
            (i + 1 == ranges.size() && shard_range.range.end != K_HASH_BUCKET_COUNT)) {
            return Status::INVALID_ARGUMENT("hash ranges is not valid");
        }
        ids.insert(shard_range.shard_id);
        expected_start = shard_range.range.end;
    }
    return Status::OK();
}

inline size_t find_shard_range(const std::vector<ShardRange>& ranges, uint32 bucket) {
    auto it = std::upper_bound(ranges.begin(), ranges.end(), bucket,
                               [](uint32 h, const ShardRange& shard_range) { return h < shard_range.range.start; });
    if (it == ranges.begin()) return ranges.size();
    --it;
    return it->range.contains(bucket) ? static_cast<size_t>(it - ranges.begin()) : ranges.size();
}

}  // namespace adviskv
