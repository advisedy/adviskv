#include "common/shard_range.h"

#include <gtest/gtest.h>

namespace adviskv {

TEST(ShardRangeTest, CoversEntireHashSpaceIncludingBoundaries) {
    for (int count : {1, 2, 3, 7, 16, 257}) {
        const auto ranges = make_shard_ranges(count);
        ASSERT_TRUE(check_shard_ranges_is_valid(count, ranges).ok());
        EXPECT_EQ(find_shard_range(ranges, 0), 0U);
        EXPECT_EQ(find_shard_range(ranges, K_HASH_BUCKET_COUNT - 1), ranges.size() - 1);
        for (size_t i = 0; i < ranges.size(); ++i) {
            EXPECT_EQ(find_shard_range(ranges, ranges[i].range.start), i);
            if (i > 0) {
                EXPECT_EQ(find_shard_range(ranges, ranges[i].range.start - 1), i - 1);
            }
        }
    }
    const auto thirds = make_shard_ranges(3);
    EXPECT_EQ(thirds[1].range.start, 21845U);
    EXPECT_EQ(thirds[2].range.start, 43690U);
}

TEST(ShardRangeTest, RejectsGapsOverlapsAndDuplicates) {
    const auto good = make_shard_ranges(3);
    auto invalid = good;
    invalid[1].range.start++;
    EXPECT_TRUE(check_shard_ranges_is_valid(3, invalid).fail());
    invalid = good;
    invalid[1].range.start--;
    EXPECT_TRUE(check_shard_ranges_is_valid(3, invalid).fail());
    invalid = good;
    invalid[1].shard_id = invalid[0].shard_id;
    EXPECT_TRUE(check_shard_ranges_is_valid(3, invalid).fail());
    invalid = good;
    invalid.back().range.end = K_HASH_BUCKET_COUNT - 1;
    EXPECT_TRUE(check_shard_ranges_is_valid(3, invalid).fail());
}

TEST(ShardRangeTest, SimilarKeysSpreadAcrossHashRanges) {
    EXPECT_EQ(hash_bucket_for_key(""), 0x2926U);
    EXPECT_EQ(hash_bucket_for_key("a"), 0xce5bU);
    EXPECT_EQ(hash_bucket_for_key("test-key"), 0x53efU);
    const auto ranges = make_shard_ranges(8);
    std::vector<int> counts(8);
    for (int i = 0; i < 10000; ++i) {
        ++counts[find_shard_range(ranges, hash_bucket_for_key("key-" + std::to_string(i)))];
    }
    for (int count : counts) {
        EXPECT_GT(count, 900);
        EXPECT_LT(count, 1600);
    }
    EXPECT_NE(hash_bucket_for_key(std::string_view("a\0b", 3)), hash_bucket_for_key("a"));
}

}  // namespace adviskv
