#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

#include "storage/engine/map_engine.h"

namespace adviskv::storage {
namespace {

// put写入后get应能读回对应值
TEST(MapEngineTest, PutAndGetExistingKey) {
    MapEngine engine;

    ASSERT_TRUE(engine.put("k1", "v1").ok());

    Value value;
    Status status = engine.get("k1", value);
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(value, "v1");
}

// get不存在的key应返回KEY_NOT_FOUND
TEST(MapEngineTest, GetMissingKeyReturnsKeyNotFound) {
    MapEngine engine;

    Value value;
    Status status = engine.get("missing", value);
    EXPECT_EQ(status.code(), StatusCode::KEY_NOT_FOUND);
}

// del删除已存在的key后，get应返回KEY_NOT_FOUND
TEST(MapEngineTest, DeleteRemovesExistingKey) {
    MapEngine engine;

    ASSERT_TRUE(engine.put("k1", "v1").ok());
    ASSERT_TRUE(engine.del("k1").ok());

    Value value;
    Status status = engine.get("k1", value);
    EXPECT_EQ(status.code(), StatusCode::KEY_NOT_FOUND);
}

// dump_all应返回所有kv，clear应清空所有数据
TEST(MapEngineTest, DumpAllAndClearReflectCurrentState) {
    MapEngine engine;

    ASSERT_TRUE(engine.put("k1", "v1").ok());
    ASSERT_TRUE(engine.put("k2", "v2").ok());

    std::vector<KV> kvs = engine.dump_all();
    std::sort(kvs.begin(), kvs.end(),
              [](const KV& lhs, const KV& rhs) { return lhs.first < rhs.first; });

    ASSERT_EQ(kvs.size(), 2U);
    EXPECT_EQ(kvs[0], KV("k1", "v1"));
    EXPECT_EQ(kvs[1], KV("k2", "v2"));

    ASSERT_TRUE(engine.clear().ok());
    EXPECT_TRUE(engine.dump_all().empty());
}

}  // namespace
}  // namespace adviskv::storage
