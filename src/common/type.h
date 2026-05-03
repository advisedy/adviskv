#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <utility>

namespace adviskv {

using int64 = int64_t;
using int32 = int32_t;
using int8 = int8_t;
using uint64 = uint64_t;
using uint32 = uint32_t;
using uint8 = uint8_t;

using Key = std::string;
using Value = std::string;
using KeyView = std::string_view;
using ValueView = std::string_view;

using KV = std::pair<Key, Value>;

using DatabaseID = int32_t;
using TableID = int32_t;
using ShardIndex = int32_t;
using ReplicaIndex = int32_t;
using NodeID = std::string;

struct ShardID {
    TableID table_id{-1};
    ShardIndex shard_index{-1};

    bool operator==(const ShardID& other) const {
        return table_id == other.table_id && shard_index == other.shard_index;
    }
};

struct ShardIDHash {
    size_t operator()(const ShardID& key) const {
        size_t h1 = std::hash<TableID>{}(key.table_id);
        size_t h2 = std::hash<ShardIndex>{}(key.shard_index);
        return h1 ^ (h2 << 1);
    }
};

using ShardKey = ShardID;
using ShardKeyHash = ShardIDHash;

struct ReplicaID {
    TableID table_id{-1};
    ShardIndex shard_index{-1};
    ReplicaIndex replica_index{-1};

    bool operator==(const ReplicaID& other) const {
        return table_id == other.table_id && shard_index == other.shard_index &&
               replica_index == other.replica_index;
    }
};

struct ReplicaIDHash {
    size_t operator()(const ReplicaID& key) const {
        size_t h1 = std::hash<TableID>{}(key.table_id);
        size_t h2 = std::hash<ShardIndex>{}(key.shard_index);
        size_t h3 = std::hash<ReplicaIndex>{}(key.replica_index);
        return h1 ^ (h2 << 1) ^ (h3 << 2);
    }
};

enum class EngineType {
    MAP = 0,
    ROCKSDB = 1,
};

enum class ReplicaRole { FOLLOWER = 0, LEADER = 1, CANDIDATE = 2 };

struct Endpoint {
    std::string ip;
    int32_t port;
};



}  // namespace adviskv
