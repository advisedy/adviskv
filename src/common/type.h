#pragma once

#include <fmt/format.h>

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <utility>

#include "common/define.h"

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

using Term = int64;

struct ShardID {
    TableID table_id{-1};
    ShardIndex shard_index{-1};

    std::string to_string() const {
        return fmt::format("{}:{}", table_id, shard_index);
    }
    bool operator==(const ShardID& other) const {
        return table_id == other.table_id && shard_index == other.shard_index;
    }

    DEFINE_OPERATOR_NOT_EQUAL(ShardID)
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

    std::string to_string() const {
        return fmt::format("{}:{}:{}", table_id, shard_index, replica_index);
    }

    bool operator==(const ReplicaID& other) const {
        return table_id == other.table_id && shard_index == other.shard_index &&
               replica_index == other.replica_index;
    }

    DEFINE_OPERATOR_NOT_EQUAL(ReplicaID)
};

struct ReplicaIDHash {
    size_t operator()(const ReplicaID& key) const {
        size_t h1 = std::hash<TableID>{}(key.table_id);
        size_t h2 = std::hash<ShardIndex>{}(key.shard_index);
        size_t h3 = std::hash<ReplicaIndex>{}(key.replica_index);
        return h1 ^ (h2 << 1) ^ (h3 << 2);
    }
};

enum class EngineType : int8 {
    MAP = 0,
    ROCKSDB = 1,
};

enum class ReplicaRole : int8 { FOLLOWER = 0, LEADER = 1, CANDIDATE = 2 };

enum class ReplicaStatus : int8 { ADDING = 0, READY = 1, LOST = 2, ERROR = 3 };

struct Endpoint {
    std::string ip;
    int32 port;

    std::string to_string() const { return fmt::format("{}:{}", ip, port); }

    bool operator==(const Endpoint& other) const {
        return ip == other.ip and port == other.port;
    }

    DEFINE_OPERATOR_NOT_EQUAL(Endpoint)
};

struct PeerMember {
    NodeID node_id;
    ReplicaID replica_id;
    Endpoint endpoint;

    bool operator==(const PeerMember& other) const {
        if (node_id != other.node_id) return false;
        if (!(replica_id == other.replica_id)) return false;
        if (!(endpoint == other.endpoint)) return false;
        return true;
    }

    DEFINE_OPERATOR_NOT_EQUAL(PeerMember)
};

}  // namespace adviskv
