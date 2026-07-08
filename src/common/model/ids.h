#pragma once

#include <cstddef>
#include <functional>

#include <fmt/format.h>

#include "common/define.h"
#include "common/types.h"

namespace adviskv {

struct ShardID {
    TableID table_id{-1};
    ShardIndex shard_index{-1};

    ShardID() = default;
    ShardID(TableID table_id, ShardIndex shard_index) : table_id(table_id), shard_index(shard_index) {}

    std::string to_string() const { return fmt::format("{}:{}", table_id, shard_index); }

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
    ReplicaSeq replica_seq{-1};

    ReplicaID() = default;
    ReplicaID(TableID table_id, ShardIndex shard_index, ReplicaSeq replica_seq)
            : table_id(table_id), shard_index(shard_index), replica_seq(replica_seq) {}

    std::string to_string() const { return fmt::format("{}:{}:{}", table_id, shard_index, replica_seq); }

    ShardID get_shard_id() const { return ShardID{table_id, shard_index}; }

    bool operator==(const ReplicaID& other) const {
        return table_id == other.table_id && shard_index == other.shard_index && replica_seq == other.replica_seq;
    }

    DEFINE_OPERATOR_NOT_EQUAL(ReplicaID)
};

struct ReplicaIDHash {
    size_t operator()(const ReplicaID& key) const {
        size_t h1 = std::hash<TableID>{}(key.table_id);
        size_t h2 = std::hash<ShardIndex>{}(key.shard_index);
        size_t h3 = std::hash<ReplicaSeq>{}(key.replica_seq);
        return h1 ^ (h2 << 1) ^ (h3 << 2);
    }
};

}  // namespace adviskv