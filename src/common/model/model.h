#pragma once

// 对于需要和protobuf转换的模型，都放在了这个文件里面。如果不需要转换的，会放在meta或者storage的model里面

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include "common/define.h"
#include "common/shard_range.h"
#include "common/status.h"
#include "common/types.h"

namespace adviskv {

struct Endpoint {
    std::string ip;
    int32_t port{0};

    Endpoint() = default;
    Endpoint(std::string ip, int32_t port) : ip(std::move(ip)), port(port) {}

    std::string to_string() const { return fmt::format("{}:{}", ip, port); }

    bool operator==(const Endpoint& other) const { return ip == other.ip and port == other.port; }

    DEFINE_OPERATOR_NOT_EQUAL(Endpoint)
};

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

enum class RaftMemberType : int32_t {
    NON_MEMBER = 0,  // Meta 侧给 LEARNER 做过渡窗口；Storage 侧表示 group 中没有这个 member。
    VOTER = 1,
    LEARNER = 2,

    MAX_VALUE = LEARNER,
};

inline bool is_raft_voter(RaftMemberType type) { return type == RaftMemberType::VOTER; }

enum class ReplicaRole : int32_t {
    FOLLOWER = 0,
    LEADER = 1,
    CANDIDATE = 2,
    PRE_CANDIDATE = 3,

    MAX_VALUE = PRE_CANDIDATE,
};

enum class StorageReplicaStatus : int32_t {
    INITIALIZING = 0,
    RECOVERING = 1,
    READY = 2,
    FAULTED = 3,

    MAX_VALUE = FAULTED,
};

enum class EngineType : int32_t {
    MAP = 0,

    MAX_VALUE = MAP,
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

    std::string to_string() const {
        return fmt::format("node_id:{}, replica_id:{}, endpoint:{}", node_id, replica_id.to_string(),
                           endpoint.to_string());
    }
};

struct RaftMember {
    PeerMember peer;
    RaftMemberType member_type{RaftMemberType::NON_MEMBER};

    bool operator==(const RaftMember& other) const { return peer == other.peer && member_type == other.member_type; }

    DEFINE_OPERATOR_NOT_EQUAL(RaftMember)

    std::string to_string() const {
        return fmt::format("peer:[{}], member_type:{}", peer.to_string(), to<int32>(member_type));
    }
};

enum class ExpectedReplicaType : int32_t {
    PRESENT = 0,
    ABSENT = 1,
    ADD_MEMBER = 2,
    REMOVE_MEMBER = 3,

    MAX_VALUE = REMOVE_MEMBER,
};

struct ExpectedReplica {
    ReplicaID replica_id;
    ExpectedReplicaType type{ExpectedReplicaType::PRESENT};  // 如果是ABSENT的话，不需要填写下面两个变量
    EngineType engine_type{EngineType::MAP};
    std::vector<PeerMember> initial_members;

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(replica_id.table_id >= 0, "replica table id should be greater than or equal to 0")
        RETURN_IF_INVALID_CONDITION(replica_id.shard_index >= 0,
                                    "replica shard index should be greater than or equal to 0")
        RETURN_IF_INVALID_CONDITION(replica_id.replica_seq >= 0, "replica index should be greater than or equal to 0")
        return Status::OK();
    }
};

using LogIndex = int64_t;

enum class WriteOpType : int32_t {
    NONE = 0,
    PUT = 1,
    DEL = 2,
    ADD_LEARNER = 3,
    PROMOTE_VOTER = 4,
    REMOVE_MEMBER = 5,
    CAS = 6,

    MAX_VALUE = CAS,
};

inline bool is_config_change_op(WriteOpType op) {
    return op == WriteOpType::ADD_LEARNER || op == WriteOpType::PROMOTE_VOTER || op == WriteOpType::REMOVE_MEMBER;
}

inline bool is_kv_write_op(WriteOpType op) {
    return op == WriteOpType::PUT || op == WriteOpType::DEL || op == WriteOpType::CAS;
}

struct LogEntry {
    Term term{0};
    LogIndex index{0};
    WriteOpType op_type{WriteOpType::NONE};
    Key key{""};
    Value value{""};
    Value expected{""};
    PeerMember config_member;
    ReplicaID config_replica_id;
    // 和 operation_id 概念不一样，这个rqeuest_id只是针对 put del cas 实现的
    // config change 或则 none 就也是空的
    std::string request_id;

    LogEntry() = default;
    LogEntry(Term term, LogIndex index, WriteOpType op_type, Key key, Value value)
            : term(term), index(index), op_type(op_type), key(std::move(key)), value(std::move(value)) {}

    bool operator==(const LogEntry& other) const {
        return term == other.term && index == other.index && op_type == other.op_type && key == other.key &&
               value == other.value && expected == other.expected && config_member == other.config_member &&
               config_replica_id == other.config_replica_id && request_id == other.request_id;
    }
    DEFINE_OPERATOR_NOT_EQUAL(LogEntry)

    std::string to_string() const {
        return fmt::format(
                "[term:{}, index:{}, key:{}, value:{}, expected:{}, op_type:{}, request_id:{}, "
                "config_member:{}, config_replica_id:{}]",
                term, index, key, value, expected, to<int32>(op_type), request_id, config_member.replica_id.to_string(),
                config_replica_id.to_string());
    }
};

}  // namespace adviskv
