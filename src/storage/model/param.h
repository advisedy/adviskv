#pragma once

#include <absl/container/internal/inlined_vector.h>

#include <cstdint>
#include <memory>
#include <optional>

#include "common/status.h"
#include "common/type.h"

namespace adviskv::storage {

using Term = int64_t;
using LogIndex = int64_t;

enum class WriteOpType : int32_t { PUT = 0, DEL = 1, NONE = 2 };

struct PeerMember {
    NodeID node_id;
    ReplicaID replica_id;
    Endpoint endpoint;
};

struct LogEntry {
    Term term{0};
    LogIndex index{0};
    WriteOpType op_type;
    Key key;
    Value value;
};

struct PutParam {
    const Key& key;
    const Value& value;

    Status validate() const { return Status::OK(); }
};

struct GetParam {
    const Key& key;
    Status validate() const { return Status::OK(); }
};

struct ReplicaInitParam {
    ReplicaID replica_id;
    EngineType engine_type;
    Endpoint local_enopoint;
    std::vector<PeerMember> members;
};

struct RequestVoteParam {
    ReplicaID from_replica_id;
    ReplicaID to_replica_id;
    Term term;
    LogIndex last_log_index;
    Term last_log_term;
};

struct RequestVoteResult {
    Term term;
    bool vote_granted{false};
};

struct AppendEntriesParam {
    ReplicaID from_replica_id;
    ReplicaID to_replica_id;
    Term term;
    std::vector<LogEntry> entries;  // 这次想要追加的日志
    LogIndex prev_log_index;        // 代表公共的最后的那个index
    Term prev_log_term;
    LogIndex leader_commit;
};

struct AppendEntriesResult {
    Term term;
    bool success;
};

enum class RaftMessageType : uint8_t {
    REQUEST_VOTE,
    APPEND_ENTRIES,
};

// RaftNode 产出的消息，由 Replica 负责，接收到了之后会通过 RaftSender 发送
struct RaftMessage {
    RaftMessageType type;
    PeerMember target;
    RequestVoteParam vote_param{};
    AppendEntriesParam append_param{};
};

}  // namespace adviskv::storage