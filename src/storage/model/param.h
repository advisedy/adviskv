#pragma once

#include <cstdint>
#include <memory>
#include <optional>

#include "common/define.h"
#include "common/status.h"
#include "common/type.h"

namespace adviskv::storage {

using LogIndex = int64_t;

enum class WriteOpType : int32_t { PUT = 0, DEL = 1, NONE = 2 };

enum class WalRecoveryAction : int32_t {
    NONE = 0,
    TRUNCATED_UNCOMMITTED = 1,
    NEED_RAFT_CATCHUP = 2,
};

struct WalRecoveryInfo {
    WalRecoveryAction action{WalRecoveryAction::NONE};
    LogIndex last_good_index{0};
    int64_t last_good_offset{0};
    LogIndex original_commit_index{0};
    LogIndex recovery_target_commit_index{0};
};

struct RaftMeta {
    Term current_term;
    LogIndex commit_index{0};
    std::optional<ReplicaID> voted_for;
    bool operator==(const RaftMeta& other) const {
        if (!(current_term == other.current_term)) return false;
        if (!(commit_index == other.commit_index)) return false;
        if (!(voted_for == other.voted_for)) return false;
        return true;
    }
    DEFINE_OPERATOR_NOT_EQUAL(RaftMeta)
};

struct LogEntry {
    Term term{0};
    LogIndex index{0};
    WriteOpType op_type;
    Key key;
    Value value;

    bool operator==(const LogEntry& other) const {
        return term == other.term and index == other.index and
               op_type == other.op_type and key == other.key and
               value == other.value;
    }
    DEFINE_OPERATOR_NOT_EQUAL(LogEntry)
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

struct DelParam {
    const Key& key;
    Status validate() const { return Status::OK(); }
};

struct ReplicaInitParam {
    ReplicaID replica_id;
    EngineType engine_type;
    Endpoint local_endpoint;
    std::vector<PeerMember> members;
    std::string data_dir;  // 存放那些持久化数据的根目录

    bool operator==(const ReplicaInitParam& other) const {
        if (!(replica_id == other.replica_id)) return false;
        if (!(engine_type == other.engine_type)) return false;
        if (!(local_endpoint == other.local_endpoint)) return false;
        if (!(members == other.members)) return false;
        if (!(data_dir == other.data_dir)) return false;
        return true;
    }

    DEFINE_OPERATOR_NOT_EQUAL(ReplicaInitParam)
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

struct InstallSnapshotParam {
    ReplicaID from_replica_id;
    ReplicaID to_replica_id;
    Term term;
    LogIndex snapshot_index;
    Term snapshot_term;
    uint64 offset{0};
    std::string data;
    bool done{false};
};

struct InstallSnapshotResult {
    Term term{0};
    bool success{false};
};

enum class RaftMessageType : uint8_t {
    REQUEST_VOTE,
    APPEND_ENTRIES,
    INSTALL_SNAPSHOT,
};

// RaftNode 产出的消息，由 Replica 负责，接收到了之后会通过 RaftSender 发送
struct RaftMessage {
    RaftMessageType type;
    PeerMember target;
    RequestVoteParam vote_param{};
    AppendEntriesParam append_param{};
    InstallSnapshotParam snapshot_param{};
};

}  // namespace adviskv::storage