#pragma once

#include <fmt/format.h>

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "common/define.h"
#include "common/optional.h"
#include "common/status.h"
#include "common/type.h"

namespace adviskv::storage {

using LogIndex = int64_t;

enum class WriteOpType : int32_t { PUT = 0, DEL = 1, NONE = 2 };

struct WriteProposal {
    WriteOpType op{WriteOpType::NONE};
    Key key;
    Value value;
};

struct NoopProposal {};

struct ProposeParam {
    std::variant<WriteProposal, NoopProposal> payload;

    static ProposeParam write(WriteOpType op, Key key, Value value) {
        return ProposeParam{
            WriteProposal{op, std::move(key), std::move(value)}};
    }

    static ProposeParam noop() { return ProposeParam{NoopProposal{}}; }
};

struct RaftMeta {
    Term current_term;
    std::optional<ReplicaID> voted_for;

    std::string to_string() const {
        return fmt::format("[term:{}, voted_for:{}]", current_term,
                           voted_for.has_value() ? voted_for->to_string()
                                                 : "none");
    }

    bool operator==(const RaftMeta& other) const {
        if (!(current_term == other.current_term)) return false;
        if (!(voted_for == other.voted_for)) return false;
        return true;
    }
    DEFINE_OPERATOR_NOT_EQUAL(RaftMeta)
};

struct LogEntry {
    Term term{0};
    LogIndex index{0};
    WriteOpType op_type;
    Key key{""};
    Value value{""};

    bool operator==(const LogEntry& other) const {
        return term == other.term and index == other.index and
               op_type == other.op_type and key == other.key and
               value == other.value;
    }
    DEFINE_OPERATOR_NOT_EQUAL(LogEntry)

    std::string to_string() const {
        return fmt::format("[term:{}, index:{}, key:{}, value:{}, op_type:{}]",
                           term, index, key, value, to<int32>(op_type));
    }
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

struct ReplicaRuntimeOptions {
    std::string data_dir;
    int32_t raft_rpc_timeout_ms{1000};

    bool operator==(const ReplicaRuntimeOptions& other) const {
        return data_dir == other.data_dir &&
               raft_rpc_timeout_ms == other.raft_rpc_timeout_ms;
    }

    DEFINE_OPERATOR_NOT_EQUAL(ReplicaRuntimeOptions)
};

struct ReplicaInitParam {
    ReplicaID replica_id;
    EngineType engine_type;
    Endpoint local_endpoint;
    std::vector<PeerMember> members;
    ReplicaRuntimeOptions runtime;

    bool operator==(const ReplicaInitParam& other) const {
        if (!(replica_id == other.replica_id)) return false;
        if (!(engine_type == other.engine_type)) return false;
        if (!(local_endpoint == other.local_endpoint)) return false;
        if (!(members == other.members)) return false;
        if (!(runtime == other.runtime)) return false;
        return true;
    }

    bool same_persisted_spec(const ReplicaInitParam& other) const {
        return replica_id == other.replica_id &&
               engine_type == other.engine_type &&
               local_endpoint == other.local_endpoint &&
               members == other.members;
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
    Term term{0};
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
    Term term{0};
    bool success{false};
    LogIndex last_log_index{0};
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
    Status status{Status::OK()};
    LogIndex snapshot_watermark{0};
};

struct SnapshotInstallPlan {
    LogIndex snapshot_index{0};
    Term snapshot_term{0};
    std::vector<LogEntry> retained_entries;
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

    std::string to_string() const {
        switch (type) {
            case RaftMessageType::REQUEST_VOTE:
                return fmt::format(
                    "[type:request_vote, target:{}, term:{}, last_log:{}:{}]",
                    target.replica_id.to_string(), vote_param.term,
                    vote_param.last_log_index, vote_param.last_log_term);
            case RaftMessageType::APPEND_ENTRIES:
                return fmt::format(
                    "[type:append_entries, target:{}, term:{}, prev:{}:{}, "
                    "leader_commit:{}, entries:{}]",
                    target.replica_id.to_string(), append_param.term,
                    append_param.prev_log_index, append_param.prev_log_term,
                    append_param.leader_commit, append_param.entries.size());
            case RaftMessageType::INSTALL_SNAPSHOT:
                return fmt::format(
                    "[type:install_snapshot, target:{}, term:{}, snapshot:{}:{}, "
                    "offset:{}, bytes:{}, done:{}]",
                    target.replica_id.to_string(), snapshot_param.term,
                    snapshot_param.snapshot_index, snapshot_param.snapshot_term,
                    snapshot_param.offset, snapshot_param.data.size(),
                    snapshot_param.done);
        }
        return "[type:unknown]";
    }
};

using RaftMessageOr = Optional<RaftMessage>;

// raftnode的产物，交给replica去做
struct RaftEffects {
    std::optional<RaftMeta> hard_state;
    std::vector<LogEntry> entries_to_append;
    std::optional<std::vector<LogEntry>> entries_to_rewrite;
    std::vector<RaftMessage> messages;

    std::string to_string() const {
        std::string result = fmt::format(
            "[hard_state:{}, append_entries:{}, rewrite_entries:{}, "
            "messages:{}",
            hard_state.has_value() ? hard_state->to_string() : "none",
            entries_to_append.size(),
            entries_to_rewrite.has_value()
                ? std::to_string(entries_to_rewrite->size())
                : "none",
            messages.size());

        if (!entries_to_append.empty()) {
            result += ", append=[";
            for (size_t i = 0; i < entries_to_append.size(); i++) {
                if (i > 0) result += ", ";
                result += entries_to_append[i].to_string();
            }
            result += "]";
        }

        if (entries_to_rewrite.has_value() && !entries_to_rewrite->empty()) {
            result += ", rewrite=[";
            for (size_t i = 0; i < entries_to_rewrite->size(); i++) {
                if (i > 0) result += ", ";
                result += (*entries_to_rewrite)[i].to_string();
            }
            result += "]";
        }

        if (!messages.empty()) {
            result += ", messages=[";
            for (size_t i = 0; i < messages.size(); i++) {
                if (i > 0) result += ", ";
                result += messages[i].to_string();
            }
            result += "]";
        }

        result += "]";
        return result;
    }
};

}  // namespace adviskv::storage