#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include "common/define.h"
#include "common/model/type.h"
#include "common/optional.h"
#include "common/status.h"

namespace adviskv::storage {

using ReplicaStatus = ::adviskv::StorageReplicaStatus;

using LogIndex = int64_t;

enum class WriteOpType : int32_t {
    PUT = 0,
    DEL = 1,
    NONE = 2,
    ADD_LEARNER = 3,
    PROMOTE_VOTER = 4,
    REMOVE_MEMBER = 5,
};

inline bool is_config_change_op(WriteOpType op) {
    return op == WriteOpType::ADD_LEARNER || op == WriteOpType::PROMOTE_VOTER ||
           op == WriteOpType::REMOVE_MEMBER;
}

struct RaftMeta {
    Term current_term{0};
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
    WriteOpType op_type{WriteOpType::NONE};
    Key key{""};
    Value value{""};
    PeerMember config_member;
    ReplicaID config_replica_id;

    LogEntry() = default;
    LogEntry(Term term, LogIndex index, WriteOpType op_type, Key key,
             Value value)
        : term(term),
          index(index),
          op_type(op_type),
          key(std::move(key)),
          value(std::move(value)) {}

    bool operator==(const LogEntry& other) const {
        return term == other.term && index == other.index &&
               op_type == other.op_type && key == other.key &&
               value == other.value && config_member == other.config_member &&
               config_replica_id == other.config_replica_id;
    }
    DEFINE_OPERATOR_NOT_EQUAL(LogEntry)

    std::string to_string() const {
        return fmt::format(
            "[term:{}, index:{}, key:{}, value:{}, op_type:{}, "
            "config_member:{}, config_replica_id:{}]",
            term, index, key, value, to<int32>(op_type),
            config_member.replica_id.to_string(),
            config_replica_id.to_string());
    }
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
    std::vector<LogEntry> entries;
    LogIndex prev_log_index;
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

struct InstallSnapshotContext {
    LogIndex snapshot_index{0};
    Term snapshot_term{0};
    std::vector<RaftMember> snapshot_members;
};

enum class RaftMessageType : uint8_t {
    REQUEST_VOTE,
    APPEND_ENTRIES,
    INSTALL_SNAPSHOT,
};

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
                    "[type:install_snapshot, target:{}, term:{}, "
                    "snapshot:{}:{}, offset:{}, bytes:{}, done:{}]",
                    target.replica_id.to_string(), snapshot_param.term,
                    snapshot_param.snapshot_index,
                    snapshot_param.snapshot_term, snapshot_param.offset,
                    snapshot_param.data.size(), snapshot_param.done);
        }
        return "[type:unknown]";
    }
};

using RaftMessageOr = Optional<RaftMessage>;

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
