#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include "common/define.h"

#include "common/model/model.h"
#include "common/optional.h"
#include "common/status.h"
#include "common/stable_hash.h"

namespace adviskv::storage {

using ReplicaStatus = ::adviskv::StorageReplicaStatus;

enum class RaftMessageType : int32_t {
    REQUEST_VOTE = 0,
    PRE_VOTE = 1,
    APPEND_ENTRIES = 2,
    INSTALL_SNAPSHOT = 3,

    MAX_VALUE = INSTALL_SNAPSHOT,
};

struct RaftMeta {
    Term current_term{0};
    Optional<ReplicaID> voted_for;

    std::string to_string() const {
        return fmt::format("[term:{}, voted_for:{}]", current_term,
                           voted_for.has_value() ? voted_for->to_string() : "none");
    }

    bool operator==(const RaftMeta& other) const {
        if (!(current_term == other.current_term)) return false;
        if (!(voted_for == other.voted_for)) return false;
        return true;
    }
    DEFINE_OPERATOR_NOT_EQUAL(RaftMeta)
};

struct SnapshotHeader {
    LogIndex apply_index{0};
    Term apply_term{0};
    std::vector<RaftMember> members;
};
using SnapshotHeaderPtr = std::shared_ptr<SnapshotHeader>;

struct RequestRecord {
    std::string request_id;
    WriteOpType op{WriteOpType::NONE};
    uint64_t payload_hash{0};
    Status apply_status{Status::OK()};
    // 用于记录 request_id 对应的写记录在状态机中的执行结果。

    static uint64_t request_payload_hash(WriteOpType op, const Key& key, const Value& value,
                                         const Value& expected = {}) {
        StableHash64 hash;
        hash.append_uint64(static_cast<uint32_t>(op));
        hash.append_uint64(static_cast<uint64_t>(key.size()));
        hash.append(key);
        hash.append_uint64(static_cast<uint64_t>(value.size()));
        hash.append(value);
        hash.append_uint64(static_cast<uint64_t>(expected.size()));
        hash.append(expected);
        return hash.get_hash();
    }

    bool matches(const LogEntry& entry) const {
        return request_id == entry.request_id && op == entry.op_type &&
               payload_hash == request_payload_hash(entry.op_type, entry.key, entry.value, entry.expected);
    }

    template<typename Other>
    bool matches(const Other& other) const {
        return request_id == other.request_id && op == other.op &&
               payload_hash == request_payload_hash(other.op, other.key, other.value, other.expected);
    }

};

struct SnapshotData {
    SnapshotHeader header;
    std::vector<KV> kvs;
    std::vector<RequestRecord> request_records;
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

// 注意这里虽然内容是一样的，但是 term 代表的含义是下一任打算要候选的 term
using PreVoteParam = RequestVoteParam;
using PreVoteResult = RequestVoteResult;

struct AppendEntriesParam {
    ReplicaID from_replica_id;
    ReplicaID to_replica_id;
    Term term;
    std::vector<LogEntry> entries;
    LogIndex prev_log_index;
    Term prev_log_term;
    LogIndex leader_commit;
    // 读多数派确认的轮次标识。0 = 普通复制；>0 = 这条 AppendEntries 是某次
    // ReadIndex 的多数派检查（心跳形式、空 entries），取值是该读的 read_seq。
    // 响应走 ReadQuorumResponseEvent 计票路径，不推进复制的 next/match index。
    // 只在发送端内存里用，不上线路。计票时要求轮次匹配，不然上一轮检查或
    // 旧心跳的迟到响应可能被当成本轮多数派确认。
    uint64 read_quorum_seq{0};
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

struct RaftMessage {
    RaftMessageType type;
    PeerMember target;
    RequestVoteParam vote_param{};
    PreVoteParam pre_vote_param{};
    AppendEntriesParam append_param{};
    InstallSnapshotParam snapshot_param{};

    std::string to_string() const {
        switch (type) {
            case RaftMessageType::REQUEST_VOTE:
                return fmt::format("[type:request_vote, target:{}, term:{}, last_log:{}:{}]",
                                   target.replica_id.to_string(), vote_param.term, vote_param.last_log_index,
                                   vote_param.last_log_term);
            case RaftMessageType::PRE_VOTE:
                return fmt::format("[type:pre_vote, target:{}, term:{}, last_log:{}:{}]",
                                   target.replica_id.to_string(), pre_vote_param.term, pre_vote_param.last_log_index,
                                   pre_vote_param.last_log_term);
            case RaftMessageType::APPEND_ENTRIES:
                return fmt::format(
                        "[type:append_entries, target:{}, term:{}, prev:{}:{}, "
                        "leader_commit:{}, entries:{}]",
                        target.replica_id.to_string(), append_param.term, append_param.prev_log_index,
                        append_param.prev_log_term, append_param.leader_commit, append_param.entries.size());
            case RaftMessageType::INSTALL_SNAPSHOT:
                return fmt::format(
                        "[type:install_snapshot, target:{}, term:{}, "
                        "snapshot:{}:{}, offset:{}, bytes:{}, done:{}]",
                        target.replica_id.to_string(), snapshot_param.term, snapshot_param.snapshot_index,
                        snapshot_param.snapshot_term, snapshot_param.offset, snapshot_param.data.size(),
                        snapshot_param.done);
        }
        return "[type:unknown]";
    }
};

using RaftMessageOr = Optional<RaftMessage>;

struct RaftEffects {
    Optional<RaftMeta> hard_state;
    std::vector<LogEntry> entries_to_append;
    Optional<std::vector<LogEntry>> entries_to_rewrite;
    std::vector<RaftMessage> messages;

    std::string to_string() const {
        std::string result = fmt::format(
                "[hard_state:{}, append_entries:{}, rewrite_entries:{}, "
                "messages:{}",
                hard_state.has_value() ? hard_state->to_string() : "none", entries_to_append.size(),
                entries_to_rewrite.has_value() ? std::to_string(entries_to_rewrite->size()) : "none", messages.size());

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
