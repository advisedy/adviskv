#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <nlohmann/json.hpp>

#include "common/status.h"
#include "storage/model/model.h"
#include "storage/model/param.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/core/raft_core.h"
#include "storage/raft/state_machine/kv_state_machine.h"

namespace adviskv::maelstrom {

using Json = nlohmann::json;

class MaelstromRaftNode {
public:
    explicit MaelstromRaftNode(std::string data_root);
    void handle_line(const std::string& line);
    void tick();

private:
    struct PendingRpc {
        storage::RaftMessageType type{storage::RaftMessageType::REQUEST_VOTE};
        ReplicaID target;
        storage::AppendEntriesParam append{};
        storage::InstallSnapshotParam snapshot{};
    };

    struct PendingClient {
        std::string dest;
        int64_t in_reply_to{0};
        std::string ok_type;
        std::string request_id;
    };

    struct PendingRead {
        int64_t id{0};
        std::string dest;
        int64_t in_reply_to{0};
        std::string key;
        storage::LogIndex read_index{0};
        Term read_term{0};
        int acks{1};
        int quorum{1};
        std::unordered_set<int64_t> outstanding_rpc_ids;
        std::chrono::steady_clock::time_point start{};
    };

    void handle_msg(const Json& msg);
    void on_init(const std::string& src, const Json& body);
    void on_write(const std::string& src, const Json& body);
    void on_read(const std::string& src, const Json& body);
    void on_cas(const std::string& src, const Json& body);
    void on_raft_request_vote(const std::string& src, const Json& body);
    void on_raft_request_vote_ok(const std::string& src, const Json& body);
    void on_raft_pre_vote(const std::string& src, const Json& body);
    void on_raft_pre_vote_ok(const std::string& src, const Json& body);
    void on_raft_append_entries(const std::string& src, const Json& body);
    void on_raft_append_entries_ok(const std::string& src, const Json& body);
    void on_raft_install_snapshot(const std::string& src, const Json& body);
    void on_raft_install_snapshot_ok(const std::string& src, const Json& body);

    Status drive_effects(storage::RaftEffects effects, std::vector<int64_t>* sent_msg_ids = nullptr);
    void apply_committed_unlocked();
    void complete_pending_clients_unlocked();
    void complete_pending_reads_unlocked();
    void fail_all_pending_reads_unlocked(const std::string& reason);
    void note_read_index_peer_ack_unlocked(int64_t rpc_msg_id, Term response_term);
    void reply_read_value_unlocked(const PendingRead& read);
    std::vector<int64_t> send_raft_messages_unlocked(std::vector<storage::RaftMessage> messages);

    void send_raw(Json msg);
    void reply_ok(const std::string& dest, int64_t in_reply_to, const std::string& type, Json extra = Json::object());
    void reply_error(const std::string& dest, int64_t in_reply_to, int32_t code, const std::string& text);

    int64_t next_msg_id_unlocked();
    ReplicaID replica_of(const std::string& node_id) const;
    std::string node_of(const ReplicaID& replica_id) const;
    PeerMember peer_of(const std::string& node_id) const;

    Json encode_log_entry(const storage::LogEntry& entry) const;
    storage::LogEntry decode_log_entry(const Json& json) const;
    Json encode_append_param(const storage::AppendEntriesParam& param) const;
    storage::AppendEntriesParam decode_append_param(const Json& body) const;
    Json encode_snapshot_param(const storage::InstallSnapshotParam& param) const;
    storage::InstallSnapshotParam decode_snapshot_param(const Json& body) const;

    static std::string key_to_string(const Json& key);
    static Json value_from_string(const std::string& value);
    static std::string value_to_string(const Json& value);

    std::string data_root_;
    std::string node_id_;
    std::vector<std::string> node_ids_;
    std::unordered_map<std::string, int32_t> node_to_seq_;
    std::unordered_map<int32_t, std::string> seq_to_node_;

    ReplicaID self_id_{};
    std::unique_ptr<storage::PersistEngine> persist_;
    std::unique_ptr<storage::KvStateMachine> state_machine_;
    std::unique_ptr<storage::RaftCore> core_;

    std::mutex mutex_;
    int64_t next_msg_id_{1};
    int64_t next_read_id_{1};
    std::unordered_map<int64_t, PendingRpc> pending_rpcs_;
    std::unordered_map<storage::LogIndex, PendingClient> pending_clients_;
    std::unordered_map<int64_t, PendingRead> pending_reads_;
    std::unordered_map<int64_t, int64_t> rpc_msg_to_read_id_;
};

}
