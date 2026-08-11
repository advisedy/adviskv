#include "maelstrom_node.h"

#include <cctype>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <utility>

#include <fmt/format.h>

#include "common/log.h"
#include "common/model/engine_type.h"
#include "common/status.h"
#include "storage/raft/state_machine/state_machine.h"

namespace adviskv::maelstrom {
namespace {

constexpr TableID kTableId = 1;
constexpr ShardIndex kShardIndex = 0;

constexpr int32_t kErrTemporarilyUnavailable = 11;
constexpr int32_t kErrKeyDoesNotExist = 20;
constexpr int32_t kErrPreconditionFailed = 22;

std::string b64_encode(const std::string& in) {
    static const char* kTable = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((in.size() + 2) / 3) * 4);
    size_t i = 0;
    while (i + 3 <= in.size()) {
        unsigned int n = (static_cast<unsigned char>(in[i]) << 16) | (static_cast<unsigned char>(in[i + 1]) << 8) |
                         static_cast<unsigned char>(in[i + 2]);
        out.push_back(kTable[(n >> 18) & 63]);
        out.push_back(kTable[(n >> 12) & 63]);
        out.push_back(kTable[(n >> 6) & 63]);
        out.push_back(kTable[n & 63]);
        i += 3;
    }
    if (i < in.size()) {
        unsigned int n = static_cast<unsigned char>(in[i]) << 16;
        out.push_back(kTable[(n >> 18) & 63]);
        if (i + 1 < in.size()) {
            n |= static_cast<unsigned char>(in[i + 1]) << 8;
            out.push_back(kTable[(n >> 12) & 63]);
            out.push_back(kTable[(n >> 6) & 63]);
            out.push_back('=');
        } else {
            out.push_back(kTable[(n >> 12) & 63]);
            out.push_back('=');
            out.push_back('=');
        }
    }
    return out;
}

std::string b64_decode(const std::string& in) {
    auto dec = [](char c) -> int {
        if (c >= 'A' && c <= 'Z') return c - 'A';
        if (c >= 'a' && c <= 'z') return c - 'a' + 26;
        if (c >= '0' && c <= '9') return c - '0' + 52;
        if (c == '+') return 62;
        if (c == '/') return 63;
        return -1;
    };
    std::string out;
    out.reserve(in.size() * 3 / 4);
    int val = 0;
    int valb = -8;
    for (unsigned char c : in) {
        if (c == '=') break;
        int d = dec(static_cast<char>(c));
        if (d < 0) continue;
        val = (val << 6) + d;
        valb += 6;
        if (valb >= 0) {
            out.push_back(static_cast<char>((val >> valb) & 0xFF));
            valb -= 8;
        }
    }
    return out;
}

}  // namespace

MaelstromRaftNode::MaelstromRaftNode(std::string data_root) : data_root_(std::move(data_root)) {}

void MaelstromRaftNode::handle_line(const std::string& line) {
    if (line.empty()) return;
    try {
        Json msg = Json::parse(line);
        handle_msg(msg);
    } catch (const std::exception& ex) {
        LOG_ERROR("failed to handle maelstrom line: {} raw={}", ex.what(), line);
    }
}

void MaelstromRaftNode::tick() {
    std::lock_guard lock(mutex_);
    if (!core_) return;
    storage::RaftEffects effects;
    core_->tick(effects);
    Status status = drive_effects(std::move(effects));
    if (status.fail()) {
        LOG_WARN("tick drive_effects failed: {}", status.to_string());
    }
    apply_committed_unlocked();
    complete_pending_clients_unlocked();
    complete_pending_reads_unlocked();
}

void MaelstromRaftNode::handle_msg(const Json& msg) {
    const std::string src = msg.at("src").get<std::string>();
    const Json& body = msg.at("body");
    const std::string type = body.at("type").get<std::string>();

    if (type == "init") {
        on_init(src, body);
    } else if (type == "write") {
        on_write(src, body);
    } else if (type == "read") {
        on_read(src, body);
    } else if (type == "cas") {
        on_cas(src, body);
    } else if (type == "raft_request_vote") {
        on_raft_request_vote(src, body);
    } else if (type == "raft_request_vote_ok") {
        on_raft_request_vote_ok(src, body);
    } else if (type == "raft_pre_vote") {
        on_raft_pre_vote(src, body);
    } else if (type == "raft_pre_vote_ok") {
        on_raft_pre_vote_ok(src, body);
    } else if (type == "raft_append_entries") {
        on_raft_append_entries(src, body);
    } else if (type == "raft_append_entries_ok") {
        on_raft_append_entries_ok(src, body);
    } else if (type == "raft_install_snapshot") {
        on_raft_install_snapshot(src, body);
    } else if (type == "raft_install_snapshot_ok") {
        on_raft_install_snapshot_ok(src, body);
    } else {
        LOG_WARN("ignore unknown message type={}", type);
        if (body.contains("msg_id")) {
            reply_error(src, body.at("msg_id").get<int64_t>(), 10, "not supported");
        }
    }
}

void MaelstromRaftNode::on_init(const std::string& src, const Json& body) {
    std::lock_guard lock(mutex_);
    node_id_ = body.at("node_id").get<std::string>();
    node_ids_.clear();
    node_to_seq_.clear();
    seq_to_node_.clear();

    const Json& ids = body.at("node_ids");
    for (size_t i = 0; i < ids.size(); ++i) {
        const std::string id = ids[i].get<std::string>();
        node_ids_.push_back(id);
        node_to_seq_[id] = static_cast<int32_t>(i);
        seq_to_node_[static_cast<int32_t>(i)] = id;
    }

    self_id_ = ReplicaID{kTableId, kShardIndex, node_to_seq_.at(node_id_)};

    std::vector<PeerMember> members;
    members.reserve(node_ids_.size());
    for (const std::string& id : node_ids_) {
        members.push_back(peer_of(id));
    }

    std::filesystem::path dir = std::filesystem::path(data_root_) / node_id_;
    std::filesystem::create_directories(dir);

    persist_ = std::make_unique<storage::PersistEngine>(dir.string(), self_id_);
    Status status = persist_->init();
    if (status.fail()) {
        reply_error(src, body.at("msg_id").get<int64_t>(), 13, status.to_string());
        return;
    }

    state_machine_ = std::make_unique<storage::KvStateMachine>(EngineType::MAP);
    core_ = std::make_unique<storage::RaftCore>(self_id_, members);

    storage::PersistEngine::RecoverResult recovered;
    status = persist_->recover(recovered);
    if (status.fail()) {
        reply_error(src, body.at("msg_id").get<int64_t>(), 13, status.to_string());
        return;
    }
    if (recovered.snapshot) {
        status = state_machine_->restore(recovered.snapshot, [this](const storage::KvVisitor& visitor) -> Status {
            return persist_->for_each_snapshot_kv(visitor);
        });
        if (status.fail()) {
            reply_error(src, body.at("msg_id").get<int64_t>(), 13, status.to_string());
            return;
        }
        core_->install_local_snapshot(storage::InstallSnapshotContext{
                recovered.snapshot->apply_index,
                recovered.snapshot->apply_term,
                recovered.snapshot->members,
        });
    }
    core_->update_raft_meta(recovered.raft_meta);
    core_->update_log_entries(recovered.wal_entries);
    if (recovered.need_recover) {
        core_->enter_recovering();
    }

    apply_committed_unlocked();

    LOG_INFO("maelstrom raft node init ok node_id={} members={} data_dir={}", node_id_, members.size(), dir.string());
    reply_ok(src, body.at("msg_id").get<int64_t>(), "init_ok");
}

void MaelstromRaftNode::on_write(const std::string& src, const Json& body) {
    std::lock_guard lock(mutex_);
    int64_t msg_id = body.at("msg_id").get<int64_t>();
    if (!core_ || !core_->is_leader()) {
        reply_error(src, msg_id, kErrTemporarilyUnavailable, "not leader");
        return;
    }
    if (mutation_in_flight_) {
        reply_error(src, msg_id, kErrTemporarilyUnavailable, "mutation in flight");
        return;
    }

    const std::string key = key_to_string(body.at("key"));
    const std::string value = value_to_string(body.at("value"));

    storage::RaftEffects effects;
    auto [status, index] = core_->propose(storage::ProposeParam::write(storage::WriteOpType::PUT, key, value), effects);
    if (status.fail()) {
        reply_error(src, msg_id, kErrTemporarilyUnavailable, status.to_string());
        return;
    }
    status = drive_effects(std::move(effects));
    if (status.fail()) {
        reply_error(src, msg_id, 13, status.to_string());
        return;
    }

    mutation_in_flight_ = true;
    pending_clients_[index] = PendingClient{src, msg_id, "write_ok"};
    apply_committed_unlocked();
    complete_pending_clients_unlocked();
}

void MaelstromRaftNode::on_read(const std::string& src, const Json& body) {
    std::lock_guard lock(mutex_);
    const int64_t msg_id = body.at("msg_id").get<int64_t>();
    if (!core_ || !core_->is_leader()) {
        reply_error(src, msg_id, kErrTemporarilyUnavailable, "not leader");
        return;
    }

    storage::LogIndex read_index = 0;
    Term read_term = 0;
    storage::RaftEffects effects;
    Status status = core_->build_append_entries_for_read(effects, read_index, read_term);
    if (status.fail()) {
        reply_error(src, msg_id, kErrTemporarilyUnavailable, status.to_string());
        return;
    }

    PendingRead pending;
    pending.id = next_read_id_++;
    pending.dest = src;
    pending.in_reply_to = msg_id;
    pending.key = key_to_string(body.at("key"));
    pending.read_index = read_index;
    pending.read_term = read_term;
    pending.acks = 1;
    pending.quorum = core_->quorum_size();
    pending.start = std::chrono::steady_clock::now();

    std::vector<int64_t> sent_ids;
    status = drive_effects(std::move(effects), &sent_ids);
    if (status.fail()) {
        reply_error(src, msg_id, 13, status.to_string());
        return;
    }
    for (int64_t rpc_id : sent_ids) {
        pending.outstanding_rpc_ids.insert(rpc_id);
        rpc_msg_to_read_id_[rpc_id] = pending.id;
    }

    pending_reads_[pending.id] = std::move(pending);
    apply_committed_unlocked();
    complete_pending_reads_unlocked();
}

void MaelstromRaftNode::on_cas(const std::string& src, const Json& body) {
    std::lock_guard lock(mutex_);
    const int64_t msg_id = body.at("msg_id").get<int64_t>();
    if (!core_ || !core_->is_leader()) {
        reply_error(src, msg_id, kErrTemporarilyUnavailable, "not leader");
        return;
    }
    if (mutation_in_flight_) {
        reply_error(src, msg_id, kErrTemporarilyUnavailable, "mutation in flight");
        return;
    }

    const std::string key = key_to_string(body.at("key"));
    const std::string from = value_to_string(body.at("from"));
    const std::string to = value_to_string(body.at("to"));

    Value cur;
    Status get_status = state_machine_->get(key, cur);
    if (get_status.code() == StatusCode::KEY_NOT_FOUND) {
        reply_error(src, msg_id, kErrKeyDoesNotExist, "key does not exist");
        return;
    }
    if (get_status.fail()) {
        reply_error(src, msg_id, 13, get_status.to_string());
        return;
    }
    if (cur != from) {
        reply_error(src, msg_id, kErrPreconditionFailed, "precondition failed");
        return;
    }

    storage::RaftEffects effects;
    auto [status, index] = core_->propose(storage::ProposeParam::write(storage::WriteOpType::PUT, key, to), effects);
    if (status.fail()) {
        reply_error(src, msg_id, kErrTemporarilyUnavailable, status.to_string());
        return;
    }
    status = drive_effects(std::move(effects));
    if (status.fail()) {
        reply_error(src, msg_id, 13, status.to_string());
        return;
    }

    mutation_in_flight_ = true;
    pending_clients_[index] = PendingClient{src, msg_id, "cas_ok"};
    apply_committed_unlocked();
    complete_pending_clients_unlocked();
}

void MaelstromRaftNode::on_raft_request_vote(const std::string& src, const Json& body) {
    std::lock_guard lock(mutex_);
    if (!core_) return;

    storage::RequestVoteParam param;
    param.from_replica_id = replica_of(src);
    param.to_replica_id = self_id_;
    param.term = body.at("term").get<int64_t>();
    param.last_log_index = body.at("last_log_index").get<int64_t>();
    param.last_log_term = body.at("last_log_term").get<int64_t>();

    storage::RequestVoteResult result;
    storage::RaftEffects effects;
    core_->handle_request_vote(param, result, effects);
    Status status = drive_effects(std::move(effects));
    if (status.fail()) {
        LOG_WARN("handle_request_vote effects failed: {}", status.to_string());
    }
    apply_committed_unlocked();
    complete_pending_clients_unlocked();
    if (!core_->is_leader()) {
        fail_all_pending_reads_unlocked("stepped down");
    } else {
        complete_pending_reads_unlocked();
    }

    Json resp = Json::object();
    resp["type"] = "raft_request_vote_ok";
    resp["in_reply_to"] = body.at("msg_id").get<int64_t>();
    resp["msg_id"] = next_msg_id_unlocked();
    resp["term"] = result.term;
    resp["vote_granted"] = result.vote_granted;

    Json msg = Json::object();
    msg["src"] = node_id_;
    msg["dest"] = src;
    msg["body"] = std::move(resp);
    send_raw(std::move(msg));
}

void MaelstromRaftNode::on_raft_request_vote_ok(const std::string&, const Json& body) {
    std::lock_guard lock(mutex_);
    if (!core_) return;
    const int64_t in_reply_to = body.at("in_reply_to").get<int64_t>();
    auto it = pending_rpcs_.find(in_reply_to);
    if (it == pending_rpcs_.end()) {
        LOG_DEBUG("stale raft_request_vote_ok in_reply_to={}", in_reply_to);
        return;
    }
    PendingRpc pending = it->second;
    pending_rpcs_.erase(it);

    storage::RequestVoteResult result;
    result.term = body.at("term").get<int64_t>();
    result.vote_granted = body.at("vote_granted").get<bool>();

    storage::RaftEffects effects;
    core_->handle_vote_response(pending.target, result, effects);
    Status status = drive_effects(std::move(effects));
    if (status.fail()) {
        LOG_WARN("handle_vote_response effects failed: {}", status.to_string());
    }
    apply_committed_unlocked();
    complete_pending_clients_unlocked();
    if (!core_->is_leader()) {
        fail_all_pending_reads_unlocked("stepped down");
    } else {
        complete_pending_reads_unlocked();
    }
}

void MaelstromRaftNode::on_raft_pre_vote(const std::string& src, const Json& body) {
    std::lock_guard lock(mutex_);
    if (!core_) return;

    storage::PreVoteParam param;
    param.from_replica_id = replica_of(src);
    param.to_replica_id = self_id_;
    param.term = body.at("term").get<int64_t>();
    param.last_log_index = body.at("last_log_index").get<int64_t>();
    param.last_log_term = body.at("last_log_term").get<int64_t>();

    storage::PreVoteResult result;
    storage::RaftEffects effects;
    core_->handle_pre_vote(param, result, effects);
    Status status = drive_effects(std::move(effects));
    if (status.fail()) {
        LOG_WARN("handle_pre_vote effects failed: {}", status.to_string());
    }
    apply_committed_unlocked();
    complete_pending_clients_unlocked();
    if (!core_->is_leader()) {
        fail_all_pending_reads_unlocked("stepped down");
    } else {
        complete_pending_reads_unlocked();
    }

    Json resp = Json::object();
    resp["type"] = "raft_pre_vote_ok";
    resp["in_reply_to"] = body.at("msg_id").get<int64_t>();
    resp["msg_id"] = next_msg_id_unlocked();
    resp["term"] = result.term;
    resp["vote_granted"] = result.vote_granted;

    Json msg = Json::object();
    msg["src"] = node_id_;
    msg["dest"] = src;
    msg["body"] = std::move(resp);
    send_raw(std::move(msg));
}

void MaelstromRaftNode::on_raft_pre_vote_ok(const std::string&, const Json& body) {
    std::lock_guard lock(mutex_);
    if (!core_) return;
    const int64_t in_reply_to = body.at("in_reply_to").get<int64_t>();
    auto it = pending_rpcs_.find(in_reply_to);
    if (it == pending_rpcs_.end()) {
        LOG_DEBUG("stale raft_pre_vote_ok in_reply_to={}", in_reply_to);
        return;
    }
    PendingRpc pending = it->second;
    pending_rpcs_.erase(it);

    storage::PreVoteResult result;
    result.term = body.at("term").get<int64_t>();
    result.vote_granted = body.at("vote_granted").get<bool>();

    storage::RaftEffects effects;
    core_->handle_pre_vote_response(pending.target, result, effects);
    Status status = drive_effects(std::move(effects));
    if (status.fail()) {
        LOG_WARN("handle_pre_vote_response effects failed: {}", status.to_string());
    }
    apply_committed_unlocked();
    complete_pending_clients_unlocked();
    if (!core_->is_leader()) {
        fail_all_pending_reads_unlocked("stepped down");
    } else {
        complete_pending_reads_unlocked();
    }
}

void MaelstromRaftNode::on_raft_append_entries(const std::string& src, const Json& body) {
    std::lock_guard lock(mutex_);
    if (!core_) return;

    storage::AppendEntriesParam param = decode_append_param(body);
    param.from_replica_id = replica_of(src);
    param.to_replica_id = self_id_;

    storage::AppendEntriesResult result;
    storage::RaftEffects effects;
    core_->handle_append_entries(param, result, effects);
    Status status = drive_effects(std::move(effects));
    if (status.fail()) {
        LOG_WARN("handle_append_entries effects failed: {}", status.to_string());
    }
    apply_committed_unlocked();
    complete_pending_clients_unlocked();
    if (!core_->is_leader()) {
        fail_all_pending_reads_unlocked("stepped down");
    } else {
        complete_pending_reads_unlocked();
    }

    Json resp = Json::object();
    resp["type"] = "raft_append_entries_ok";
    resp["in_reply_to"] = body.at("msg_id").get<int64_t>();
    resp["msg_id"] = next_msg_id_unlocked();
    resp["term"] = result.term;
    resp["success"] = result.success;
    resp["last_log_index"] = result.last_log_index;

    Json msg = Json::object();
    msg["src"] = node_id_;
    msg["dest"] = src;
    msg["body"] = std::move(resp);
    send_raw(std::move(msg));
}

void MaelstromRaftNode::on_raft_append_entries_ok(const std::string&, const Json& body) {
    std::lock_guard lock(mutex_);
    if (!core_) return;
    const int64_t in_reply_to = body.at("in_reply_to").get<int64_t>();
    auto it = pending_rpcs_.find(in_reply_to);
    if (it == pending_rpcs_.end()) {
        LOG_DEBUG("stale raft_append_entries_ok in_reply_to={}", in_reply_to);
        return;
    }
    PendingRpc pending = it->second;
    pending_rpcs_.erase(it);

    storage::AppendEntriesResult result;
    result.term = body.at("term").get<int64_t>();
    result.success = body.at("success").get<bool>();
    result.last_log_index = body.at("last_log_index").get<int64_t>();

    storage::RaftEffects effects;
    Status status = core_->handle_append_response(pending.target, pending.append, result, effects);
    if (status.fail()) {
        LOG_DEBUG("handle_append_response: {}", status.to_string());
    }
    status = drive_effects(std::move(effects));
    if (status.fail()) {
        LOG_WARN("handle_append_response effects failed: {}", status.to_string());
    }
    note_read_index_peer_ack_unlocked(in_reply_to, result.term);
    apply_committed_unlocked();
    complete_pending_clients_unlocked();
    if (!core_->is_leader()) {
        fail_all_pending_reads_unlocked("stepped down");
    } else {
        complete_pending_reads_unlocked();
    }
}

void MaelstromRaftNode::on_raft_install_snapshot(const std::string& src, const Json& body) {
    std::lock_guard lock(mutex_);
    if (!core_) return;

    storage::InstallSnapshotParam param = decode_snapshot_param(body);
    param.from_replica_id = replica_of(src);
    param.to_replica_id = self_id_;

    storage::RaftEffects prepare_effects;
    Status prepare_status = core_->prepare_install_snapshot(param, prepare_effects);
    Status status = drive_effects(std::move(prepare_effects));
    if (status.fail()) {
        LOG_WARN("prepare_install_snapshot effects failed: {}", status.to_string());
    }

    storage::InstallSnapshotResult result;
    result.term = core_->current_term();
    result.status = prepare_status;

    if (prepare_status.ok()) {
        status = persist_->append_snapshot_chunk(param);
        if (status.fail()) {
            result.status = status;
        } else if (param.done) {
            status = persist_->finish_snapshot_receive();
            if (status.ok()) {
                storage::PersistEngine::RecoverResult recovered;
                storage::SnapshotPtr snap;
                status = persist_->load_snapshot_meta(snap);
                if (status.ok() && snap) {
                    status = state_machine_->restore(snap, [this](const storage::KvVisitor& visitor) -> Status {
                        return persist_->for_each_snapshot_kv(visitor);
                    });
                }
                if (status.ok()) {
                    storage::InstallSnapshotContext context;
                    context.snapshot_index = param.snapshot_index;
                    context.snapshot_term = param.snapshot_term;
                    if (snap) {
                        context.snapshot_members = snap->members;
                    }
                    storage::RaftEffects install_effects;
                    core_->commit_install_snapshot(context, install_effects);
                    status = drive_effects(std::move(install_effects));
                }
            }
            result.status = status;
        }
    } else if (prepare_status.code() == StatusCode::ALREADY_EXIST ||
               (prepare_status.fail() && param.snapshot_index <= core_->snapshot_index())) {
        if (prepare_status.code() == StatusCode::ALREADY_EXIST) {
            result.snapshot_watermark = param.snapshot_index;
        } else {
            result.snapshot_watermark = core_->snapshot_index();
        }
    }

    apply_committed_unlocked();
    complete_pending_clients_unlocked();
    if (!core_->is_leader()) {
        fail_all_pending_reads_unlocked("stepped down");
    } else {
        complete_pending_reads_unlocked();
    }

    Json resp = Json::object();
    resp["type"] = "raft_install_snapshot_ok";
    resp["in_reply_to"] = body.at("msg_id").get<int64_t>();
    resp["msg_id"] = next_msg_id_unlocked();
    resp["term"] = result.term;
    resp["ok"] = result.status.ok();
    resp["status_code"] = static_cast<int64_t>(result.status.code());
    resp["status_msg"] = result.status.msg();
    resp["snapshot_watermark"] = result.snapshot_watermark;

    Json msg = Json::object();
    msg["src"] = node_id_;
    msg["dest"] = src;
    msg["body"] = std::move(resp);
    send_raw(std::move(msg));
}

void MaelstromRaftNode::on_raft_install_snapshot_ok(const std::string&, const Json& body) {
    std::lock_guard lock(mutex_);
    if (!core_) return;
    const int64_t in_reply_to = body.at("in_reply_to").get<int64_t>();
    auto it = pending_rpcs_.find(in_reply_to);
    if (it == pending_rpcs_.end()) {
        LOG_DEBUG("stale raft_install_snapshot_ok in_reply_to={}", in_reply_to);
        return;
    }
    PendingRpc pending = it->second;
    pending_rpcs_.erase(it);

    storage::InstallSnapshotResult result;
    result.term = body.at("term").get<int64_t>();
    const bool ok = body.at("ok").get<bool>();
    if (ok) {
        result.status = Status::OK();
    } else {
        result.status = Status{StatusCode::ERROR, body.value("status_msg", "snapshot failed")};
    }
    if (body.contains("snapshot_watermark")) {
        result.snapshot_watermark = body.at("snapshot_watermark").get<int64_t>();
    }

    storage::RaftEffects effects;
    core_->handle_install_snapshot_response(pending.target, pending.snapshot, result, effects);
    Status status = drive_effects(std::move(effects));
    if (status.fail()) {
        LOG_WARN("handle_install_snapshot_response effects failed: {}", status.to_string());
    }
    note_read_index_peer_ack_unlocked(in_reply_to, result.term);
    apply_committed_unlocked();
    complete_pending_clients_unlocked();
    if (!core_->is_leader()) {
        fail_all_pending_reads_unlocked("stepped down");
    } else {
        complete_pending_reads_unlocked();
    }
}

Status MaelstromRaftNode::drive_effects(storage::RaftEffects effects, std::vector<int64_t>* sent_msg_ids) {
    if (effects.entries_to_rewrite.has_value() && !effects.entries_to_append.empty()) {
        return Status::INVALID_ARGUMENT("cannot append and rewrite raft log in one effects batch");
    }
    if (effects.hard_state.has_value()) {
        RETURN_IF_INVALID_STATUS(persist_->save_raft_meta(*effects.hard_state))
    }
    if (effects.entries_to_rewrite.has_value()) {
        RETURN_IF_INVALID_STATUS(persist_->rewrite_wal(*effects.entries_to_rewrite))
    }
    if (!effects.entries_to_append.empty()) {
        RETURN_IF_INVALID_STATUS(persist_->append_wal_batch(effects.entries_to_append))
    }
    if (!effects.messages.empty()) {
        std::vector<int64_t> ids = send_raft_messages_unlocked(std::move(effects.messages));
        if (sent_msg_ids != nullptr) {
            sent_msg_ids->insert(sent_msg_ids->end(), ids.begin(), ids.end());
        }
    }
    return Status::OK();
}

void MaelstromRaftNode::apply_committed_unlocked() {
    if (!core_ || !state_machine_) return;
    std::vector<storage::LogEntry> entries = core_->extract_committed_entries();
    for (const storage::LogEntry& entry : entries) {
        if (is_config_change_op(entry.op_type)) {
            Status status = core_->apply_config_entry(entry);
            if (status.fail()) {
                LOG_ERROR("apply_config_entry failed: {}", status.to_string());
            }
        } else {
            Status status = state_machine_->apply(entry);
            if (status.fail()) {
                LOG_ERROR("state_machine apply failed: {}", status.to_string());
            }
            core_->advance_last_applied(entry.index);
        }
    }
}

void MaelstromRaftNode::complete_pending_clients_unlocked() {
    if (!core_) return;
    const storage::LogIndex applied = core_->last_applied();
    std::vector<storage::LogIndex> done;
    for (const auto& [index, client] : pending_clients_) {
        if (index <= applied) {
            reply_ok(client.dest, client.in_reply_to, client.ok_type);
            done.push_back(index);
        }
    }
    for (storage::LogIndex idx : done) {
        pending_clients_.erase(idx);
    }
    if (pending_clients_.empty()) {
        mutation_in_flight_ = false;
    }
}

void MaelstromRaftNode::note_read_index_peer_ack_unlocked(int64_t rpc_msg_id, Term response_term) {
    auto map_it = rpc_msg_to_read_id_.find(rpc_msg_id);
    if (map_it == rpc_msg_to_read_id_.end()) {
        return;
    }
    const int64_t read_id = map_it->second;
    rpc_msg_to_read_id_.erase(map_it);

    auto read_it = pending_reads_.find(read_id);
    if (read_it == pending_reads_.end()) {
        return;
    }
    PendingRead& read = read_it->second;
    read.outstanding_rpc_ids.erase(rpc_msg_id);
    if (response_term == read.read_term) {
        read.acks++;
    }
}

void MaelstromRaftNode::reply_read_value_unlocked(const PendingRead& read) {
    Value value;
    Status status = state_machine_->get(read.key, value);
    Json extra = Json::object();
    if (status.code() == StatusCode::KEY_NOT_FOUND) {
        extra["value"] = Json(nullptr);
    } else if (status.fail()) {
        reply_error(read.dest, read.in_reply_to, 13, status.to_string());
        return;
    } else {
        extra["value"] = value_from_string(value);
    }
    reply_ok(read.dest, read.in_reply_to, "read_ok", std::move(extra));
}

void MaelstromRaftNode::fail_all_pending_reads_unlocked(const std::string& reason) {
    for (auto& [id, read] : pending_reads_) {
        (void)id;
        for (int64_t rpc_id : read.outstanding_rpc_ids) {
            rpc_msg_to_read_id_.erase(rpc_id);
        }
        reply_error(read.dest, read.in_reply_to, kErrTemporarilyUnavailable, reason);
    }
    pending_reads_.clear();
}

void MaelstromRaftNode::complete_pending_reads_unlocked() {
    if (!core_) return;

    constexpr auto kReadTimeout = std::chrono::seconds(2);
    const auto now = std::chrono::steady_clock::now();

    if (!core_->is_leader()) {
        fail_all_pending_reads_unlocked("not leader");
        return;
    }

    std::vector<int64_t> done_ids;
    std::vector<int64_t> fail_ids;
    for (auto& [id, read] : pending_reads_) {
        if (now - read.start > kReadTimeout) {
            fail_ids.push_back(id);
            continue;
        }
        if (core_->current_term() != read.read_term) {
            fail_ids.push_back(id);
            continue;
        }
        if (read.acks < read.quorum) {
            continue;
        }
        if (state_machine_->apply_index() < read.read_index && core_->last_applied() < read.read_index) {
            continue;
        }
        if (state_machine_->apply_index() < read.read_index) {
            continue;
        }
        reply_read_value_unlocked(read);
        done_ids.push_back(id);
    }

    for (int64_t id : fail_ids) {
        auto it = pending_reads_.find(id);
        if (it == pending_reads_.end()) continue;
        for (int64_t rpc_id : it->second.outstanding_rpc_ids) {
            rpc_msg_to_read_id_.erase(rpc_id);
        }
        reply_error(it->second.dest, it->second.in_reply_to, kErrTemporarilyUnavailable,
                    "read index timeout or term changed");
        pending_reads_.erase(it);
    }
    for (int64_t id : done_ids) {
        auto it = pending_reads_.find(id);
        if (it == pending_reads_.end()) continue;
        for (int64_t rpc_id : it->second.outstanding_rpc_ids) {
            rpc_msg_to_read_id_.erase(rpc_id);
        }
        pending_reads_.erase(it);
    }
}

std::vector<int64_t> MaelstromRaftNode::send_raft_messages_unlocked(std::vector<storage::RaftMessage> messages) {
    std::vector<int64_t> sent_ids;
    sent_ids.reserve(messages.size());
    for (storage::RaftMessage& message : messages) {
        const std::string dest =
                message.target.node_id.empty() ? node_of(message.target.replica_id) : message.target.node_id;
        Json body = Json::object();
        const int64_t msg_id = next_msg_id_unlocked();
        body["msg_id"] = msg_id;

        PendingRpc pending;
        pending.target = message.target.replica_id;
        pending.type = message.type;

        switch (message.type) {
            case storage::RaftMessageType::REQUEST_VOTE: {
                body["type"] = "raft_request_vote";
                body["term"] = message.vote_param.term;
                body["last_log_index"] = message.vote_param.last_log_index;
                body["last_log_term"] = message.vote_param.last_log_term;
                break;
            }
            case storage::RaftMessageType::PRE_VOTE: {
                body["type"] = "raft_pre_vote";
                body["term"] = message.pre_vote_param.term;
                body["last_log_index"] = message.pre_vote_param.last_log_index;
                body["last_log_term"] = message.pre_vote_param.last_log_term;
                break;
            }
            case storage::RaftMessageType::APPEND_ENTRIES: {
                body["type"] = "raft_append_entries";
                Json encoded = encode_append_param(message.append_param);
                for (const auto& [field, field_value] : encoded.items()) {
                    body[field] = field_value;
                }
                pending.append = message.append_param;
                break;
            }
            case storage::RaftMessageType::INSTALL_SNAPSHOT: {
                body["type"] = "raft_install_snapshot";
                Json encoded = encode_snapshot_param(message.snapshot_param);
                for (const auto& [field, field_value] : encoded.items()) {
                    body[field] = field_value;
                }
                pending.snapshot = message.snapshot_param;
                break;
            }
        }

        pending_rpcs_[msg_id] = std::move(pending);
        sent_ids.push_back(msg_id);

        Json msg = Json::object();
        msg["src"] = node_id_;
        msg["dest"] = dest;
        msg["body"] = std::move(body);
        send_raw(std::move(msg));
    }
    return sent_ids;
}

void MaelstromRaftNode::send_raw(Json msg) {
    const std::string line = msg.dump();
    std::fwrite(line.data(), 1, line.size(), stdout);
    std::fputc('\n', stdout);
    std::fflush(stdout);
}

void MaelstromRaftNode::reply_ok(const std::string& dest, int64_t in_reply_to, const std::string& type, Json extra) {
    Json body = std::move(extra);
    body["type"] = type;
    body["msg_id"] = next_msg_id_unlocked();
    body["in_reply_to"] = in_reply_to;

    Json msg = Json::object();
    msg["src"] = node_id_;
    msg["dest"] = dest;
    msg["body"] = std::move(body);
    send_raw(std::move(msg));
}

void MaelstromRaftNode::reply_error(const std::string& dest, int64_t in_reply_to, int32_t code,
                                    const std::string& text) {
    Json body = Json::object();
    body["type"] = "error";
    body["msg_id"] = next_msg_id_unlocked();
    body["in_reply_to"] = in_reply_to;
    body["code"] = code;
    body["text"] = text;

    Json msg = Json::object();
    msg["src"] = node_id_;
    msg["dest"] = dest;
    msg["body"] = std::move(body);
    send_raw(std::move(msg));
}

int64_t MaelstromRaftNode::next_msg_id_unlocked() { return next_msg_id_++; }

ReplicaID MaelstromRaftNode::replica_of(const std::string& node_id) const {
    return ReplicaID{kTableId, kShardIndex, node_to_seq_.at(node_id)};
}

std::string MaelstromRaftNode::node_of(const ReplicaID& replica_id) const {
    return seq_to_node_.at(replica_id.replica_seq);
}

PeerMember MaelstromRaftNode::peer_of(const std::string& node_id) const {
    PeerMember member;
    member.node_id = node_id;
    member.replica_id = replica_of(node_id);
    member.endpoint = Endpoint{"127.0.0.1", 9000 + member.replica_id.replica_seq};
    return member;
}

Json MaelstromRaftNode::encode_log_entry(const storage::LogEntry& entry) const {
    Json json = Json::object();
    json["term"] = entry.term;
    json["index"] = entry.index;
    json["op_type"] = static_cast<int64_t>(entry.op_type);
    json["key"] = entry.key;
    json["value"] = entry.value;
    return json;
}

storage::LogEntry MaelstromRaftNode::decode_log_entry(const Json& json) const {
    storage::LogEntry entry;
    entry.term = json.at("term").get<int64_t>();
    entry.index = json.at("index").get<int64_t>();
    entry.op_type = static_cast<storage::WriteOpType>(json.at("op_type").get<int64_t>());
    entry.key = json.at("key").get<std::string>();
    entry.value = json.at("value").get<std::string>();
    return entry;
}

Json MaelstromRaftNode::encode_append_param(const storage::AppendEntriesParam& param) const {
    Json json = Json::object();
    json["term"] = param.term;
    json["prev_log_index"] = param.prev_log_index;
    json["prev_log_term"] = param.prev_log_term;
    json["leader_commit"] = param.leader_commit;
    Json entries = Json::array();
    for (const storage::LogEntry& entry : param.entries) {
        entries.push_back(encode_log_entry(entry));
    }
    json["entries"] = Json(std::move(entries));
    return json;
}

storage::AppendEntriesParam MaelstromRaftNode::decode_append_param(const Json& body) const {
    storage::AppendEntriesParam param;
    param.term = body.at("term").get<int64_t>();
    param.prev_log_index = body.at("prev_log_index").get<int64_t>();
    param.prev_log_term = body.at("prev_log_term").get<int64_t>();
    param.leader_commit = body.at("leader_commit").get<int64_t>();
    for (const Json& entry_json : body.at("entries")) {
        param.entries.push_back(decode_log_entry(entry_json));
    }
    return param;
}

Json MaelstromRaftNode::encode_snapshot_param(const storage::InstallSnapshotParam& param) const {
    Json json = Json::object();
    json["term"] = param.term;
    json["snapshot_index"] = param.snapshot_index;
    json["snapshot_term"] = param.snapshot_term;
    json["offset"] = static_cast<int64_t>(param.offset);
    json["data_b64"] = b64_encode(param.data);
    json["done"] = param.done;
    return json;
}

storage::InstallSnapshotParam MaelstromRaftNode::decode_snapshot_param(const Json& body) const {
    storage::InstallSnapshotParam param;
    param.term = body.at("term").get<int64_t>();
    param.snapshot_index = body.at("snapshot_index").get<int64_t>();
    param.snapshot_term = body.at("snapshot_term").get<int64_t>();
    param.offset = static_cast<uint64_t>(body.at("offset").get<int64_t>());
    param.data = b64_decode(body.at("data_b64").get<std::string>());
    param.done = body.at("done").get<bool>();
    return param;
}

std::string MaelstromRaftNode::key_to_string(const Json& key) {
    if (key.is_string()) return key.get<std::string>();
    if (key.is_number()) return std::to_string(key.get<int64_t>());
    return key.dump();
}

Json MaelstromRaftNode::value_from_string(const std::string& value) {
    if (!value.empty() && (std::isdigit(static_cast<unsigned char>(value[0])) || value[0] == '-')) {
        try {
            size_t pos = 0;
            long long parsed = std::stoll(value, &pos);
            if (pos == value.size()) return Json(static_cast<int64_t>(parsed));
        } catch (...) {
        }
    }
    return Json(value);
}

std::string MaelstromRaftNode::value_to_string(const Json& value) {
    if (value.is_null()) return "";
    if (value.is_string()) return value.get<std::string>();
    if (value.is_boolean()) return value.get<bool>() ? "true" : "false";
    if (value.is_number()) {
        return std::to_string(value.get<int64_t>());
    }
    return value.dump();
}

}  // namespace adviskv::maelstrom
