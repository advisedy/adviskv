#include "test/storage/raft/core/raft_test_harness.h"

#include <algorithm>
#include <sstream>
#include <stdexcept>
#include <type_traits>

#include <fmt/format.h>

namespace adviskv::storage::test {
namespace {

void require_node_id(TestNodeId node, int size) {
    if (node < 0 || node >= size) {
        throw std::out_of_range(fmt::format("raft test node {} outside [0, {})", node, size));
    }
}

void require_ok(const Status& status, const std::string& operation) {
    if (status.fail()) {
        throw std::runtime_error(fmt::format("{} failed: {}", operation, status.to_string()));
    }
}

}  // namespace

RaftTestMessageKind RaftTestEnvelope::kind() const {
    if (std::holds_alternative<VoteRequestPayload>(payload)) return RaftTestMessageKind::VOTE_REQUEST;
    if (std::holds_alternative<VoteResponsePayload>(payload)) return RaftTestMessageKind::VOTE_RESPONSE;
    if (std::holds_alternative<AppendRequestPayload>(payload)) return RaftTestMessageKind::APPEND_REQUEST;
    if (std::holds_alternative<AppendResponsePayload>(payload)) return RaftTestMessageKind::APPEND_RESPONSE;
    if (std::holds_alternative<SnapshotRequestPayload>(payload)) return RaftTestMessageKind::SNAPSHOT_REQUEST;
    return RaftTestMessageKind::SNAPSHOT_RESPONSE;
}

std::string RaftTestEnvelope::describe() const {
    return std::visit(
            [&](const auto& value) -> std::string {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, VoteRequestPayload>) {
                    return fmt::format("#{} {}->{} vote-request term={} log={}:{}", id, from, to, value.request.term,
                                       value.request.last_log_index, value.request.last_log_term);
                } else if constexpr (std::is_same_v<T, VoteResponsePayload>) {
                    return fmt::format("#{} {}->{} vote-response term={} granted={}", id, from, to, value.response.term,
                                       value.response.vote_granted);
                } else if constexpr (std::is_same_v<T, AppendRequestPayload>) {
                    return fmt::format("#{} {}->{} append-request term={} prev={}:{} entries={} commit={}", id, from,
                                       to, value.request.term, value.request.prev_log_index,
                                       value.request.prev_log_term, value.request.entries.size(),
                                       value.request.leader_commit);
                } else if constexpr (std::is_same_v<T, AppendResponsePayload>) {
                    return fmt::format("#{} {}->{} append-response term={} success={} last={}", id, from, to,
                                       value.response.term, value.response.success, value.response.last_log_index);
                } else if constexpr (std::is_same_v<T, SnapshotRequestPayload>) {
                    return fmt::format("#{} {}->{} snapshot-request term={} snapshot={}:{}", id, from, to,
                                       value.request.term, value.request.snapshot_index, value.request.snapshot_term);
                } else {
                    return fmt::format("#{} {}->{} snapshot-response term={} status={} watermark={}", id, from, to,
                                       value.response.term, static_cast<int>(value.response.status.code()),
                                       value.response.snapshot_watermark);
                }
            },
            payload);
}

RaftTestCluster RaftTestCluster::voters(int voter_count, int table_id) {
    if (voter_count <= 0) throw std::invalid_argument("voter_count must be positive");

    RaftTestCluster cluster;
    std::vector<PeerMember> members;
    members.reserve(voter_count);
    for (int i = 0; i < voter_count; ++i) {
        members.push_back(make_test_member(table_id, i));
    }
    for (const PeerMember& member : members) {
        RaftTestNodeSpec spec;
        spec.self = member;
        spec.initial_voters = members;
        cluster.add_node(std::move(spec));
    }
    return cluster;
}

TestNodeId RaftTestCluster::add_node(RaftTestNodeSpec spec) {
    if (find_node(spec.self.replica_id) >= 0) {
        throw std::invalid_argument("duplicate raft test replica id");
    }
    if (spec.election_ticks <= 0 || spec.heartbeat_ticks <= 0) {
        throw std::invalid_argument("raft test timing must be positive");
    }

    int32_t election_ticks = spec.election_ticks;
    RaftCoreTimingConfig timing;
    timing.heartbeat_ticks = spec.heartbeat_ticks;
    timing.next_election_timeout = [election_ticks]() { return election_ticks; };

    Node new_node;
    new_node.self = spec.self;
    new_node.core = std::make_unique<RaftCore>(spec.self.replica_id, spec.initial_voters, std::move(timing));
    new_node.election_ticks = election_ticks;
    new_node.kv = std::move(spec.kv);

    if (spec.snapshot.has_value()) {
        const RaftTestSnapshot& snapshot = *spec.snapshot;
        new_node.core->install_local_snapshot(InstallSnapshotContext{snapshot.index, snapshot.term, snapshot.members});
        new_node.snapshots[snapshot.index] = snapshot;
        new_node.kv = snapshot.kv;
    }
    new_node.core->update_raft_meta(spec.hard_state);
    new_node.core->update_log_entries(spec.entries);
    if (spec.membership.has_value()) {
        new_node.core->update_membership(*spec.membership);
    }
    if (spec.recovering) {
        new_node.core->enter_recovering();
    }

    nodes_.push_back(std::move(new_node));
    return static_cast<TestNodeId>(nodes_.size() - 1);
}

int RaftTestCluster::node_count() const { return static_cast<int>(nodes_.size()); }

RaftCore& RaftTestCluster::core(TestNodeId id) { return *node(id).core; }

const RaftCore& RaftTestCluster::core(TestNodeId id) const { return *node(id).core; }

const PeerMember& RaftTestCluster::member(TestNodeId id) const { return node(id).self; }

ReplicaID RaftTestCluster::replica_id(TestNodeId id) const { return member(id).replica_id; }

TestNodeId RaftTestCluster::find_node(const ReplicaID& replica_id) const {
    for (TestNodeId i = 0; i < static_cast<TestNodeId>(nodes_.size()); ++i) {
        if (nodes_[i].self.replica_id == replica_id) return i;
    }
    return -1;
}

const std::map<Key, Value>& RaftTestCluster::kv(TestNodeId id) const { return node(id).kv; }

const RaftEffects& RaftTestCluster::last_effects(TestNodeId id) const { return node(id).last_effects; }

void RaftTestCluster::campaign(TestNodeId id) {
    Node& candidate = node(id);
    const Term old_term = candidate.core->current_term();
    for (int32_t i = 0; i <= candidate.election_ticks; ++i) {
        tick(id);
        if (candidate.core->current_term() > old_term) return;
    }
    throw std::runtime_error(fmt::format("node {} did not campaign after {} ticks", id, candidate.election_ticks));
}

void RaftTestCluster::tick(TestNodeId id) {
    RaftEffects effects;
    node(id).core->tick(effects);
    accept_effects(id, std::move(effects));
}

void RaftTestCluster::tick_all() {
    for (TestNodeId id = 0; id < static_cast<TestNodeId>(nodes_.size()); ++id)
        tick(id);
}

RaftTestProposalResult RaftTestCluster::propose(TestNodeId id, const ProposeParam& proposal) {
    RaftEffects effects;
    auto [status, index] = node(id).core->propose(proposal, effects);
    accept_effects(id, std::move(effects));
    return {status, index};
}

std::vector<RaftTestProposalResult> RaftTestCluster::propose_batch(TestNodeId id,
                                                                   const std::vector<ProposeParam>& proposals) {
    RaftEffects effects;
    std::vector<std::pair<Status, LogIndex>> core_results = node(id).core->propose_batch(proposals, effects);
    accept_effects(id, std::move(effects));

    std::vector<RaftTestProposalResult> results;
    results.reserve(core_results.size());
    for (auto& [status, index] : core_results) {
        results.push_back(RaftTestProposalResult{std::move(status), index});
    }
    return results;
}

Status RaftTestCluster::ensure_add_learner(TestNodeId leader, const PeerMember& learner) {
    RaftEffects effects;
    Status status = node(leader).core->ensure_add_learner(learner, effects);
    accept_effects(leader, std::move(effects));
    return status;
}

Status RaftTestCluster::ensure_remove_member(TestNodeId leader, const ReplicaID& replica_id) {
    RaftEffects effects;
    Status status = node(leader).core->ensure_remove_member(replica_id, effects);
    accept_effects(leader, std::move(effects));
    return status;
}

void RaftTestCluster::apply(TestNodeId id) {
    Node& target = node(id);
    std::vector<LogEntry> entries = target.core->extract_committed_entries();
    for (const LogEntry& entry : entries) {
        if (is_config_change_op(entry.op_type)) {
            require_ok(target.core->apply_config_entry(entry), "apply config entry");
            continue;
        }
        if (entry.op_type == WriteOpType::PUT) {
            target.kv[entry.key] = entry.value;
        } else if (entry.op_type == WriteOpType::DEL) {
            target.kv.erase(entry.key);
        }
        target.core->advance_last_applied(entry.index);
    }
}

void RaftTestCluster::apply_all() {
    for (TestNodeId id = 0; id < static_cast<TestNodeId>(nodes_.size()); ++id)
        apply(id);
}

void RaftTestCluster::compact(TestNodeId id, LogIndex index) {
    Node& target = node(id);
    if (index != target.core->last_applied()) {
        throw std::invalid_argument("raft test snapshots must capture the current applied state");
    }
    require_ok(target.core->truncate_log(index), "compact raft log");
    const LogIndex snapshot_index = target.core->snapshot_index();
    const Term snapshot_term = target.core->snapshot_term();
    target.snapshots[snapshot_index] =
            RaftTestSnapshot{snapshot_index, snapshot_term, target.core->raft_members(), target.kv};
}

void RaftTestCluster::enter_recovering(TestNodeId id) { node(id).core->enter_recovering(); }

const std::vector<RaftTestEnvelope>& RaftTestCluster::pending() const { return pending_; }

std::vector<TestMessageId> RaftTestCluster::message_ids(RaftTestMessageKind kind, std::optional<TestNodeId> from,
                                                        std::optional<TestNodeId> to) const {
    std::vector<TestMessageId> result;
    for (const RaftTestEnvelope& envelope : pending_) {
        if (envelope.kind() != kind) continue;
        if (from.has_value() && envelope.from != *from) continue;
        if (to.has_value() && envelope.to != *to) continue;
        result.push_back(envelope.id);
    }
    return result;
}

std::optional<TestMessageId> RaftTestCluster::first_message(RaftTestMessageKind kind, std::optional<TestNodeId> from,
                                                            std::optional<TestNodeId> to) const {
    std::vector<TestMessageId> ids = message_ids(kind, from, to);
    if (ids.empty()) return std::nullopt;
    return ids.front();
}

size_t RaftTestCluster::pending_count(RaftTestMessageKind kind, std::optional<TestNodeId> from,
                                      std::optional<TestNodeId> to) const {
    return message_ids(kind, from, to).size();
}

TestMessageId RaftTestCluster::enqueue(RaftTestEnvelope envelope) {
    envelope.id = next_message_id_++;
    TestMessageId id = envelope.id;
    record("enqueue " + envelope.describe());
    pending_.push_back(std::move(envelope));
    return id;
}

std::optional<RaftTestEnvelope> RaftTestCluster::take(TestMessageId id) {
    auto it = std::find_if(pending_.begin(), pending_.end(), [id](const auto& envelope) { return envelope.id == id; });
    if (it == pending_.end()) return std::nullopt;
    RaftTestEnvelope result = std::move(*it);
    pending_.erase(it);
    record("take " + result.describe());
    return result;
}

Status RaftTestCluster::deliver(TestMessageId id) {
    std::optional<RaftTestEnvelope> envelope = take(id);
    if (!envelope.has_value()) return Status::INVALID_ARGUMENT("unknown raft test message");
    return deliver(std::move(*envelope));
}

Status RaftTestCluster::deliver(RaftTestEnvelope envelope) {
    if (blocked(envelope.from, envelope.to)) {
        record("blocked-drop " + envelope.describe());
        return Status::OK();
    }
    record("deliver " + envelope.describe());
    return deliver_unblocked(envelope);
}

void RaftTestCluster::deliver_all(size_t max_events) {
    size_t delivered = 0;
    while (!pending_.empty()) {
        if (delivered++ >= max_events) {
            throw std::runtime_error("raft test delivery exceeded limit\n" + trace());
        }
        Status status = deliver(pending_.front().id);
        if (status.code() == StatusCode::INVALID_ARGUMENT) {
            throw std::runtime_error(status.to_string());
        }
    }
}

bool RaftTestCluster::drop(TestMessageId id) {
    std::optional<RaftTestEnvelope> envelope = take(id);
    if (!envelope.has_value()) return false;
    record("drop " + envelope->describe());
    return true;
}

TestMessageId RaftTestCluster::duplicate(TestMessageId id) {
    auto it = std::find_if(pending_.begin(), pending_.end(), [id](const auto& envelope) { return envelope.id == id; });
    if (it == pending_.end()) throw std::invalid_argument("unknown raft test message");
    RaftTestEnvelope copy = *it;
    return enqueue(std::move(copy));
}

void RaftTestCluster::drop_all() {
    while (!pending_.empty())
        drop(pending_.back().id);
}

Status RaftTestCluster::fail(TestMessageId id, const Status& status) {
    std::optional<RaftTestEnvelope> envelope = take(id);
    if (!envelope.has_value()) return Status::INVALID_ARGUMENT("unknown raft test message");
    record("fail " + envelope->describe());

    if (const auto* append = std::get_if<AppendRequestPayload>(&envelope->payload)) {
        node(envelope->from).core->handle_append_send_failed(replica_id(envelope->to), append->request, status);
    } else if (const auto* snapshot = std::get_if<SnapshotRequestPayload>(&envelope->payload)) {
        node(envelope->from)
                .core->handle_install_snapshot_send_failed(replica_id(envelope->to), snapshot->request, status);
    }
    return Status::OK();
}

void RaftTestCluster::cut(TestNodeId from, TestNodeId to) { blocked_links_.insert(link_key(from, to)); }

void RaftTestCluster::isolate(TestNodeId id) {
    require_node_id(id, node_count());
    for (TestNodeId other = 0; other < static_cast<TestNodeId>(nodes_.size()); ++other) {
        if (other == id) continue;
        cut(id, other);
        cut(other, id);
    }
}

void RaftTestCluster::heal(TestNodeId from, TestNodeId to) { blocked_links_.erase(link_key(from, to)); }

void RaftTestCluster::heal_all() { blocked_links_.clear(); }

std::string RaftTestCluster::trace() const {
    std::ostringstream out;
    for (const std::string& event : trace_)
        out << event << '\n';
    return out.str();
}

uint64_t RaftTestCluster::link_key(TestNodeId from, TestNodeId to) {
    return (static_cast<uint64_t>(static_cast<uint32_t>(from)) << 32U) | static_cast<uint32_t>(to);
}

bool RaftTestCluster::blocked(TestNodeId from, TestNodeId to) const {
    return blocked_links_.count(link_key(from, to)) > 0;
}

RaftTestCluster::Node& RaftTestCluster::node(TestNodeId id) {
    require_node_id(id, node_count());
    return nodes_[id];
}

const RaftTestCluster::Node& RaftTestCluster::node(TestNodeId id) const {
    require_node_id(id, node_count());
    return nodes_[id];
}

void RaftTestCluster::accept_effects(TestNodeId source, RaftEffects effects) {
    if (effects.entries_to_rewrite.has_value() && !effects.entries_to_append.empty()) {
        throw std::runtime_error("RaftEffects cannot append and rewrite in the same step");
    }
    node(source).last_effects = effects;
    for (const RaftMessage& message : effects.messages) {
        enqueue(request_envelope(source, message));
    }
}

RaftTestEnvelope RaftTestCluster::request_envelope(TestNodeId source, const RaftMessage& message) const {
    const TestNodeId target = find_node(message.target.replica_id);
    if (target < 0) {
        throw std::runtime_error("raft message targets a node absent from the test cluster");
    }

    RaftTestEnvelope envelope;
    envelope.from = source;
    envelope.to = target;
    switch (message.type) {
        case RaftMessageType::REQUEST_VOTE:
            envelope.payload = VoteRequestPayload{message.vote_param};
            break;
        case RaftMessageType::APPEND_ENTRIES:
            envelope.payload = AppendRequestPayload{message.append_param};
            break;
        case RaftMessageType::INSTALL_SNAPSHOT: {
            const Node& source_node = node(source);
            auto it = source_node.snapshots.find(message.snapshot_param.snapshot_index);
            if (it == source_node.snapshots.end()) {
                throw std::runtime_error(fmt::format("node {} has no snapshot image at index {}", source,
                                                     message.snapshot_param.snapshot_index));
            }
            RaftTestSnapshot image = it->second;
            if (image.term != message.snapshot_param.snapshot_term) {
                throw std::runtime_error(fmt::format("node {} snapshot term mismatch at index {}: image={}, message={}",
                                                     source, image.index, image.term,
                                                     message.snapshot_param.snapshot_term));
            }
            envelope.payload = SnapshotRequestPayload{message.snapshot_param, std::move(image)};
            break;
        }
    }
    return envelope;
}

Status RaftTestCluster::deliver_unblocked(const RaftTestEnvelope& envelope) {
    return std::visit(
            [&](const auto& value) -> Status {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, VoteRequestPayload>) {
                    RequestVoteResult result;
                    RaftEffects effects;
                    node(envelope.to).core->handle_request_vote(value.request, result, effects);
                    accept_effects(envelope.to, std::move(effects));
                    enqueue(RaftTestEnvelope{0, envelope.to, envelope.from,
                                             VoteResponsePayload{replica_id(envelope.to), result}});
                    return Status::OK();
                } else if constexpr (std::is_same_v<T, VoteResponsePayload>) {
                    RaftEffects effects;
                    node(envelope.to).core->handle_vote_response(value.from, value.response, effects);
                    accept_effects(envelope.to, std::move(effects));
                    return Status::OK();
                } else if constexpr (std::is_same_v<T, AppendRequestPayload>) {
                    AppendEntriesResult result;
                    RaftEffects effects;
                    node(envelope.to).core->handle_append_entries(value.request, result, effects);
                    accept_effects(envelope.to, std::move(effects));
                    enqueue(RaftTestEnvelope{0, envelope.to, envelope.from,
                                             AppendResponsePayload{replica_id(envelope.to), value.request, result}});
                    return Status::OK();
                } else if constexpr (std::is_same_v<T, AppendResponsePayload>) {
                    RaftEffects effects;
                    Status status =
                            node(envelope.to)
                                    .core->handle_append_response(value.from, value.request, value.response, effects);
                    accept_effects(envelope.to, std::move(effects));
                    return status;
                } else if constexpr (std::is_same_v<T, SnapshotRequestPayload>) {
                    RaftEffects prepare_effects;
                    Status status = node(envelope.to).core->prepare_install_snapshot(value.request, prepare_effects);
                    accept_effects(envelope.to, std::move(prepare_effects));

                    InstallSnapshotResult result;
                    result.term = node(envelope.to).core->current_term();
                    result.status = status;
                    if (status.ok()) {
                        RaftEffects install_effects;
                        node(envelope.to)
                                .core->commit_install_snapshot(
                                        InstallSnapshotContext{value.snapshot.index, value.snapshot.term,
                                                               value.snapshot.members},
                                        install_effects);
                        node(envelope.to).kv = value.snapshot.kv;
                        node(envelope.to).snapshots[value.snapshot.index] = value.snapshot;
                        accept_effects(envelope.to, std::move(install_effects));
                    } else if (status.code() == StatusCode::ALREADY_EXIST) {
                        result.snapshot_watermark =
                                std::max(value.request.snapshot_index, node(envelope.to).core->snapshot_index());
                    }
                    enqueue(RaftTestEnvelope{0, envelope.to, envelope.from,
                                             SnapshotResponsePayload{replica_id(envelope.to), value.request, result}});
                    return Status::OK();
                } else {
                    RaftEffects effects;
                    node(envelope.to)
                            .core->handle_install_snapshot_response(value.from, value.request, value.response, effects);
                    accept_effects(envelope.to, std::move(effects));
                    return Status::OK();
                }
            },
            envelope.payload);
}

void RaftTestCluster::record(std::string event) { trace_.push_back(std::move(event)); }

PeerMember make_test_member(int table_id, int replica_seq) {
    return PeerMember{fmt::format("node-{}", replica_seq), ReplicaID{table_id, 0, replica_seq},
                      Endpoint{"127.0.0.1", 19000 + replica_seq}};
}

LogEntry make_test_entry(Term term, LogIndex index, WriteOpType op, Key key, Value value) {
    return LogEntry{term, index, op, std::move(key), std::move(value)};
}

}  // namespace adviskv::storage::test
