#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "storage/raft/core/raft_core.h"

namespace adviskv::storage::test {

using TestNodeId = int32_t;
using TestMessageId = uint64_t;

struct RaftTestSnapshot {
    LogIndex index{0};
    Term term{0};
    std::vector<RaftMember> members;
    std::map<Key, Value> kv;
};

struct RaftTestNodeSpec {
    PeerMember self;
    std::vector<PeerMember> initial_voters;
    RaftMeta hard_state;
    std::vector<LogEntry> entries;
    std::optional<std::vector<RaftMember>> membership;
    std::optional<RaftTestSnapshot> snapshot;
    std::map<Key, Value> kv;
    bool recovering{false};
    int32_t election_ticks{10};
    int32_t heartbeat_ticks{1};
};

struct VoteRequestPayload {
    RequestVoteParam request;
};

struct VoteResponsePayload {
    ReplicaID from;
    RequestVoteResult response;
};

struct AppendRequestPayload {
    AppendEntriesParam request;
};

struct AppendResponsePayload {
    ReplicaID from;
    AppendEntriesParam request;
    AppendEntriesResult response;
};

struct SnapshotRequestPayload {
    InstallSnapshotParam request;
    RaftTestSnapshot snapshot;
};

struct SnapshotResponsePayload {
    ReplicaID from;
    InstallSnapshotParam request;
    InstallSnapshotResult response;
};

using RaftTestPayload = std::variant<VoteRequestPayload, VoteResponsePayload, AppendRequestPayload,
                                     AppendResponsePayload, SnapshotRequestPayload, SnapshotResponsePayload>;

enum class RaftTestMessageKind : uint8_t {
    VOTE_REQUEST,
    VOTE_RESPONSE,
    APPEND_REQUEST,
    APPEND_RESPONSE,
    SNAPSHOT_REQUEST,
    SNAPSHOT_RESPONSE,
};

struct RaftTestEnvelope {
    TestMessageId id{0};
    TestNodeId from{-1};
    TestNodeId to{-1};
    RaftTestPayload payload;

    RaftTestMessageKind kind() const;
    std::string describe() const;
};

struct RaftTestProposalResult {
    Status status;
    LogIndex index{-1};
};

class RaftTestCluster {
public:
    static RaftTestCluster voters(int voter_count, int table_id = 700);

    TestNodeId add_node(RaftTestNodeSpec spec);

    int node_count() const;
    RaftCore& core(TestNodeId node);
    const RaftCore& core(TestNodeId node) const;
    const PeerMember& member(TestNodeId node) const;
    ReplicaID replica_id(TestNodeId node) const;
    TestNodeId find_node(const ReplicaID& replica_id) const;
    const std::map<Key, Value>& kv(TestNodeId node) const;
    const RaftEffects& last_effects(TestNodeId node) const;

    void campaign(TestNodeId node);
    void tick(TestNodeId node);
    void tick_all();
    RaftTestProposalResult propose(TestNodeId node, const ProposeParam& proposal);
    std::vector<RaftTestProposalResult> propose_batch(TestNodeId node, const std::vector<ProposeParam>& proposals);
    Status ensure_add_learner(TestNodeId leader, const PeerMember& learner);
    Status ensure_remove_member(TestNodeId leader, const ReplicaID& replica_id);
    void apply(TestNodeId node);
    void apply_all();
    void compact(TestNodeId node, LogIndex index);
    void enter_recovering(TestNodeId node);

    const std::vector<RaftTestEnvelope>& pending() const;
    std::vector<TestMessageId> message_ids(RaftTestMessageKind kind, std::optional<TestNodeId> from = std::nullopt,
                                           std::optional<TestNodeId> to = std::nullopt) const;
    std::optional<TestMessageId> first_message(RaftTestMessageKind kind, std::optional<TestNodeId> from = std::nullopt,
                                               std::optional<TestNodeId> to = std::nullopt) const;
    size_t pending_count(RaftTestMessageKind kind, std::optional<TestNodeId> from = std::nullopt,
                         std::optional<TestNodeId> to = std::nullopt) const;

    TestMessageId enqueue(RaftTestEnvelope envelope);
    std::optional<RaftTestEnvelope> take(TestMessageId id);
    Status deliver(TestMessageId id);
    Status deliver(RaftTestEnvelope envelope);
    void deliver_all(size_t max_events = 10000);
    bool drop(TestMessageId id);
    TestMessageId duplicate(TestMessageId id);
    void drop_all();
    Status fail(TestMessageId id, const Status& status);

    void cut(TestNodeId from, TestNodeId to);
    void isolate(TestNodeId node);
    void heal(TestNodeId from, TestNodeId to);
    void heal_all();

    std::string trace() const;

private:
    struct Node {
        PeerMember self;
        std::unique_ptr<RaftCore> core;
        std::map<Key, Value> kv;
        std::map<LogIndex, RaftTestSnapshot> snapshots;
        RaftEffects last_effects;
        int32_t election_ticks{10};
    };

    static uint64_t link_key(TestNodeId from, TestNodeId to);
    bool blocked(TestNodeId from, TestNodeId to) const;
    Node& node(TestNodeId node);
    const Node& node(TestNodeId node) const;
    void accept_effects(TestNodeId source, RaftEffects effects);
    RaftTestEnvelope request_envelope(TestNodeId source, const RaftMessage& message) const;
    Status deliver_unblocked(const RaftTestEnvelope& envelope);
    void record(std::string event);

    std::vector<Node> nodes_;
    std::vector<RaftTestEnvelope> pending_;
    std::set<uint64_t> blocked_links_;
    std::vector<std::string> trace_;
    TestMessageId next_message_id_{1};
};

PeerMember make_test_member(int table_id, int replica_seq);
LogEntry make_test_entry(Term term, LogIndex index, WriteOpType op = WriteOpType::PUT, Key key = {}, Value value = {});

}  // namespace adviskv::storage::test
