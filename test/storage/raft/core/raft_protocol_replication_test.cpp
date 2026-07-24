#include <algorithm>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "test/storage/raft/core/raft_test_harness.h"

namespace adviskv::storage::test {
namespace {

const RaftTestEnvelope& pending_message(const RaftTestCluster& cluster, TestMessageId id) {
    auto it = std::find_if(cluster.pending().begin(), cluster.pending().end(),
                           [id](const RaftTestEnvelope& envelope) { return envelope.id == id; });
    if (it == cluster.pending().end()) throw std::runtime_error("raft test message is not pending");
    return *it;
}

const AppendEntriesParam& pending_append(const RaftTestCluster& cluster, TestMessageId id) {
    return std::get<AppendRequestPayload>(pending_message(cluster, id).payload).request;
}

void elect(RaftTestCluster& cluster, TestNodeId candidate = 0) {
    cluster.campaign(candidate);
    cluster.deliver_all();
    ASSERT_EQ(cluster.core(candidate).role(), ReplicaRole::LEADER) << cluster.trace();
}

void deliver_append_round_trip(RaftTestCluster& cluster, TestNodeId leader, TestNodeId follower) {
    std::optional<TestMessageId> request = cluster.first_message(RaftTestMessageKind::APPEND_REQUEST, leader, follower);
    ASSERT_TRUE(request.has_value()) << cluster.trace();
    ASSERT_TRUE(cluster.deliver(*request).ok());
    std::optional<TestMessageId> response =
            cluster.first_message(RaftTestMessageKind::APPEND_RESPONSE, follower, leader);
    ASSERT_TRUE(response.has_value()) << cluster.trace();
    Status status = cluster.deliver(*response);
    ASSERT_NE(status.code(), StatusCode::INVALID_ARGUMENT) << status.to_string();
}

TestMessageId enqueue_append_request(RaftTestCluster& cluster, TestNodeId from, TestNodeId to, Term term,
                                     std::vector<LogEntry> entries, LogIndex prev_index, Term prev_term,
                                     LogIndex leader_commit = 0) {
    AppendEntriesParam request{cluster.replica_id(from),
                               cluster.replica_id(to),
                               term,
                               std::move(entries),
                               prev_index,
                               prev_term,
                               leader_commit};
    return cluster.enqueue(RaftTestEnvelope{0, from, to, AppendRequestPayload{std::move(request)}});
}

TestMessageId enqueue_append_response(RaftTestCluster& cluster, TestNodeId from, TestNodeId to,
                                      AppendEntriesParam request, Term term, bool success,
                                      LogIndex follower_last_index) {
    AppendEntriesResult result{term, success, follower_last_index};
    return cluster.enqueue(
            RaftTestEnvelope{0, from, to, AppendResponsePayload{cluster.replica_id(from), std::move(request), result}});
}

std::vector<LogEntry> entries(std::initializer_list<std::pair<Term, LogIndex>> terms_and_indices) {
    std::vector<LogEntry> result;
    for (const auto& [term, index] : terms_and_indices) {
        result.push_back(make_test_entry(term, index, WriteOpType::PUT, "k" + std::to_string(index),
                                         "v" + std::to_string(term)));
    }
    return result;
}

// Reference: etcd TestLeaderStartReplication; Raft paper section 5.3.
// 场景：启动 3 个空日志节点，节点 0 获得节点 1 的选票成为首任 leader。
// 过程：只投递完成选举所需的投票消息，保留 leader 随后发给两个 follower 的复制请求。
// 预期：leader 先追加当前 term 的 no-op，并向两个 follower 广播同一条日志。
TEST(RaftReplicationProtocolTest, NewLeaderAppendsCurrentTermNoopAndBroadcastsIt) {
    RaftTestCluster cluster = RaftTestCluster::voters(3);
    cluster.campaign(0);

    ASSERT_TRUE(cluster.deliver(*cluster.first_message(RaftTestMessageKind::VOTE_REQUEST, 0, 1)).ok());
    ASSERT_TRUE(cluster.deliver(*cluster.first_message(RaftTestMessageKind::VOTE_RESPONSE, 1, 0)).ok());

    ASSERT_EQ(cluster.core(0).role(), ReplicaRole::LEADER);
    ASSERT_EQ(cluster.core(0).log_entries_for_test().size(), 1U);
    const LogEntry& noop = cluster.core(0).log_entries_for_test().front();
    EXPECT_EQ(noop.index, 1);
    EXPECT_EQ(noop.term, 1);
    EXPECT_EQ(noop.op_type, WriteOpType::NONE);
    EXPECT_EQ(cluster.last_effects(0).entries_to_append, std::vector<LogEntry>{noop});
    EXPECT_EQ(cluster.pending_count(RaftTestMessageKind::APPEND_REQUEST, 0), 2U);

    for (TestMessageId id : cluster.message_ids(RaftTestMessageKind::APPEND_REQUEST, 0)) {
        const AppendEntriesParam& request = pending_append(cluster, id);
        ASSERT_EQ(request.entries.size(), 1U);
        EXPECT_EQ(request.entries.front(), noop);
        EXPECT_EQ(request.prev_log_index, 0);
        EXPECT_EQ(request.prev_log_term, 0);
    }
}

// Reference: etcd TestProposal and TestLeaderStartReplication.
// 场景：3 节点集群已经选出节点 0 为 leader，节点 1 是 follower。
// 过程：先在 follower 上提交写请求，再在 leader 上提交写请求并检查生成的复制消息。
// 预期：follower 拒绝 proposal；leader 追加日志，并从上一条 no-op 后向 follower 复制。
TEST(RaftReplicationProtocolTest, LeaderProposalBuildsAppendEntriesAndFollowerRejectsProposal) {
    RaftTestCluster cluster = RaftTestCluster::voters(3);
    elect(cluster);

    RaftTestProposalResult rejected = cluster.propose(1, ProposeParam::write(WriteOpType::PUT, "follower", "value"));
    EXPECT_EQ(rejected.status.code(), StatusCode::NOT_LEADER);
    EXPECT_EQ(rejected.index, -1);

    RaftTestProposalResult proposed = cluster.propose(0, ProposeParam::write(WriteOpType::PUT, "key", "value"));
    ASSERT_TRUE(proposed.status.ok()) << proposed.status.to_string();
    EXPECT_EQ(proposed.index, 2);
    ASSERT_EQ(cluster.last_effects(0).entries_to_append.size(), 1U);
    EXPECT_EQ(cluster.last_effects(0).entries_to_append.front(),
              make_test_entry(1, 2, WriteOpType::PUT, "key", "value"));

    ASSERT_EQ(cluster.pending_count(RaftTestMessageKind::APPEND_REQUEST, 0), 2U);
    const AppendEntriesParam& request =
            pending_append(cluster, *cluster.first_message(RaftTestMessageKind::APPEND_REQUEST, 0, 1));
    EXPECT_EQ(request.prev_log_index, 1);
    EXPECT_EQ(request.prev_log_term, 1);
    ASSERT_EQ(request.entries.size(), 1U);
    EXPECT_EQ(request.entries.front().index, 2);
}

class CommitQuorumTest : public ::testing::TestWithParam<int> {};

// Reference: etcd TestLeaderCommitEntry; Raft paper sections 5.3 and 5.4.2.
// 场景：分别启动 1、3、5 个 voter，选出节点 0 为 leader 后追加一条写日志。
// 过程：按 follower 编号逐个投递 AppendEntries 及响应，观察每次确认后的 commit index。
// 预期：少于多数派确认时不能提交，恰好达到 voter quorum 后立即提交。
TEST_P(CommitQuorumTest, CommitsOnlyAfterVoterQuorumAcknowledges) {
    const int node_count = GetParam();
    RaftTestCluster cluster = RaftTestCluster::voters(node_count);
    elect(cluster);

    RaftTestProposalResult proposed = cluster.propose(0, ProposeParam::write(WriteOpType::PUT, "quorum", "ok"));
    ASSERT_TRUE(proposed.status.ok());

    const int required_follower_acks = node_count / 2;
    for (int follower = 1; follower <= required_follower_acks; ++follower) {
        if (follower < required_follower_acks) EXPECT_EQ(cluster.core(0).commit_index(), 1);
        deliver_append_round_trip(cluster, 0, follower);
    }

    EXPECT_EQ(cluster.core(0).commit_index(), 2);
}

INSTANTIATE_TEST_SUITE_P(ClusterSizes, CommitQuorumTest, ::testing::Values(1, 3, 5),
                         [](const ::testing::TestParamInfo<int>& info) {
                             return "Nodes" + std::to_string(info.param);
                         });

// Reference: etcd TestLeaderAcknowledgeCommit.
// 场景：3 节点集群中，节点 0 的新日志先由节点 1 确认并形成多数派提交。
// 过程：节点 2 在 leader 提交前已接收日志，随后再接收一次携带新 commit index 的心跳。
// 预期：日志复制本身不会让节点 2 猜测提交状态；后续心跳会把提交位置传播给它。
TEST(RaftReplicationProtocolTest, CommitPropagatesOnLaterHeartbeat) {
    RaftTestCluster cluster = RaftTestCluster::voters(3);
    elect(cluster);
    ASSERT_TRUE(cluster.propose(0, ProposeParam::write(WriteOpType::PUT, "k", "v")).status.ok());

    deliver_append_round_trip(cluster, 0, 1);
    ASSERT_EQ(cluster.core(0).commit_index(), 2);

    deliver_append_round_trip(cluster, 0, 2);
    EXPECT_EQ(cluster.core(2).commit_index(), 1);

    cluster.tick(0);
    deliver_append_round_trip(cluster, 0, 2);
    EXPECT_EQ(cluster.core(2).commit_index(), 2);
}

// 场景：5 节点集群选出节点 0，leader 追加一条写日志。
// 过程：只让节点 1 完成复制确认，其余三个 follower 不响应。
// 预期：leader 虽持有新日志，但只有 2/5 副本，commit index 必须停在此前的 no-op。
TEST(RaftReplicationProtocolTest, MinorityCannotCommitAProposal) {
    RaftTestCluster cluster = RaftTestCluster::voters(5);
    elect(cluster);
    ASSERT_TRUE(cluster.propose(0, ProposeParam::write(WriteOpType::PUT, "minority", "value")).status.ok());

    deliver_append_round_trip(cluster, 0, 1);

    EXPECT_EQ(cluster.core(0).commit_index(), 1);
    EXPECT_EQ(cluster.core(0).last_log_index(), 2);
}

struct FollowerAppendCase {
    const char* name;
    std::vector<LogEntry> initial;
    LogIndex prev_index;
    Term prev_term;
    std::vector<LogEntry> incoming;
    std::vector<LogEntry> expected;
};

void PrintTo(const FollowerAppendCase& value, std::ostream* stream) { *stream << value.name; }

class FollowerAppendTableTest : public ::testing::TestWithParam<FollowerAppendCase> {};

// Reference: etcd TestFollowerAppendEntries and TestFollowerCheckMsgApp.
// 场景：构造具有不同初始日志的 follower，包括空日志、相同前缀和冲突后缀。
// 过程：leader 发送一个 prev index/term 可匹配的 AppendEntries，携带各参数实例的日志片段。
// 预期：follower 接受相同前缀、追加缺失后缀，并从首个 term 冲突处覆盖旧日志。
TEST_P(FollowerAppendTableTest, AcceptsMatchingPrefixAndRewritesConflicts) {
    const FollowerAppendCase& test = GetParam();
    std::vector<PeerMember> members{make_test_member(711, 0), make_test_member(711, 1)};
    RaftTestCluster cluster;
    RaftTestNodeSpec leader;
    leader.self = members[0];
    leader.initial_voters = members;
    cluster.add_node(std::move(leader));
    RaftTestNodeSpec follower;
    follower.self = members[1];
    follower.initial_voters = members;
    follower.hard_state = RaftMeta{3, std::nullopt};
    follower.entries = test.initial;
    cluster.add_node(std::move(follower));

    ASSERT_TRUE(
            cluster.deliver(enqueue_append_request(cluster, 0, 1, 3, test.incoming, test.prev_index, test.prev_term))
                    .ok());
    const TestMessageId response_id = *cluster.first_message(RaftTestMessageKind::APPEND_RESPONSE, 1, 0);
    const AppendEntriesResult& response =
            std::get<AppendResponsePayload>(pending_message(cluster, response_id).payload).response;
    EXPECT_TRUE(response.success);
    EXPECT_EQ(cluster.core(1).log_entries_for_test(), test.expected);
}

INSTANTIATE_TEST_SUITE_P(
        PrefixAndConflictCases, FollowerAppendTableTest,
        ::testing::Values(
                FollowerAppendCase{"EmptyLog", {}, 0, 0, entries({{1, 1}, {1, 2}}), entries({{1, 1}, {1, 2}})},
                FollowerAppendCase{"IdenticalPrefix", entries({{1, 1}, {1, 2}}), 0, 0, entries({{1, 1}, {1, 2}}),
                                   entries({{1, 1}, {1, 2}})},
                FollowerAppendCase{"AppendSuffix", entries({{1, 1}}), 1, 1, entries({{1, 2}, {1, 3}}),
                                   entries({{1, 1}, {1, 2}, {1, 3}})},
                FollowerAppendCase{"RewriteConflict", entries({{1, 1}, {1, 2}, {1, 3}}), 1, 1,
                                   entries({{2, 2}, {2, 3}}), entries({{1, 1}, {2, 2}, {2, 3}})},
                FollowerAppendCase{"AppendNewTermSuffix", entries({{1, 1}, {1, 2}}), 0, 0,
                                   entries({{1, 1}, {1, 2}, {2, 3}}), entries({{1, 1}, {1, 2}, {2, 3}})}),
        [](const ::testing::TestParamInfo<FollowerAppendCase>& info) { return info.param.name; });

// 场景：节点 1 只有 index=1、term=1 的日志，节点 0 以 term=2 向它复制。
// 过程：先发送 prev term 不匹配的 index=2，再越过 index=2 直接发送 index=3。
// 预期：两次请求都被拒绝，follower 原日志保持不变，不产生空洞或错误覆盖。
TEST(RaftReplicationProtocolTest, FollowerRejectsGapAndPreviousTermMismatch) {
    std::vector<PeerMember> members{make_test_member(712, 0), make_test_member(712, 1)};
    RaftTestCluster cluster;
    for (int i = 0; i < 2; ++i) {
        RaftTestNodeSpec spec;
        spec.self = members[i];
        spec.initial_voters = members;
        spec.hard_state = RaftMeta{2, std::nullopt};
        if (i == 1) spec.entries = entries({{1, 1}});
        cluster.add_node(std::move(spec));
    }

    ASSERT_TRUE(cluster.deliver(enqueue_append_request(cluster, 0, 1, 2, entries({{2, 2}}), 1, 2)).ok());
    TestMessageId mismatch_id = *cluster.first_message(RaftTestMessageKind::APPEND_RESPONSE, 1, 0);
    EXPECT_FALSE(std::get<AppendResponsePayload>(pending_message(cluster, mismatch_id).payload).response.success);
    cluster.drop(mismatch_id);

    ASSERT_TRUE(cluster.deliver(enqueue_append_request(cluster, 0, 1, 2, entries({{2, 3}}), 0, 0)).ok());
    TestMessageId gap_id = *cluster.first_message(RaftTestMessageKind::APPEND_RESPONSE, 1, 0);
    EXPECT_FALSE(std::get<AppendResponsePayload>(pending_message(cluster, gap_id).payload).response.success);
    EXPECT_EQ(cluster.core(1).log_entries_for_test(), entries({{1, 1}}));
}

// Reference: etcd TestLeaderSyncFollowerLog.
// 场景：leader 的后缀属于 term=3，follower 在同一位置保留更长的 term=2 分叉日志。
// 过程：选举完成后反复投递复制请求和拒绝响应，让 leader 逐步回退 next index。
// 预期：leader 找到共同前缀，覆盖 follower 的分叉后缀，最终两边日志完全一致。
TEST(RaftReplicationProtocolTest, LeaderBacksOffUntilDivergentFollowerConverges) {
    std::vector<PeerMember> members{make_test_member(713, 0), make_test_member(713, 1)};
    RaftTestCluster cluster;
    RaftTestNodeSpec leader;
    leader.self = members[0];
    leader.initial_voters = members;
    leader.hard_state = RaftMeta{3, std::nullopt};
    leader.entries = entries({{1, 1}, {3, 2}, {3, 3}});
    cluster.add_node(std::move(leader));
    RaftTestNodeSpec follower;
    follower.self = members[1];
    follower.initial_voters = members;
    follower.hard_state = RaftMeta{3, std::nullopt};
    follower.entries = entries({{1, 1}, {2, 2}, {2, 3}, {2, 4}});
    cluster.add_node(std::move(follower));

    elect(cluster);
    for (int round = 0; round < 6 && cluster.core(1).log_entries_for_test() != cluster.core(0).log_entries_for_test();
         ++round) {
        cluster.tick(0);
        deliver_append_round_trip(cluster, 0, 1);
    }

    EXPECT_EQ(cluster.core(1).log_entries_for_test(), cluster.core(0).log_entries_for_test()) << cluster.trace();
}

// 场景：3 节点集群选出节点 0 后隔离节点 2，leader 连续提交三条写日志。
// 过程：节点 0、1 在多数派内完成提交；随后解除隔离并驱动一次复制和 apply。
// 预期：落后 follower 补齐日志、commit index 和 KV 状态，最终与 leader 一致。
TEST(RaftReplicationProtocolTest, LaggingFollowerCatchesUpAfterPartitionHeals) {
    RaftTestCluster cluster = RaftTestCluster::voters(3);
    elect(cluster);
    cluster.isolate(2);

    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(cluster.propose(0, ProposeParam::write(WriteOpType::PUT, "k" + std::to_string(i),
                                                           "v" + std::to_string(i)))
                            .status.ok());
        cluster.deliver_all();
    }
    ASSERT_LT(cluster.core(2).last_log_index(), cluster.core(0).last_log_index());

    cluster.heal_all();
    cluster.tick(0);
    cluster.deliver_all();
    cluster.apply_all();

    EXPECT_EQ(cluster.core(2).log_entries_for_test(), cluster.core(0).log_entries_for_test());
    EXPECT_EQ(cluster.core(2).commit_index(), cluster.core(0).commit_index());
    EXPECT_EQ(cluster.kv(2), cluster.kv(0));
}

// 场景：旧 leader 节点 0 被隔离并追加未提交日志，节点 1、2 随后选出新 leader。
// 过程：新 leader 提交另一条写日志，再恢复旧 leader 的网络并向其复制新日志。
// 预期：旧 leader 退为 follower，未提交旧日志被覆盖，状态机只出现新 leader 的写入。
TEST(RaftReplicationProtocolTest, NewLeaderOverwritesUncommittedOldLeaderEntry) {
    RaftTestCluster cluster = RaftTestCluster::voters(3);
    elect(cluster, 0);
    cluster.isolate(0);

    ASSERT_TRUE(cluster.propose(0, ProposeParam::write(WriteOpType::PUT, "old", "uncommitted")).status.ok());
    cluster.deliver_all();
    ASSERT_EQ(cluster.core(0).commit_index(), 1);

    elect(cluster, 1);
    ASSERT_TRUE(cluster.propose(1, ProposeParam::write(WriteOpType::PUT, "new", "committed")).status.ok());
    cluster.deliver_all();
    ASSERT_GE(cluster.core(1).commit_index(), 3);

    cluster.heal_all();
    cluster.tick(1);
    cluster.deliver_all();
    cluster.apply_all();

    EXPECT_EQ(cluster.core(0).role(), ReplicaRole::FOLLOWER);
    EXPECT_EQ(cluster.core(0).log_entries_for_test(), cluster.core(1).log_entries_for_test());
    EXPECT_EQ(cluster.kv(0).count("old"), 0U);
    EXPECT_EQ(cluster.kv(0).at("new"), "committed");
}

// Reference: etcd TestLeaderOnlyCommitsLogFromCurrentTerm; Raft paper figure 8.
// 场景：3 个节点都带有 term=1 的两条未提交日志，节点 0 在 term=2 当选 leader。
// 过程：先伪造 follower 仅确认旧 term 日志，再让它确认 leader 新增的 term=2 no-op。
// 预期：旧 term 日志不能单独推进 commit；当前 term 日志形成多数派后连同旧日志一起提交。
TEST(RaftReplicationProtocolTest, PreviousTermEntriesCommitOnlyWithCurrentTermEntry) {
    std::vector<PeerMember> members{make_test_member(714, 0), make_test_member(714, 1), make_test_member(714, 2)};
    RaftTestCluster cluster;
    for (const PeerMember& member : members) {
        RaftTestNodeSpec spec;
        spec.self = member;
        spec.initial_voters = members;
        spec.hard_state = RaftMeta{1, std::nullopt};
        spec.entries = entries({{1, 1}, {1, 2}});
        cluster.add_node(std::move(spec));
    }

    cluster.campaign(0);
    ASSERT_TRUE(cluster.deliver(*cluster.first_message(RaftTestMessageKind::VOTE_REQUEST, 0, 1)).ok());
    ASSERT_TRUE(cluster.deliver(*cluster.first_message(RaftTestMessageKind::VOTE_RESPONSE, 1, 0)).ok());
    ASSERT_EQ(cluster.core(0).role(), ReplicaRole::LEADER);
    ASSERT_EQ(cluster.core(0).last_log_index(), 3);

    AppendEntriesParam old_entries{cluster.replica_id(0), cluster.replica_id(1), 2, entries({{1, 1}, {1, 2}}), 0, 0, 0};
    ASSERT_TRUE(cluster.deliver(enqueue_append_response(cluster, 1, 0, old_entries, 2, true, 2)).ok());
    EXPECT_EQ(cluster.core(0).commit_index(), 0);

    deliver_append_round_trip(cluster, 0, 1);
    EXPECT_EQ(cluster.core(0).commit_index(), 3);
}

// 场景：leader 向节点 1 复制新日志，并保留同一成功响应的副本及一个较旧的拒绝响应。
// 过程：连续投递两次成功响应，然后再投递针对旧请求的 stale reject。
// 预期：重复成功不重复计数，迟到拒绝也不能回退已经确认的 follower progress。
TEST(RaftReplicationProtocolTest, DuplicateSuccessAndStaleRejectDoNotRegressProgress) {
    RaftTestCluster cluster = RaftTestCluster::voters(3);
    elect(cluster);
    ASSERT_TRUE(cluster.propose(0, ProposeParam::write(WriteOpType::PUT, "stable", "value")).status.ok());

    const TestMessageId request_id = *cluster.first_message(RaftTestMessageKind::APPEND_REQUEST, 0, 1);
    const AppendEntriesParam sent = pending_append(cluster, request_id);
    ASSERT_TRUE(cluster.deliver(request_id).ok());
    const TestMessageId response_id = *cluster.first_message(RaftTestMessageKind::APPEND_RESPONSE, 1, 0);
    const TestMessageId duplicate_id = cluster.duplicate(response_id);
    ASSERT_TRUE(cluster.deliver(response_id).ok());
    ASSERT_TRUE(cluster.deliver(duplicate_id).ok());
    const LogIndex next_after_success = cluster.core(0).next_index_for_test(cluster.replica_id(1));

    ASSERT_TRUE(cluster.deliver(enqueue_append_response(cluster, 1, 0, sent, 1, false, 0)).ok());
    EXPECT_EQ(cluster.core(0).next_index_for_test(cluster.replica_id(1)), next_after_success);
    EXPECT_EQ(cluster.core(0).commit_index(), 2);
}

// 场景：节点 0 在旧 term 发出复制请求后因高 term 响应退位，随后又在 term=3 当选。
// 过程：新任期建立后，投递一条针对旧 term 请求的延迟成功响应。
// 预期：旧响应不改变新 leader 的角色、term、next index 或 commit index。
TEST(RaftReplicationProtocolTest, DelayedPreviousTermResponseIsIgnoredByNewLeader) {
    RaftTestCluster cluster = RaftTestCluster::voters(3, 716);
    elect(cluster);
    ASSERT_TRUE(cluster.propose(0, ProposeParam::write(WriteOpType::PUT, "old-term", "value")).status.ok());
    const AppendEntriesParam old_request =
            pending_append(cluster, *cluster.first_message(RaftTestMessageKind::APPEND_REQUEST, 0, 1));

    Status step_down = cluster.deliver(enqueue_append_response(cluster, 2, 0, old_request, 2, false, 1));
    ASSERT_EQ(step_down.code(), StatusCode::NOT_LEADER);
    cluster.drop_all();
    elect(cluster, 0);
    ASSERT_EQ(cluster.core(0).current_term(), 3);
    const LogIndex next_before = cluster.core(0).next_index_for_test(cluster.replica_id(1));
    const LogIndex commit_before = cluster.core(0).commit_index();

    ASSERT_TRUE(cluster.deliver(enqueue_append_response(cluster, 1, 0, old_request, 1, true, 2)).ok());

    EXPECT_EQ(cluster.core(0).role(), ReplicaRole::LEADER);
    EXPECT_EQ(cluster.core(0).current_term(), 3);
    EXPECT_EQ(cluster.core(0).next_index_for_test(cluster.replica_id(1)), next_before);
    EXPECT_EQ(cluster.core(0).commit_index(), commit_before);
}

// 场景：2 节点日志均为空，但 index=2 的复制消息先于 index=1 到达 follower。
// 过程：先投递乱序后缀并确认被拒绝，再投递缺失前缀，最后重发后缀。
// 预期：follower 不接受带空洞的日志；前缀补齐后，同一后缀能够成功追加。
TEST(RaftReplicationProtocolTest, OutOfOrderAppendIsRejectedThenSucceedsAfterPrefix) {
    RaftTestCluster cluster = RaftTestCluster::voters(2, 715);
    const std::vector<LogEntry> first = entries({{1, 1}});
    const std::vector<LogEntry> second = entries({{1, 2}});

    ASSERT_TRUE(cluster.deliver(enqueue_append_request(cluster, 0, 1, 1, second, 1, 1)).ok());
    TestMessageId early_response = *cluster.first_message(RaftTestMessageKind::APPEND_RESPONSE, 1, 0);
    EXPECT_FALSE(std::get<AppendResponsePayload>(pending_message(cluster, early_response).payload).response.success);
    cluster.drop(early_response);

    ASSERT_TRUE(cluster.deliver(enqueue_append_request(cluster, 0, 1, 1, first, 0, 0)).ok());
    cluster.drop(*cluster.first_message(RaftTestMessageKind::APPEND_RESPONSE, 1, 0));
    ASSERT_TRUE(cluster.deliver(enqueue_append_request(cluster, 0, 1, 1, second, 1, 1)).ok());

    EXPECT_EQ(cluster.core(1).log_entries_for_test(), entries({{1, 1}, {1, 2}}));
}

// 场景：节点 0 是 term=1 的 leader，节点 1 返回 term=2 的 AppendEntries 拒绝响应。
// 过程：直接把这个 higher-term response 投递给 leader。
// 预期：节点 0 更新 term 并立即退为 follower，而不是继续按旧任期复制。
TEST(RaftReplicationProtocolTest, HigherTermAppendResponseForcesLeaderToStepDown) {
    RaftTestCluster cluster = RaftTestCluster::voters(3);
    elect(cluster);
    AppendEntriesParam sent{cluster.replica_id(0), cluster.replica_id(1), 1, {}, 1, 1, 1};

    Status status = cluster.deliver(enqueue_append_response(cluster, 1, 0, sent, 2, false, 1));

    EXPECT_EQ(status.code(), StatusCode::NOT_LEADER);
    EXPECT_EQ(cluster.core(0).role(), ReplicaRole::FOLLOWER);
    EXPECT_EQ(cluster.core(0).current_term(), 2);
}

// 场景：3 节点集群隔离节点 2，leader 一次批量追加 600 条写日志。
// 过程：观察首批 AppendEntries 大小，再通过多轮响应和心跳把余下日志复制给节点 1。
// 预期：单条消息不超过实现的 256-entry 上限，分批复制后 follower 日志和 KV 完整一致。
TEST(RaftReplicationProtocolTest, CapsAppendBatchAndReplicatesRemainingEntries) {
    RaftTestCluster cluster = RaftTestCluster::voters(3);
    elect(cluster);
    cluster.isolate(2);

    std::vector<ProposeParam> proposals;
    for (int i = 0; i < 600; ++i) {
        proposals.push_back(
                ProposeParam::write(WriteOpType::PUT, "batch-" + std::to_string(i), "value-" + std::to_string(i)));
    }
    std::vector<RaftTestProposalResult> results = cluster.propose_batch(0, proposals);
    ASSERT_EQ(results.size(), proposals.size());
    ASSERT_TRUE(std::all_of(results.begin(), results.end(),
                            [](const RaftTestProposalResult& result) { return result.status.ok(); }));

    const TestMessageId first_id = *cluster.first_message(RaftTestMessageKind::APPEND_REQUEST, 0, 1);
    EXPECT_EQ(pending_append(cluster, first_id).entries.size(), 256U);

    for (int round = 0; round < 10 && cluster.core(1).last_log_index() < cluster.core(0).last_log_index(); ++round) {
        deliver_append_round_trip(cluster, 0, 1);
        for (TestMessageId id : cluster.message_ids(RaftTestMessageKind::APPEND_REQUEST, 0, 2)) {
            cluster.drop(id);
        }
        if (cluster.core(1).last_log_index() < cluster.core(0).last_log_index()) cluster.tick(0);
    }
    ASSERT_EQ(cluster.core(1).last_log_index(), cluster.core(0).last_log_index()) << cluster.trace();

    cluster.tick(0);
    deliver_append_round_trip(cluster, 0, 1);
    cluster.apply(0);
    cluster.apply(1);
    EXPECT_EQ(cluster.kv(1), cluster.kv(0));
    EXPECT_EQ(cluster.kv(0).size(), 600U);
}

}  // namespace
}  // namespace adviskv::storage::test
