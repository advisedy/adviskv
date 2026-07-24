#include <algorithm>
#include <optional>
#include <ostream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "test/storage/raft/core/raft_test_harness.h"

namespace adviskv::storage::test {
namespace {

RequestVoteParam vote_request(const RaftTestCluster& cluster, TestNodeId from, TestNodeId to, Term term,
                              LogIndex last_index, Term last_term) {
    return RequestVoteParam{cluster.replica_id(from), cluster.replica_id(to), term, last_index, last_term};
}

TestMessageId enqueue_vote_request(RaftTestCluster& cluster, TestNodeId from, TestNodeId to, Term term,
                                   LogIndex last_index = 0, Term last_term = 0) {
    return cluster.enqueue(RaftTestEnvelope{
            0, from, to, VoteRequestPayload{vote_request(cluster, from, to, term, last_index, last_term)}});
}

TestMessageId enqueue_vote_response(RaftTestCluster& cluster, TestNodeId from, TestNodeId to, const ReplicaID& voter,
                                    Term term, bool granted) {
    return cluster.enqueue(RaftTestEnvelope{0, from, to, VoteResponsePayload{voter, RequestVoteResult{term, granted}}});
}

TestMessageId enqueue_heartbeat(RaftTestCluster& cluster, TestNodeId from, TestNodeId to, Term term,
                                LogIndex leader_commit = 0) {
    AppendEntriesParam request{cluster.replica_id(from), cluster.replica_id(to), term, {}, 0, 0, leader_commit};
    return cluster.enqueue(RaftTestEnvelope{0, from, to, AppendRequestPayload{std::move(request)}});
}

RequestVoteResult take_vote_response(RaftTestCluster& cluster, TestNodeId from, TestNodeId to) {
    std::optional<TestMessageId> id = cluster.first_message(RaftTestMessageKind::VOTE_RESPONSE, from, to);
    if (!id.has_value()) throw std::runtime_error("expected vote response");
    std::optional<RaftTestEnvelope> envelope = cluster.take(*id);
    return std::get<VoteResponsePayload>(envelope->payload).response;
}

std::vector<LogEntry> log_ending_at(LogIndex index, Term term) {
    std::vector<LogEntry> entries;
    for (LogIndex i = 1; i <= index; ++i) {
        entries.push_back(make_test_entry(i == index ? term : std::max<Term>(1, term - 1), i));
    }
    return entries;
}

class ElectionQuorumTest : public ::testing::TestWithParam<int> {};

// Reference: etcd-io/raft@26647d57 TestStartAsFollower and
// TestLeaderElectionInOneRoundRPC; Raft paper section 5.2.
// 场景：启动 1/3/5 个 term=0 的 follower，只推进节点 0 的固定选举时钟。
// 过程：前 9 个 tick 保持 follower，第 10 个 tick 发起竞选并投递完本轮投票消息。
// 预期：节点 0 获得 voter quorum，成为 term=1 的 leader，并提交当前 term 的 no-op。
TEST_P(ElectionQuorumTest, FixedTimeoutElectsLeaderWithQuorum) {
    RaftTestCluster cluster = RaftTestCluster::voters(GetParam());

    for (int node = 0; node < cluster.node_count(); ++node) {
        EXPECT_EQ(cluster.core(node).role(), ReplicaRole::FOLLOWER);
        EXPECT_EQ(cluster.core(node).current_term(), 0);
    }

    for (int tick = 0; tick < 9; ++tick)
        cluster.tick(0);
    EXPECT_EQ(cluster.core(0).role(), ReplicaRole::FOLLOWER);

    cluster.tick(0);
    cluster.deliver_all();

    EXPECT_EQ(cluster.core(0).role(), ReplicaRole::LEADER) << cluster.trace();
    EXPECT_EQ(cluster.core(0).current_term(), 1);
    EXPECT_EQ(cluster.core(0).last_log_index(), 1);
    EXPECT_EQ(cluster.core(0).last_log_term(), 1);
    EXPECT_EQ(cluster.core(0).commit_index(), 1);
}

INSTANTIATE_TEST_SUITE_P(ClusterSizes, ElectionQuorumTest, ::testing::Values(1, 3, 5),
                         [](const ::testing::TestParamInfo<int>& info) {
                             return "Nodes" + std::to_string(info.param);
                         });

// Reference: etcd TestFollowerVote and TestVoter; Raft paper section 5.2.
// 场景：三节点集群中的节点 2 在 term=1 依次收到节点 0、节点 1、节点 0 的投票请求。
// 预期：节点 2 首次投给节点 0，拒绝同 term 的竞争者，但允许节点 0 重试同一张票。
TEST(RaftElectionProtocolTest, GrantsOneVotePerTermAndRepeatsVoteForSameCandidate) {
    RaftTestCluster cluster = RaftTestCluster::voters(3);

    ASSERT_TRUE(cluster.deliver(enqueue_vote_request(cluster, 0, 2, 1)).ok());
    RequestVoteResult first = take_vote_response(cluster, 2, 0);
    EXPECT_TRUE(first.vote_granted);
    EXPECT_EQ(first.term, 1);

    ASSERT_TRUE(cluster.deliver(enqueue_vote_request(cluster, 1, 2, 1)).ok());
    RequestVoteResult competing = take_vote_response(cluster, 2, 1);
    EXPECT_FALSE(competing.vote_granted);
    EXPECT_EQ(competing.term, 1);

    ASSERT_TRUE(cluster.deliver(enqueue_vote_request(cluster, 0, 2, 1)).ok());
    RequestVoteResult repeated = take_vote_response(cluster, 2, 0);
    EXPECT_TRUE(repeated.vote_granted);
    EXPECT_EQ(repeated.term, 1);
}

struct LogFreshnessCase {
    const char* name;
    LogIndex local_index;
    Term local_term;
    LogIndex candidate_index;
    Term candidate_term;
    bool granted;
};

void PrintTo(const LogFreshnessCase& value, std::ostream* stream) { *stream << value.name; }

class VoteLogFreshnessTest : public ::testing::TestWithParam<LogFreshnessCase> {};

// Reference: etcd TestVoteRequest; Raft paper section 5.4.1.
// 场景：一个 term=9 的 voter 持有指定末尾 term/index 的日志，收到 term=10 candidate 的投票请求。
// 过程：参数表分别构造 candidate 日志 term 更旧、相同或更新，以及同 term 时 index 更短/相同/更长。
// 预期：先比较末尾 term；只有 term 相同时才比较 index，并据此决定是否投票。
TEST_P(VoteLogFreshnessTest, ComparesLastTermBeforeLastIndex) {
    const LogFreshnessCase& test = GetParam();
    std::vector<PeerMember> members{make_test_member(701, 0), make_test_member(701, 1)};
    RaftTestCluster cluster;

    RaftTestNodeSpec candidate;
    candidate.self = members[0];
    candidate.initial_voters = members;
    cluster.add_node(candidate);

    RaftTestNodeSpec voter;
    voter.self = members[1];
    voter.initial_voters = members;
    voter.hard_state = RaftMeta{9, std::nullopt};
    voter.entries = log_ending_at(test.local_index, test.local_term);
    cluster.add_node(std::move(voter));

    ASSERT_TRUE(
            cluster.deliver(enqueue_vote_request(cluster, 0, 1, 10, test.candidate_index, test.candidate_term)).ok());
    RequestVoteResult response = take_vote_response(cluster, 1, 0);
    EXPECT_EQ(response.vote_granted, test.granted);
    EXPECT_EQ(response.term, 10);
}

INSTANTIATE_TEST_SUITE_P(CandidateLogs, VoteLogFreshnessTest,
                         ::testing::Values(LogFreshnessCase{"OlderTermShorter", 2, 2, 1, 1, false},
                                           LogFreshnessCase{"OlderTermLonger", 2, 2, 3, 1, false},
                                           LogFreshnessCase{"SameTermShorter", 2, 2, 1, 2, false},
                                           LogFreshnessCase{"SameLog", 2, 2, 2, 2, true},
                                           LogFreshnessCase{"SameTermLonger", 2, 2, 3, 2, true},
                                           LogFreshnessCase{"NewerTermShorter", 2, 2, 1, 3, true}),
                         [](const ::testing::TestParamInfo<LogFreshnessCase>& info) { return info.param.name; });

// Reference: etcd TestLeaderElectionInOneRoundRPC.
// 场景：五节点集群的节点 0 已成为 term=1 candidate，并清空真实投票请求。
// 过程：同一个 voter 的赞成响应重复到达两次，随后另一个 voter 再投赞成票。
// 预期：重复响应只计一票；节点 0 在第三张不同来源的票到达后才成为 leader。
TEST(RaftElectionProtocolTest, DuplicateVoteResponseDoesNotIncreaseQuorum) {
    RaftTestCluster cluster = RaftTestCluster::voters(5);
    cluster.campaign(0);
    cluster.drop_all();

    ASSERT_TRUE(cluster.deliver(enqueue_vote_response(cluster, 1, 0, cluster.replica_id(1), 1, true)).ok());
    ASSERT_TRUE(cluster.deliver(enqueue_vote_response(cluster, 1, 0, cluster.replica_id(1), 1, true)).ok());
    EXPECT_EQ(cluster.core(0).role(), ReplicaRole::CANDIDATE);

    ASSERT_TRUE(cluster.deliver(enqueue_vote_response(cluster, 2, 0, cluster.replica_id(2), 1, true)).ok());
    EXPECT_EQ(cluster.core(0).role(), ReplicaRole::LEADER);
}

// Reference: etcd TestRejectStaleTermMessage and TestUpdateTermFromMessage.
// 场景：三节点集群的节点 0 是 term=1 candidate，依次收到旧 term 响应、旧 term 请求和高 term 响应。
// 预期：term=0 的消息不改变竞选状态；term=2 的合法 voter 响应使节点 0 更新 term 并退回 follower。
TEST(RaftElectionProtocolTest, IgnoresStaleMessagesAndStepsDownForHigherTermVoter) {
    RaftTestCluster cluster = RaftTestCluster::voters(3);
    cluster.campaign(0);
    cluster.drop_all();

    ASSERT_TRUE(cluster.deliver(enqueue_vote_response(cluster, 1, 0, cluster.replica_id(1), 0, true)).ok());
    EXPECT_EQ(cluster.core(0).role(), ReplicaRole::CANDIDATE);
    EXPECT_EQ(cluster.core(0).current_term(), 1);

    ASSERT_TRUE(cluster.deliver(enqueue_vote_request(cluster, 1, 0, 0)).ok());
    RequestVoteResult stale = take_vote_response(cluster, 0, 1);
    EXPECT_FALSE(stale.vote_granted);
    EXPECT_EQ(stale.term, 1);

    ASSERT_TRUE(cluster.deliver(enqueue_vote_response(cluster, 1, 0, cluster.replica_id(1), 2, false)).ok());
    EXPECT_EQ(cluster.core(0).role(), ReplicaRole::FOLLOWER);
    EXPECT_EQ(cluster.core(0).current_term(), 2);
}

// Reference: etcd TestCandidateFallback; Raft paper section 5.2.
// 场景：节点 0 已是 term=1 candidate，此时收到节点 1 发来的同 term AppendEntries 心跳。
// 预期：同 term leader 的 AppendEntries 证明本 term 已有 leader，节点 0 立即退回 follower。
TEST(RaftElectionProtocolTest, CandidateFallsBackOnSameTermAppendEntries) {
    RaftTestCluster cluster = RaftTestCluster::voters(3);
    cluster.campaign(0);
    cluster.drop_all();

    ASSERT_TRUE(cluster.deliver(enqueue_heartbeat(cluster, 1, 0, 1)).ok());

    EXPECT_EQ(cluster.core(0).role(), ReplicaRole::FOLLOWER);
    EXPECT_EQ(cluster.core(0).current_term(), 1);
}

// Reference: etcd TestCandidateResetTermMsgHeartbeat and Raft paper section 5.2.
// 场景：两节点集群的节点 1 先收到 term=1 leader 心跳，再单独推进自己的选举时钟。
// 过程：心跳后的前 9 个 tick 不竞选，第 10 个 tick 才再次超时。
// 预期：心跳确实重置完整选举周期，节点 1 超时后以 term=2 成为 candidate。
TEST(RaftElectionProtocolTest, HeartbeatResetsElectionTimeout) {
    RaftTestCluster cluster = RaftTestCluster::voters(2);

    ASSERT_TRUE(cluster.deliver(enqueue_heartbeat(cluster, 0, 1, 1)).ok());
    cluster.drop_all();
    for (int tick = 0; tick < 9; ++tick)
        cluster.tick(1);
    EXPECT_EQ(cluster.core(1).role(), ReplicaRole::FOLLOWER);

    cluster.tick(1);
    EXPECT_EQ(cluster.core(1).role(), ReplicaRole::CANDIDATE);
    EXPECT_EQ(cluster.core(1).current_term(), 2);
}

// Reference: etcd TestDuelingCandidates.
// 场景：四节点集群中节点 0 和节点 1 同时在 term=1 竞选，节点 2、3 分别投给不同 candidate。
// 过程：刻意丢弃交叉投票请求制造 2:2 split vote，再让节点 0 发起下一 term 竞选。
// 预期：term=1 无 leader；节点 0 在 term=2 获得 quorum 后收敛为唯一 leader。
TEST(RaftElectionProtocolTest, SplitVoteConvergesInTheNextTerm) {
    RaftTestCluster cluster = RaftTestCluster::voters(4);
    cluster.campaign(0);
    cluster.campaign(1);

    ASSERT_TRUE(cluster.deliver(*cluster.first_message(RaftTestMessageKind::VOTE_REQUEST, 0, 2)).ok());
    ASSERT_TRUE(cluster.deliver(*cluster.first_message(RaftTestMessageKind::VOTE_REQUEST, 1, 3)).ok());
    ASSERT_TRUE(cluster.deliver(*cluster.first_message(RaftTestMessageKind::VOTE_REQUEST, 0, 1)).ok());
    ASSERT_TRUE(cluster.deliver(*cluster.first_message(RaftTestMessageKind::VOTE_REQUEST, 1, 0)).ok());
    ASSERT_TRUE(cluster.drop(*cluster.first_message(RaftTestMessageKind::VOTE_REQUEST, 0, 3)));
    ASSERT_TRUE(cluster.drop(*cluster.first_message(RaftTestMessageKind::VOTE_REQUEST, 1, 2)));
    cluster.deliver_all();

    EXPECT_EQ(cluster.core(0).role(), ReplicaRole::CANDIDATE);
    EXPECT_EQ(cluster.core(1).role(), ReplicaRole::CANDIDATE);

    cluster.campaign(0);
    cluster.deliver_all();
    EXPECT_EQ(cluster.core(0).role(), ReplicaRole::LEADER) << cluster.trace();
    EXPECT_EQ(cluster.core(0).current_term(), 2);
}

// Reference: etcd TestLearnerElectionTimeout and TestLearnerCampaign.
// 场景：同一 membership 中分别启动 voter、learner 和不在配置内的 non-member，并只推进后两者时钟。
// 预期：learner 和 non-member 即使多次超过选举 timeout，也始终不发起竞选、不提升 term。
TEST(RaftElectionProtocolTest, LearnerAndNonMemberDoNotCampaign) {
    const PeerMember voter = make_test_member(702, 0);
    const PeerMember learner = make_test_member(702, 1);
    const PeerMember outsider = make_test_member(702, 2);
    const std::vector<RaftMember> membership{{voter, RaftMemberType::VOTER}, {learner, RaftMemberType::LEARNER}};
    RaftTestCluster cluster;

    for (const PeerMember& self : {voter, learner, outsider}) {
        RaftTestNodeSpec spec;
        spec.self = self;
        spec.initial_voters = {voter};
        spec.membership = membership;
        cluster.add_node(std::move(spec));
    }

    for (int tick = 0; tick < 20; ++tick) {
        cluster.tick(1);
        cluster.tick(2);
    }
    EXPECT_EQ(cluster.core(1).role(), ReplicaRole::FOLLOWER);
    EXPECT_EQ(cluster.core(2).role(), ReplicaRole::FOLLOWER);
    EXPECT_EQ(cluster.core(1).current_term(), 0);
    EXPECT_EQ(cluster.core(2).current_term(), 0);
}

// 场景：一个 voter 分别收到已知 learner 和未知 non-member 发来的高 term RequestVote。
// 预期：二者都没有竞选资格，voter 拒绝投票，且不接受它们声明的 term。
TEST(RaftElectionProtocolTest, VoterRejectsLearnerAndNonMemberCandidates) {
    const PeerMember voter = make_test_member(702, 0);
    const PeerMember learner = make_test_member(702, 1);
    const PeerMember outsider = make_test_member(702, 2);
    const std::vector<RaftMember> membership{{voter, RaftMemberType::VOTER}, {learner, RaftMemberType::LEARNER}};
    RaftTestCluster cluster;

    for (const PeerMember& self : {voter, learner, outsider}) {
        RaftTestNodeSpec spec;
        spec.self = self;
        spec.initial_voters = {voter};
        spec.membership = membership;
        cluster.add_node(std::move(spec));
    }

    ASSERT_TRUE(cluster.deliver(enqueue_vote_request(cluster, 1, 0, 5)).ok());
    RequestVoteResult from_learner = take_vote_response(cluster, 0, 1);
    EXPECT_FALSE(from_learner.vote_granted);
    EXPECT_EQ(cluster.core(0).current_term(), 0);

    ASSERT_TRUE(cluster.deliver(enqueue_vote_request(cluster, 2, 0, 5)).ok());
    RequestVoteResult from_outsider = take_vote_response(cluster, 0, 2);
    EXPECT_FALSE(from_outsider.vote_granted);
    EXPECT_EQ(cluster.core(0).current_term(), 0);
}

// Reference: etcd TestLearnerCanVote and campaign_learner_must_vote. A learner
// cannot campaign, but it must vote for a known, up-to-date voter. Its local
// config may lag a committed promotion that already made it part of quorum.
// 场景：一个仍把自己视为 learner 的节点收到已知 voter 发来的 term=5 RequestVote。
// 预期：learner 自己不能竞选，但应为合法 voter 投票并更新 term；当前实现错误地直接拒绝。
TEST(RaftElectionProtocolTest, LearnerCanVoteForKnownVoter) {
    const PeerMember voter = make_test_member(702, 0);
    const PeerMember learner = make_test_member(702, 1);
    const std::vector<RaftMember> membership{{voter, RaftMemberType::VOTER}, {learner, RaftMemberType::LEARNER}};
    RaftTestCluster cluster;

    for (const PeerMember& self : {voter, learner}) {
        RaftTestNodeSpec spec;
        spec.self = self;
        spec.initial_voters = {voter};
        spec.membership = membership;
        cluster.add_node(std::move(spec));
    }

    ASSERT_TRUE(cluster.deliver(enqueue_vote_request(cluster, 0, 1, 5)).ok());
    RequestVoteResult by_learner = take_vote_response(cluster, 1, 0);
    EXPECT_TRUE(by_learner.vote_granted);
    EXPECT_EQ(by_learner.term, 5);
    EXPECT_EQ(cluster.core(1).current_term(), 5);
}

// Expected by Raft paper section 5.2 and etcd TestLeaderElectionInOneRoundRPC:
// election quorum contains only voters. Current behavior records a learner
// response and immediately becomes leader (raft_core_election.cpp).
// 场景：三 voter 加一 learner 的集群中，节点 0 竞选后只收到 learner 的赞成响应。
// 预期：self vote 加 learner 响应不构成 voter quorum；当前实现却把 learner 计票并当选。
TEST(RaftElectionProtocolTest, DISABLE_LearnerVoteResponseDoesNotCountTowardQuorum) {
    std::vector<PeerMember> voters{make_test_member(703, 0), make_test_member(703, 1), make_test_member(703, 2)};
    const PeerMember learner = make_test_member(703, 3);
    const std::vector<RaftMember> membership{{voters[0], RaftMemberType::VOTER},
                                             {voters[1], RaftMemberType::VOTER},
                                             {voters[2], RaftMemberType::VOTER},
                                             {learner, RaftMemberType::LEARNER}};
    RaftTestCluster cluster;
    for (const PeerMember& member : voters) {
        RaftTestNodeSpec spec;
        spec.self = member;
        spec.initial_voters = voters;
        spec.membership = membership;
        cluster.add_node(std::move(spec));
    }
    RaftTestNodeSpec learner_spec;
    learner_spec.self = learner;
    learner_spec.initial_voters = voters;
    learner_spec.membership = membership;
    cluster.add_node(std::move(learner_spec));

    cluster.campaign(0);
    cluster.drop_all();
    ASSERT_TRUE(cluster.deliver(enqueue_vote_response(cluster, 3, 0, learner.replica_id, 1, true)).ok());

    EXPECT_EQ(cluster.core(0).role(), ReplicaRole::CANDIDATE);
}

// Expected by Raft paper section 5.2 and etcd TestLeaderElectionInOneRoundRPC:
// election quorum contains only configured voters. Current behavior accepts a
// same-term response from any ReplicaID (raft_core_election.cpp).
// 场景：五 voter 集群的节点 0 竞选后，先收到未知 ReplicaID 的赞成票，再收到一个合法 voter 的赞成票。
// 预期：未知来源不计票，节点 0 仍缺 voter quorum；当前实现错误地将未知来源计入选举。
TEST(RaftElectionProtocolTest, DISABLED_UnknownVoteResponseDoesNotCountTowardQuorum) {
    RaftTestCluster cluster = RaftTestCluster::voters(5);
    const ReplicaID unknown{999, 0, 99};
    cluster.campaign(0);
    cluster.drop_all();

    ASSERT_TRUE(cluster.deliver(enqueue_vote_response(cluster, -1, 0, unknown, 1, true)).ok());
    ASSERT_TRUE(cluster.deliver(enqueue_vote_response(cluster, 1, 0, cluster.replica_id(1), 1, true)).ok());

    EXPECT_EQ(cluster.core(0).role(), ReplicaRole::CANDIDATE);
}

// Expected by the AdvisKV membership contract: an unconfigured response cannot
// alter election state. Current behavior trusts its higher term and forces the
// candidate to step down before validating the sender (raft_core_election.cpp).
// 场景：term=1 candidate 收到未知 ReplicaID 伪造的 term=10 拒绝响应。
// 预期：未配置来源不能改变选举状态；当前实现先信任 term，导致 candidate 退位并跳到 term=10。
TEST(RaftElectionProtocolTest, DISABLED_UnknownHigherTermVoteResponseDoesNotChangeElection) {
    RaftTestCluster cluster = RaftTestCluster::voters(3);
    const ReplicaID unknown{999, 0, 99};
    cluster.campaign(0);
    cluster.drop_all();

    ASSERT_TRUE(cluster.deliver(enqueue_vote_response(cluster, -1, 0, unknown, 10, false)).ok());

    EXPECT_EQ(cluster.core(0).role(), ReplicaRole::CANDIDATE);
    EXPECT_EQ(cluster.core(0).current_term(), 1);
}

}  // namespace
}  // namespace adviskv::storage::test
