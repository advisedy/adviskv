#include <algorithm>
#include <optional>
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

void become_uncommitted_leader(RaftTestCluster& cluster) {
    cluster.campaign(0);
    ASSERT_TRUE(cluster.deliver(*cluster.first_message(RaftTestMessageKind::VOTE_REQUEST, 0, 1)).ok());
    ASSERT_TRUE(cluster.deliver(*cluster.first_message(RaftTestMessageKind::VOTE_RESPONSE, 1, 0)).ok());
    ASSERT_EQ(cluster.core(0).role(), ReplicaRole::LEADER) << cluster.trace();
    ASSERT_EQ(cluster.core(0).commit_index(), 0);
}

void deliver_append_round_trip(RaftTestCluster& cluster, TestNodeId leader, TestNodeId follower) {
    TestMessageId request = *cluster.first_message(RaftTestMessageKind::APPEND_REQUEST, leader, follower);
    ASSERT_TRUE(cluster.deliver(request).ok());
    TestMessageId response = *cluster.first_message(RaftTestMessageKind::APPEND_RESPONSE, follower, leader);
    Status status = cluster.deliver(response);
    ASSERT_NE(status.code(), StatusCode::INVALID_ARGUMENT) << status.to_string();
}

TestMessageId enqueue_append_request(RaftTestCluster& cluster, TestNodeId from, TestNodeId to, Term term,
                                     std::vector<LogEntry> entries, LogIndex prev_index, Term prev_term,
                                     LogIndex leader_commit) {
    AppendEntriesParam request{cluster.replica_id(from),
                               cluster.replica_id(to),
                               term,
                               std::move(entries),
                               prev_index,
                               prev_term,
                               leader_commit};
    return cluster.enqueue(RaftTestEnvelope{0, from, to, AppendRequestPayload{std::move(request)}});
}

TestMessageId enqueue_vote_request(RaftTestCluster& cluster, TestNodeId from, TestNodeId to, Term term) {
    RequestVoteParam request{cluster.replica_id(from), cluster.replica_id(to), term, 0, 0};
    return cluster.enqueue(RaftTestEnvelope{0, from, to, VoteRequestPayload{request}});
}

TestMessageId enqueue_snapshot_request(RaftTestCluster& cluster, TestNodeId from, TestNodeId to, Term term,
                                       RaftTestSnapshot snapshot) {
    InstallSnapshotParam request;
    request.from_replica_id = cluster.replica_id(from);
    request.to_replica_id = cluster.replica_id(to);
    request.term = term;
    request.snapshot_index = snapshot.index;
    request.snapshot_term = snapshot.term;
    return cluster.enqueue(RaftTestEnvelope{0, from, to, SnapshotRequestPayload{request, std::move(snapshot)}});
}

// AdvisKV ReadIndex boundary corresponding to etcd TestReadOnlyForNewLeader.
// 场景：3 节点集群刚选出新 leader，但当前 term 的 no-op 尚未得到多数派确认。
// 过程：客户端立即请求构造 ReadIndex。
// 预期：leader 拒绝读屏障，避免基于尚未确认领导权的状态提供线性一致读。
TEST(RaftReadProtocolTest, NewLeaderRejectsReadBeforeCurrentTermEntryCommits) {
    RaftTestCluster cluster = RaftTestCluster::voters(3, 731);
    become_uncommitted_leader(cluster);
    RaftEffects effects;
    LogIndex read_index = -1;
    Term read_term = -1;

    Status status = cluster.core(0).build_append_entries_for_read(effects, read_index, read_term);

    EXPECT_EQ(status.code(), StatusCode::NOT_YET_COMMIT);
    EXPECT_TRUE(effects.messages.empty());
    EXPECT_EQ(read_index, -1);
    EXPECT_EQ(read_term, -1);
}

// 场景：新 leader 的当前 term no-op 已经提交，领导权得到多数派确认。
// 过程：请求 ReadIndex，并检查 leader 生成的发往其他 voter 的 quorum probe。
// 预期：返回当前 commit index，且探测消息可用于再次确认当前任期的多数派可达性。
TEST(RaftReadProtocolTest, CommittedLeaderReturnsReadIndexAndBuildsQuorumProbe) {
    RaftTestCluster cluster = RaftTestCluster::voters(3, 732);
    become_uncommitted_leader(cluster);
    deliver_append_round_trip(cluster, 0, 1);
    ASSERT_EQ(cluster.core(0).commit_index(), 1);
    RaftEffects effects;
    LogIndex read_index = -1;
    Term read_term = -1;

    Status status = cluster.core(0).build_append_entries_for_read(effects, read_index, read_term);

    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(read_index, 1);
    EXPECT_EQ(read_term, 1);
    ASSERT_EQ(effects.messages.size(), 2U);
    for (const RaftMessage& message : effects.messages) {
        EXPECT_EQ(message.type, RaftMessageType::APPEND_ENTRIES);
        EXPECT_EQ(message.append_param.term, 1);
        EXPECT_EQ(message.append_param.leader_commit, 1);
    }
}

// 场景：节点 0 是当前 term leader，复制日志或 snapshot 时收到同 term 的失败响应。
// 过程：依次投递 AppendEntries rejection 和 Snapshot failure response。
// 预期：失败只调整复制进度，不应把仍处于当前 term 的 leader 错误降级。
TEST(RaftReadProtocolTest, SameTermReplicationFailuresDoNotDemoteLeader) {
    RaftTestCluster cluster = RaftTestCluster::voters(3, 733);
    cluster.campaign(0);
    cluster.deliver_all();
    ASSERT_TRUE(cluster.core(0).is_leader());
    AppendEntriesParam append_request{cluster.replica_id(0), cluster.replica_id(1), 1, {}, 1, 1, 1};
    AppendEntriesResult append_reject{1, false, 0};
    RaftEffects append_effects;

    Status append_status = cluster.core(0).handle_append_response(cluster.replica_id(1), append_request, append_reject,
                                                                  append_effects);
    EXPECT_TRUE(append_status.ok());
    EXPECT_TRUE(cluster.core(0).is_leader());

    InstallSnapshotParam snapshot_request;
    snapshot_request.from_replica_id = cluster.replica_id(0);
    snapshot_request.to_replica_id = cluster.replica_id(1);
    snapshot_request.term = 1;
    snapshot_request.snapshot_index = 1;
    snapshot_request.snapshot_term = 1;
    InstallSnapshotResult snapshot_reject{1, Status::RPC_ERROR("send failed"), 0};
    RaftEffects snapshot_effects;
    cluster.core(0).handle_install_snapshot_response(cluster.replica_id(1), snapshot_request, snapshot_reject,
                                                     snapshot_effects);

    EXPECT_TRUE(cluster.core(0).is_leader());
    EXPECT_EQ(cluster.core(0).current_term(), 1);
}

// 场景：节点以 recovering 状态启动，尚未通过日志或 snapshot 达到恢复目标。
// 过程：触发 election tick、接收投票请求，并在本节点尝试 proposal。
// 预期：节点不发起竞选、不投票且拒绝写提议，避免残缺状态参与协议决策。
TEST(RaftRecoveryProtocolTest, RecoveringNodeDoesNotCampaignVoteOrPropose) {
    std::vector<PeerMember> members{make_test_member(734, 0), make_test_member(734, 1)};
    RaftTestCluster cluster;
    RaftTestNodeSpec leader;
    leader.self = members[0];
    leader.initial_voters = members;
    cluster.add_node(std::move(leader));
    RaftTestNodeSpec recovering;
    recovering.self = members[1];
    recovering.initial_voters = members;
    recovering.recovering = true;
    cluster.add_node(std::move(recovering));

    for (int i = 0; i < 20; ++i)
        cluster.tick(1);
    EXPECT_EQ(cluster.core(1).role(), ReplicaRole::FOLLOWER);
    EXPECT_EQ(cluster.core(1).current_term(), 0);

    ASSERT_TRUE(cluster.deliver(enqueue_vote_request(cluster, 0, 1, 2)).ok());
    TestMessageId response_id = *cluster.first_message(RaftTestMessageKind::VOTE_RESPONSE, 1, 0);
    const RequestVoteResult& response =
            std::get<VoteResponsePayload>(pending_message(cluster, response_id).payload).response;
    EXPECT_FALSE(response.vote_granted);
    EXPECT_EQ(response.term, 2);

    RaftTestProposalResult proposal = cluster.propose(1, ProposeParam::write(WriteOpType::PUT, "blocked", "value"));
    EXPECT_EQ(proposal.status.code(), StatusCode::IS_RECOVERING);
    EXPECT_EQ(proposal.index, -1);
}

// 场景：recovering follower 缺少恢复目标之前的日志，leader 按顺序发送缺失 entries。
// 过程：投递 AppendEntries 直至 follower 的日志和 commit 覆盖 recovery target。
// 预期：节点在真正追平后离开 recovering，恢复正常 follower 行为。
TEST(RaftRecoveryProtocolTest, AppendEntriesFinishesRecoveryAfterLogCatchesUp) {
    RaftTestCluster cluster = RaftTestCluster::voters(2, 735);
    cluster.enter_recovering(1);
    std::vector<LogEntry> entries{make_test_entry(1, 1, WriteOpType::PUT, "one", "1"),
                                  make_test_entry(1, 2, WriteOpType::PUT, "two", "2")};

    ASSERT_TRUE(cluster.deliver(enqueue_append_request(cluster, 0, 1, 1, entries, 0, 0, 2)).ok());

    EXPECT_TRUE(cluster.core(1).is_ready());
    EXPECT_EQ(cluster.core(1).commit_index(), 2);
    EXPECT_EQ(cluster.core(1).last_log_index(), 2);
    cluster.apply(1);
    EXPECT_EQ(cluster.kv(1).at("one"), "1");
    EXPECT_EQ(cluster.kv(1).at("two"), "2");
}

// 场景：recovering follower 无法仅靠现存日志追平，leader 向它发送覆盖目标的 snapshot。
// 过程：安装带成员和 KV image 的 snapshot，并处理安装完成后的状态切换。
// 预期：节点恢复 snapshot 状态和成员集合，跨过 recovery target 后回到 ready。
TEST(RaftRecoveryProtocolTest, SnapshotFinishesRecoveryAndRestoresState) {
    RaftTestCluster cluster = RaftTestCluster::voters(2, 736);
    cluster.enter_recovering(1);
    RaftTestSnapshot snapshot{5, 2, cluster.core(0).raft_members(), {{"snapshot", "restored"}}};

    ASSERT_TRUE(cluster.deliver(enqueue_snapshot_request(cluster, 0, 1, 2, snapshot)).ok());

    EXPECT_TRUE(cluster.core(1).is_ready());
    EXPECT_EQ(cluster.core(1).snapshot_index(), 5);
    EXPECT_EQ(cluster.core(1).commit_index(), 5);
    EXPECT_EQ(cluster.core(1).last_applied(), 5);
    EXPECT_EQ(cluster.kv(1).at("snapshot"), "restored");
}

// Expected by the AdvisKV recovering contract: a node remains recovering until
// the leader's target is covered. Current behavior has no recovery watermark;
// any accepted empty heartbeat exits recovery without log or commit progress
// (raft_core_append.cpp/recovery.cpp). There is no direct etcd counterpart.
// 场景：recovering 节点只收到不含日志、也不含 snapshot 的 heartbeat。
// 过程：heartbeat 携带较高 commit，但没有提供恢复目标所需的数据。
// 预期：节点应继续 recovering；当前实现会仅凭 commit 提前 ready，故禁用以记录缺陷。
TEST(RaftRecoveryProtocolTest, DISABLED_HeartbeatWithoutRecoveryDataKeepsNodeRecovering) {
    RaftTestCluster cluster = RaftTestCluster::voters(2, 737);
    cluster.enter_recovering(1);

    ASSERT_TRUE(cluster.deliver(enqueue_append_request(cluster, 0, 1, 1, {}, 0, 0, 5)).ok());

    EXPECT_TRUE(cluster.core(1).is_recovering());
    EXPECT_EQ(cluster.core(1).commit_index(), 0);
    EXPECT_EQ(cluster.core(1).last_log_index(), 0);
}

}  // namespace
}  // namespace adviskv::storage::test
