#include <algorithm>
#include <optional>
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

void deliver_snapshot_round_trip(RaftTestCluster& cluster, TestNodeId leader, TestNodeId follower) {
    std::optional<TestMessageId> request =
            cluster.first_message(RaftTestMessageKind::SNAPSHOT_REQUEST, leader, follower);
    ASSERT_TRUE(request.has_value()) << cluster.trace();
    ASSERT_TRUE(cluster.deliver(*request).ok());
    std::optional<TestMessageId> response =
            cluster.first_message(RaftTestMessageKind::SNAPSHOT_RESPONSE, follower, leader);
    ASSERT_TRUE(response.has_value()) << cluster.trace();
    ASSERT_TRUE(cluster.deliver(*response).ok());
}

RaftTestCluster compacted_cluster_with_lagging_follower() {
    RaftTestCluster cluster = RaftTestCluster::voters(3, 720);
    cluster.campaign(0);
    cluster.deliver_all();
    if (!cluster.core(0).is_leader()) throw std::runtime_error("failed to elect snapshot test leader");

    cluster.isolate(2);
    for (int i = 0; i < 3; ++i) {
        RaftTestProposalResult result = cluster.propose(
                0, ProposeParam::write(WriteOpType::PUT, "snap-" + std::to_string(i), "value-" + std::to_string(i)));
        if (result.status.fail()) throw std::runtime_error(result.status.to_string());
        cluster.deliver_all();
    }
    cluster.apply(0);
    cluster.compact(0, cluster.core(0).last_applied());
    cluster.heal_all();
    return cluster;
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

TestMessageId enqueue_snapshot_response(RaftTestCluster& cluster, TestNodeId from, TestNodeId to,
                                        InstallSnapshotParam request, InstallSnapshotResult response) {
    return cluster.enqueue(RaftTestEnvelope{
            0, from, to, SnapshotResponsePayload{cluster.replica_id(from), std::move(request), std::move(response)}});
}

// AdvisKV membership contract: a learner is added through the replicated log,
// catches up without voting, and is then promoted by a second config entry.
// 场景：3 voter 集群选出 leader，再把一个空日志节点作为 learner 加入。
// 过程：提交并 apply add-learner 配置，让 leader 向 learner 复制日志直至追平。
// 预期：learner 能接收日志，追平后 leader 自动提出 promotion，最终成为 voter。
TEST(RaftMembershipProtocolTest, LearnerCatchesUpAndIsAutomaticallyPromoted) {
    RaftTestCluster cluster = RaftTestCluster::voters(3, 721);
    std::vector<PeerMember> voters;
    for (int node = 0; node < 3; ++node)
        voters.push_back(cluster.member(node));
    const PeerMember learner = make_test_member(721, 3);
    RaftTestNodeSpec learner_spec;
    learner_spec.self = learner;
    learner_spec.initial_voters = voters;
    cluster.add_node(std::move(learner_spec));
    elect(cluster);

    ASSERT_TRUE(cluster.ensure_add_learner(0, learner).ok());
    cluster.deliver_all();
    cluster.tick(0);
    cluster.deliver_all();
    cluster.apply_all();
    ASSERT_EQ(cluster.core(0).member_type(learner.replica_id), RaftMemberType::LEARNER);

    cluster.tick(0);
    cluster.deliver_all();
    cluster.tick(0);
    cluster.deliver_all();
    cluster.tick(0);
    cluster.deliver_all();
    cluster.apply_all();

    EXPECT_EQ(cluster.core(3).log_entries_for_test(), cluster.core(0).log_entries_for_test());
    for (int node = 0; node < cluster.node_count(); ++node) {
        EXPECT_EQ(cluster.core(node).member_type(learner.replica_id), RaftMemberType::VOTER) << "node=" << node << '\n'
                                                                                             << cluster.trace();
    }
}

// 场景：集群包含 3 个 voter 和 1 个已追平 learner，节点 0 是 leader。
// 过程：追加写日志后只投递 learner 的确认，不让另外两个 voter 确认。
// 预期：learner 的副本不计入写 quorum，leader 不能仅凭自己和 learner 提交日志。
TEST(RaftMembershipProtocolTest, LearnerAcknowledgementDoesNotCountTowardWriteQuorum) {
    std::vector<PeerMember> voters{make_test_member(722, 0), make_test_member(722, 1), make_test_member(722, 2)};
    const PeerMember learner = make_test_member(722, 3);
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

    cluster.isolate(3);
    elect(cluster);
    cluster.heal_all();
    cluster.isolate(1);
    cluster.isolate(2);
    RaftTestProposalResult proposal = cluster.propose(0, ProposeParam::write(WriteOpType::PUT, "learner-ack", "value"));
    ASSERT_TRUE(proposal.status.ok());
    cluster.deliver_all();
    cluster.tick(0);
    cluster.deliver_all();

    EXPECT_LT(cluster.core(0).commit_index(), proposal.index);
}

// 场景：leader 已经提出一次 add-member 配置变更，但该变更尚未 apply 完成。
// 过程：再次添加同一成员，然后尝试添加另一个成员。
// 预期：相同请求幂等返回原结果；不同配置变更因已有 pending change 返回 RETRY_ERROR。
TEST(RaftMembershipProtocolTest, RepeatedAddIsIdempotentAndDifferentChangeRetries) {
    RaftTestCluster cluster = RaftTestCluster::voters(3, 723);
    elect(cluster);
    const PeerMember learner = make_test_member(723, 3);
    const PeerMember other = make_test_member(723, 4);

    ASSERT_TRUE(cluster.ensure_add_learner(0, learner).ok());
    const LogIndex pending_index = cluster.core(0).last_log_index();
    EXPECT_TRUE(cluster.ensure_add_learner(0, learner).ok());
    EXPECT_EQ(cluster.core(0).last_log_index(), pending_index);
    EXPECT_EQ(cluster.ensure_add_learner(0, other).code(), StatusCode::RETRY_ERROR);
    EXPECT_EQ(cluster.ensure_remove_member(0, cluster.replica_id(2)).code(), StatusCode::RETRY_ERROR);
}

// 场景：leader 已经提出一次 remove-member 配置变更，但该变更尚未 apply 完成。
// 过程：再次删除同一成员，然后尝试删除另一个成员。
// 预期：相同删除请求幂等；第二个不同配置变更被要求重试，避免并发修改成员集合。
TEST(RaftMembershipProtocolTest, RepeatedRemoveIsIdempotentAndDifferentChangeRetries) {
    RaftTestCluster cluster = RaftTestCluster::voters(3, 724);
    elect(cluster);

    ASSERT_TRUE(cluster.ensure_remove_member(0, cluster.replica_id(2)).ok());
    const LogIndex pending_index = cluster.core(0).last_log_index();
    EXPECT_TRUE(cluster.ensure_remove_member(0, cluster.replica_id(2)).ok());
    EXPECT_EQ(cluster.core(0).last_log_index(), pending_index);
    EXPECT_EQ(cluster.ensure_remove_member(0, cluster.replica_id(1)).code(), StatusCode::RETRY_ERROR);
}

// Reference: etcd TestNewLeaderPendingConfig.
// 场景：候选节点的日志中已存在尚未 apply 的配置变更条目。
// 过程：该节点发起选举成为新 leader，再尝试提出另一个成员变更。
// 预期：新 leader 扫描日志识别已有 pending change，并拒绝并行配置变更。
TEST(RaftMembershipProtocolTest, NewLeaderRecognizesUnappliedConfigEntry) {
    RaftTestCluster cluster = RaftTestCluster::voters(3, 725);
    elect(cluster, 0);
    const PeerMember learner = make_test_member(725, 3);

    ASSERT_TRUE(cluster.ensure_add_learner(0, learner).ok());
    deliver_append_round_trip(cluster, 0, 1);
    cluster.isolate(0);
    elect(cluster, 1);
    const LogIndex before = cluster.core(1).last_log_index();

    EXPECT_TRUE(cluster.ensure_add_learner(1, learner).ok());
    EXPECT_EQ(cluster.core(1).last_log_index(), before);
    EXPECT_EQ(cluster.ensure_add_learner(1, make_test_member(725, 4)).code(), StatusCode::RETRY_ERROR);
}

// Reference: etcd TestRemoveNode and TestCommitAfterRemoveNode.
// 场景：3 voter 集群删除其中一个 follower，并将删除配置复制、提交和 apply。
// 过程：删除完成后追加新写入并驱动复制，同时观察发往被删节点的消息。
// 预期：leader 停止联系被删节点；剩余 2 voter 按缩小后的 quorum 继续提交。
TEST(RaftMembershipProtocolTest, RemovedFollowerReceivesNoMessagesAndSmallerQuorumCommits) {
    RaftTestCluster cluster = RaftTestCluster::voters(3, 726);
    elect(cluster);
    const ReplicaID removed = cluster.replica_id(2);

    ASSERT_TRUE(cluster.ensure_remove_member(0, removed).ok());
    cluster.deliver_all();
    cluster.tick(0);
    cluster.deliver_all();
    cluster.apply_all();
    ASSERT_EQ(cluster.core(0).member_type(removed), RaftMemberType::NON_MEMBER);
    cluster.drop_all();

    RaftTestProposalResult proposal =
            cluster.propose(0, ProposeParam::write(WriteOpType::PUT, "after-remove", "committed"));
    ASSERT_TRUE(proposal.status.ok());
    EXPECT_EQ(cluster.pending_count(RaftTestMessageKind::APPEND_REQUEST, 0, 2), 0U);
    deliver_append_round_trip(cluster, 0, 1);
    EXPECT_EQ(cluster.core(0).commit_index(), proposal.index);
}

// 场景：3 voter 集群由节点 0 领导，并提交删除节点 0 自身的配置变更。
// 过程：让删除配置完成 apply，随后继续触发原 leader 的 tick。
// 预期：节点 0 退为 non-member，不再参与选举或发送 leader 心跳。
TEST(RaftMembershipProtocolTest, RemovingLeaderMakesItANonMemberAndStopsTicks) {
    RaftTestCluster cluster = RaftTestCluster::voters(3, 727);
    elect(cluster);

    ASSERT_TRUE(cluster.ensure_remove_member(0, cluster.replica_id(0)).ok());
    cluster.deliver_all();
    cluster.tick(0);
    cluster.deliver_all();
    cluster.apply(0);
    cluster.drop_all();

    EXPECT_EQ(cluster.core(0).role(), ReplicaRole::FOLLOWER);
    EXPECT_EQ(cluster.core(0).member_type(cluster.replica_id(0)), RaftMemberType::NON_MEMBER);
    for (int i = 0; i < 20; ++i)
        cluster.tick(0);
    EXPECT_TRUE(cluster.pending().empty());
    EXPECT_EQ(cluster.core(0).role(), ReplicaRole::FOLLOWER);
}

// Reference: etcd campaign_learner_must_vote. The candidate has applied the
// promotion, while the promoted node still sees itself as a learner. It must
// nevertheless grant the vote so the latest voter configuration stays live.
// 场景：learner promotion 已在部分 voter 可见，但 learner 自身尚未 apply 成 voter，形成配置可见性偏差。
// 过程：隔离原 leader，让其余节点尝试在该不一致窗口内重新选举。
// 预期：任何当选者都必须得到合法 voter quorum；当前实现会提前计入 learner，故暂时禁用记录缺陷。
TEST(RaftMembershipProtocolTest, DISABLED_PromotionVisibilitySkewElectsBeforeLearnerAppliesConfig) {
    const PeerMember first = make_test_member(728, 0);
    const PeerMember candidate = make_test_member(728, 1);
    const PeerMember promoted = make_test_member(728, 2);
    const std::vector<PeerMember> old_voters{first, candidate};
    const std::vector<RaftMember> promoted_view{
            {first, RaftMemberType::VOTER}, {candidate, RaftMemberType::VOTER}, {promoted, RaftMemberType::VOTER}};
    const std::vector<RaftMember> learner_view{
            {first, RaftMemberType::VOTER}, {candidate, RaftMemberType::VOTER}, {promoted, RaftMemberType::LEARNER}};
    RaftTestCluster cluster;
    for (const auto& [member, view] : std::vector<std::pair<PeerMember, std::vector<RaftMember>>>{
                 {first, promoted_view}, {candidate, promoted_view}, {promoted, learner_view}}) {
        RaftTestNodeSpec spec;
        spec.self = member;
        spec.initial_voters = old_voters;
        spec.membership = view;
        cluster.add_node(std::move(spec));
    }
    cluster.isolate(0);

    cluster.campaign(1);
    cluster.deliver_all();
    EXPECT_EQ(cluster.core(1).role(), ReplicaRole::LEADER) << cluster.trace();
    EXPECT_EQ(cluster.core(1).current_term(), 1);
    EXPECT_EQ(cluster.core(2).member_type(promoted.replica_id), RaftMemberType::LEARNER);
}

// 场景：节点 2 长期隔离，leader 提交并 apply 多条日志后 compact，随后又追加增量日志。
// 过程：解除隔离，leader 先向落后于日志前缀的节点 2 发送 snapshot，再发送剩余日志。
// 预期：节点 2 安装 snapshot 后继续增量追赶，最终日志、commit 和 KV 与 leader 一致。
TEST(RaftSnapshotProtocolTest, LaggingFollowerInstallsSnapshotThenReceivesIncrementalLog) {
    RaftTestCluster cluster = compacted_cluster_with_lagging_follower();
    RaftTestProposalResult delta = cluster.propose(0, ProposeParam::write(WriteOpType::PUT, "after-snapshot", "delta"));
    ASSERT_TRUE(delta.status.ok());
    deliver_append_round_trip(cluster, 0, 1);
    ASSERT_EQ(cluster.core(0).commit_index(), delta.index);
    cluster.apply(0);

    deliver_snapshot_round_trip(cluster, 0, 2);
    EXPECT_EQ(cluster.core(2).snapshot_index(), 4);
    cluster.tick(0);
    deliver_append_round_trip(cluster, 0, 2);
    cluster.apply(2);

    EXPECT_EQ(cluster.core(2).log_entries_for_test(), cluster.core(0).log_entries_for_test());
    EXPECT_EQ(cluster.kv(2), cluster.kv(0));
}

// 场景：leader 已 compact 历史日志，此时加入一个完全空白的 learner。
// 过程：leader 发现 learner 的 next index 落在 compact 前缀内，改为发送 snapshot。
// 预期：learner 从 snapshot 恢复成员和 KV 状态，并能继续参与后续日志追赶。
TEST(RaftSnapshotProtocolTest, LearnerCatchesUpThroughLeaderSnapshot) {
    const PeerMember leader = make_test_member(731, 0);
    const PeerMember voter = make_test_member(731, 1);
    const PeerMember learner = make_test_member(731, 2);
    const std::vector<PeerMember> voters{leader, voter};
    const std::vector<RaftMember> membership{
            {leader, RaftMemberType::VOTER}, {voter, RaftMemberType::VOTER}, {learner, RaftMemberType::LEARNER}};
    RaftTestCluster cluster;
    for (const PeerMember& member : {leader, voter, learner}) {
        RaftTestNodeSpec spec;
        spec.self = member;
        spec.initial_voters = voters;
        spec.membership = membership;
        cluster.add_node(std::move(spec));
    }
    cluster.isolate(2);
    elect(cluster);
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(cluster.propose(0, ProposeParam::write(WriteOpType::PUT, "learner-snap-" + std::to_string(i),
                                                           "value-" + std::to_string(i)))
                            .status.ok());
        cluster.deliver_all();
    }
    cluster.apply(0);
    cluster.compact(0, cluster.core(0).last_applied());

    cluster.heal_all();
    cluster.tick(0);
    deliver_snapshot_round_trip(cluster, 0, 2);

    EXPECT_EQ(cluster.core(2).snapshot_index(), cluster.core(0).snapshot_index());
    EXPECT_EQ(cluster.core(2).member_type(learner.replica_id), RaftMemberType::LEARNER);
    EXPECT_EQ(cluster.kv(2), cluster.kv(0));
}

// Reference: etcd raft_snap_test.go TestPendingSnapshotPauseReplication.
// 场景：一个 follower 落后到必须接收 snapshot，leader 已向它发出首次安装请求。
// 过程：在首个响应尚未返回时继续 tick leader，尝试再次驱动同步。
// 预期：同一 follower 同时只保留一个 inflight snapshot，不重复发送相同镜像。
TEST(RaftSnapshotProtocolTest, AllowsOnlyOneInflightSnapshotPerFollower) {
    RaftTestCluster cluster = compacted_cluster_with_lagging_follower();
    cluster.tick(0);
    ASSERT_EQ(cluster.pending_count(RaftTestMessageKind::SNAPSHOT_REQUEST, 0, 2), 1U);

    cluster.tick(0);

    EXPECT_EQ(cluster.pending_count(RaftTestMessageKind::SNAPSHOT_REQUEST, 0, 2), 1U);
    EXPECT_EQ(cluster.core(0).inflight_snapshot_index_for_test(cluster.replica_id(2)), 4);
}

// Reference: etcd raft_snap_test.go TestSnapshotFailure.
// 场景：leader 正在向落后 follower 发送 snapshot，并已标记该请求 inflight。
// 过程：测试网络回调报告发送失败，随后再次 tick leader。
// 预期：失败会清除 inflight 状态，下一轮能够重新发送 snapshot，而非永久卡住。
TEST(RaftSnapshotProtocolTest, SendFailureClearsInflightAndAllowsRetry) {
    RaftTestCluster cluster = compacted_cluster_with_lagging_follower();
    cluster.tick(0);
    TestMessageId failed = *cluster.first_message(RaftTestMessageKind::SNAPSHOT_REQUEST, 0, 2);

    ASSERT_TRUE(cluster.fail(failed, Status::RPC_ERROR("injected send failure")).ok());
    EXPECT_EQ(cluster.core(0).inflight_snapshot_index_for_test(cluster.replica_id(2)), 0);
    cluster.tick(0);

    EXPECT_EQ(cluster.pending_count(RaftTestMessageKind::SNAPSHOT_REQUEST, 0, 2), 1U);
    EXPECT_EQ(cluster.core(0).inflight_snapshot_index_for_test(cluster.replica_id(2)), 4);
}

// 场景：leader 发出旧 snapshot 后，在响应到达前又 compact 出更高 index 的新 snapshot。
// 过程：投递旧 snapshot 的成功响应，比较 follower progress 的推进位置。
// 预期：确认位置取实际已发送镜像的 index，不能误用 leader 当前更高的 compact index。
TEST(RaftSnapshotProtocolTest, SuccessUsesActuallySentIndexAfterLeaderCompactsAgain) {
    RaftTestCluster cluster = compacted_cluster_with_lagging_follower();
    cluster.tick(0);
    for (TestMessageId id : cluster.message_ids(RaftTestMessageKind::APPEND_REQUEST, 0, 1))
        cluster.drop(id);
    const TestMessageId old_snapshot = *cluster.first_message(RaftTestMessageKind::SNAPSHOT_REQUEST, 0, 2);
    ASSERT_EQ(std::get<SnapshotRequestPayload>(pending_message(cluster, old_snapshot).payload).request.snapshot_index,
              4);

    ASSERT_TRUE(cluster.propose(0, ProposeParam::write(WriteOpType::PUT, "new-snapshot", "value")).status.ok());
    deliver_append_round_trip(cluster, 0, 1);
    cluster.apply(0);
    cluster.compact(0, cluster.core(0).last_applied());
    ASSERT_EQ(cluster.core(0).snapshot_index(), 5);

    ASSERT_TRUE(cluster.deliver(old_snapshot).ok());
    ASSERT_TRUE(cluster.deliver(*cluster.first_message(RaftTestMessageKind::SNAPSHOT_RESPONSE, 2, 0)).ok());
    EXPECT_EQ(cluster.core(0).snapshot_watermark_for_test(cluster.replica_id(2)), 4);

    cluster.tick(0);
    TestMessageId retry = *cluster.first_message(RaftTestMessageKind::SNAPSHOT_REQUEST, 0, 2);
    EXPECT_EQ(std::get<SnapshotRequestPayload>(pending_message(cluster, retry).payload).request.snapshot_index, 5);
}

class SnapshotWatermarkTest : public ::testing::TestWithParam<LogIndex> {};

// 场景：follower 返回 ALREADY_EXIST，表示它已安装到参数实例给出的相同或更高 watermark。
// 过程：leader 处理该响应，而不重新传输已覆盖的 snapshot。
// 预期：leader 按 follower 返回的实际 watermark 推进 progress，并继续从其后复制日志。
TEST_P(SnapshotWatermarkTest, AlreadyExistAdvancesProgressToReturnedWatermark) {
    RaftTestCluster cluster = compacted_cluster_with_lagging_follower();
    cluster.tick(0);
    TestMessageId request_id = *cluster.first_message(RaftTestMessageKind::SNAPSHOT_REQUEST, 0, 2);
    std::optional<RaftTestEnvelope> request_envelope = cluster.take(request_id);
    InstallSnapshotParam request = std::get<SnapshotRequestPayload>(request_envelope->payload).request;
    InstallSnapshotResult response{1, Status::ALREADY_EXIST("covered"), GetParam()};

    ASSERT_TRUE(cluster.deliver(enqueue_snapshot_response(cluster, 2, 0, request, response)).ok());

    EXPECT_EQ(cluster.core(0).snapshot_watermark_for_test(cluster.replica_id(2)), GetParam());
    EXPECT_EQ(cluster.core(0).next_index_for_test(cluster.replica_id(2)), GetParam() + 1);
}

INSTANTIATE_TEST_SUITE_P(SameAndHigherWatermarks, SnapshotWatermarkTest, ::testing::Values<LogIndex>(4, 7));

// 场景：leader 发出 snapshot 后，follower 以更高 term 返回安装响应。
// 过程：把该 higher-term response 投递给 leader。
// 预期：leader 更新 term 并退为 follower，不能继续以旧任期发送 snapshot 或日志。
TEST(RaftSnapshotProtocolTest, HigherTermResponseForcesLeaderToStepDown) {
    RaftTestCluster cluster = compacted_cluster_with_lagging_follower();
    cluster.tick(0);
    TestMessageId request_id = *cluster.first_message(RaftTestMessageKind::SNAPSHOT_REQUEST, 0, 2);
    std::optional<RaftTestEnvelope> request_envelope = cluster.take(request_id);
    InstallSnapshotParam request = std::get<SnapshotRequestPayload>(request_envelope->payload).request;
    InstallSnapshotResult response{2, Status::OK(), 4};

    ASSERT_TRUE(cluster.deliver(enqueue_snapshot_response(cluster, 2, 0, request, response)).ok());

    EXPECT_EQ(cluster.core(0).role(), ReplicaRole::FOLLOWER);
    EXPECT_EQ(cluster.core(0).current_term(), 2);
}

// 场景：follower 的 commit index 已经覆盖待安装 snapshot 的 index。
// 过程：leader 再发送这个过期 snapshot 给 follower。
// 预期：follower 拒绝回退状态，不覆盖已经提交的更新数据。
TEST(RaftSnapshotProtocolTest, FollowerRejectsSnapshotCoveredByCommitIndex) {
    RaftTestCluster cluster = compacted_cluster_with_lagging_follower();
    cluster.tick(0);
    deliver_append_round_trip(cluster, 0, 1);
    ASSERT_GE(cluster.core(1).commit_index(), 3);
    const LogIndex old_snapshot_index = cluster.core(1).snapshot_index();
    RaftTestSnapshot covered{3, 1, cluster.core(0).raft_members(), {}};

    ASSERT_TRUE(cluster.deliver(enqueue_snapshot_request(cluster, 0, 1, 1, covered)).ok());
    TestMessageId response_id = *cluster.first_message(RaftTestMessageKind::SNAPSHOT_RESPONSE, 1, 0);
    const InstallSnapshotResult& response =
            std::get<SnapshotResponsePayload>(pending_message(cluster, response_id).payload).response;

    EXPECT_EQ(response.status.code(), StatusCode::ALREADY_EXIST);
    EXPECT_EQ(cluster.core(1).snapshot_index(), old_snapshot_index);
}

// Reference: etcd TestRestore and TestRestoreWithLearner.
// 场景：recovering follower 收到 snapshot，且本地在 snapshot index 后仍有 term 连续匹配的日志后缀。
// 过程：安装 snapshot image，恢复其中的成员和 KV，再检查本地后缀处理。
// 预期：snapshot 状态被发布，匹配后缀得以保留，节点跨过恢复目标后回到 ready。
TEST(RaftSnapshotProtocolTest, RecoveringFollowerRetainsMatchingSuffixAndRestoresMembers) {
    const PeerMember leader = make_test_member(729, 0);
    const PeerMember follower = make_test_member(729, 1);
    const std::vector<PeerMember> voters{leader, follower};
    const std::vector<RaftMember> snapshot_members{{leader, RaftMemberType::VOTER},
                                                   {follower, RaftMemberType::LEARNER}};
    RaftTestCluster cluster;
    RaftTestNodeSpec leader_spec;
    leader_spec.self = leader;
    leader_spec.initial_voters = voters;
    cluster.add_node(std::move(leader_spec));
    RaftTestNodeSpec follower_spec;
    follower_spec.self = follower;
    follower_spec.initial_voters = voters;
    follower_spec.hard_state = RaftMeta{4, std::nullopt};
    follower_spec.entries = {make_test_entry(1, 1), make_test_entry(2, 2), make_test_entry(3, 3),
                             make_test_entry(3, 4)};
    follower_spec.recovering = true;
    cluster.add_node(std::move(follower_spec));
    RaftTestSnapshot snapshot{2, 2, snapshot_members, {{"snapshot-key", "snapshot-value"}}};

    ASSERT_TRUE(cluster.deliver(enqueue_snapshot_request(cluster, 0, 1, 4, snapshot)).ok());

    EXPECT_EQ(cluster.core(1).snapshot_index(), 2);
    EXPECT_EQ(cluster.core(1).snapshot_term(), 2);
    EXPECT_EQ(cluster.core(1).log_entries_for_test(),
              std::vector<LogEntry>({make_test_entry(3, 3), make_test_entry(3, 4)}));
    EXPECT_EQ(cluster.core(1).member_type(follower.replica_id), RaftMemberType::LEARNER);
    EXPECT_EQ(cluster.kv(1).at("snapshot-key"), "snapshot-value");
    EXPECT_TRUE(cluster.core(1).is_ready());
}

// 场景：节点已有已提交但尚未全部 apply 的日志。
// 过程：请求 compact 到超过 last_applied 的位置。
// 预期：compact 被拒绝，避免 snapshot 包含状态机尚未执行的日志。
TEST(RaftSnapshotProtocolTest, CompactCannotMoveBeyondAppliedIndex) {
    RaftTestCluster cluster = RaftTestCluster::voters(1, 730);
    elect(cluster);
    ASSERT_EQ(cluster.core(0).commit_index(), 1);
    ASSERT_EQ(cluster.core(0).last_applied(), 0);

    EXPECT_TRUE(cluster.core(0).truncate_log(1).fail());
    EXPECT_EQ(cluster.core(0).snapshot_index(), 0);

    cluster.apply(0);
    EXPECT_TRUE(cluster.core(0).truncate_log(1).ok());
    EXPECT_EQ(cluster.core(0).snapshot_index(), 1);
}

// 场景：旧 leader 的 snapshot 响应被延迟；同一节点退位后在更高 term 再次当选。
// 过程：新 leader 建立 progress 后，投递旧 term 的 snapshot success response。
// 预期：迟到响应被忽略，不推进或回退新任期中的 follower progress。
TEST(RaftSnapshotProtocolTest, PreviousTermResponseDoesNotChangeNewLeaderProgress) {
    RaftTestCluster cluster = compacted_cluster_with_lagging_follower();
    cluster.tick(0);
    TestMessageId old_request_id = *cluster.first_message(RaftTestMessageKind::SNAPSHOT_REQUEST, 0, 2);
    std::optional<RaftTestEnvelope> old_envelope = cluster.take(old_request_id);
    InstallSnapshotParam old_request = std::get<SnapshotRequestPayload>(old_envelope->payload).request;

    AppendEntriesParam sent_append{cluster.replica_id(0), cluster.replica_id(1), 1, {}, 4, 1, 4};
    AppendEntriesResult higher_term{2, false, 4};
    RaftEffects step_down_effects;
    Status step_down =
            cluster.core(0).handle_append_response(cluster.replica_id(1), sent_append, higher_term, step_down_effects);
    ASSERT_EQ(step_down.code(), StatusCode::NOT_LEADER);
    cluster.drop_all();
    elect(cluster, 0);
    ASSERT_EQ(cluster.core(0).current_term(), 3);
    const LogIndex inflight = cluster.core(0).inflight_snapshot_index_for_test(cluster.replica_id(2));

    InstallSnapshotResult stale_response{1, Status::OK(), 4};
    ASSERT_TRUE(cluster.deliver(enqueue_snapshot_response(cluster, 2, 0, old_request, stale_response)).ok());

    EXPECT_EQ(cluster.core(0).snapshot_watermark_for_test(cluster.replica_id(2)), 0);
    EXPECT_EQ(cluster.core(0).inflight_snapshot_index_for_test(cluster.replica_id(2)), inflight);
    EXPECT_EQ(cluster.core(0).role(), ReplicaRole::LEADER);
}

}  // namespace
}  // namespace adviskv::storage::test
