#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status.h"
#include "common/type.h"
#include "storage/model/param.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/raft_node.h"
#include "test_env.h"

namespace adviskv::storage {
namespace fs = std::filesystem;

// 这边这个raft是逐个tick的测试，所以流程上来说没有测试raft的并发性。
// 只是保证了raft的正确性

class RaftCluster {
   public:
    RaftCluster() = default;

    // 创建一个有num_nodes节点数量的raft group
    void create(int num_nodes, int table_id) {
        table_id_ = table_id;
        int shard_index = 0;

        all_members_.clear();
        for (int i = 0; i < num_nodes; i++) {
            ReplicaID rid{table_id_, shard_index, i};
            PeerMember member;
            member.node_id = "node_" + std::to_string(i);
            member.replica_id = rid;
            member.endpoint = {"127.0.0.1", 9000 + i};
            all_members_.push_back(member);
        }

        persists_.clear();
        nodes_.clear();
        active_.clear();
        isolated_.clear();
        effect_outboxes_.clear();
        id_to_idx_.clear();
        temp_dirs_.clear();

        for (int i = 0; i < num_nodes; i++) {
            auto dir = adviskv::test::make_unique_test_dir(
                "raft_cluster_t" + std::to_string(table_id) + "_n" +
                    std::to_string(i),
                0);
            fs::create_directories(dir);
            temp_dirs_.push_back(dir);

            auto persist = std::make_unique<PersistEngine>(
                dir.string(), all_members_[i].replica_id);
            Status s = persist->init();
            ASSERT_TRUE(s.ok()) << s.to_string();

            auto node = std::make_unique<RaftNode>(all_members_[i].replica_id,
                                                   all_members_);

            persists_.push_back(std::move(persist));
            nodes_.push_back(std::move(node));
            active_.push_back(true);
            isolated_.push_back(false);
            effect_outboxes_.emplace_back();
            id_to_idx_[all_members_[i].replica_id] = i;
        }
    }

    // 这里对于isolate的节点也会tick，其实应该分的再仔细一点
    // 区分开来是网络分区还是宕机了
    void tick_node(int node_idx) {
        if (node_idx < 0 || node_idx >= static_cast<int>(nodes_.size())) {
            return;
        }
        if (!active_[node_idx]) return;

        RaftEffects effects;
        nodes_[node_idx]->tick(effects);
        Status status = drive_raft_effects(node_idx, std::move(effects));
        ASSERT_TRUE(status.ok()) << status.to_string();
    }

    void tick_all() {
        for (int i = 0; i < static_cast<int>(nodes_.size()); i++) {
            tick_node(i);
        }
        route_all_messages();
    }

    // 只推进本地 timer，不自动路由消息；用于需要精确控制投递顺序的协议测试。
    void tick_nodes_only() {
        for (int i = 0; i < static_cast<int>(nodes_.size()); i++) {
            tick_node(i);
        }
    }

    // tick，n轮
    void tick_n(int n) {
        for (int i = 0; i < n; i++) {
            tick_all();
        }
    }

    // 一直tick，直到出来了一个leader（candi是没有的）
    bool tick_until_stable(int max_rounds = 300) {
        for (int r = 0; r < max_rounds; r++) {
            tick_all();

            int leaders = 0;
            int candidates = 0;
            for (int i = 0; i < static_cast<int>(nodes_.size()); i++) {
                if (!active_[i] || isolated_[i]) continue;
                auto role = nodes_[i]->role();
                if (role == ReplicaRole::LEADER) leaders++;
                if (role == ReplicaRole::CANDIDATE) candidates++;
            }

            if (leaders == 1 && candidates == 0) return true;
        }
        return false;
    }

    void isolate(int idx) { isolated_[idx] = true; }

    void restore(int idx) { isolated_[idx] = false; }

    void crash(int idx) { active_[idx] = false; }

    RaftNode* node_ptr(int idx) {
        if (idx < 0 || idx >= static_cast<int>(nodes_.size())) return nullptr;
        return nodes_[idx].get();
    }

    int size() const { return (int)nodes_.size(); }

    bool is_active(int idx) const { return active_[idx]; }

    bool is_isolate(int idx) const { return isolated_[idx]; }

    int leader_idx() const {
        for (int i = 0; i < static_cast<int>(nodes_.size()); i++) {
            if (!active_[i] || isolated_[i]) continue;
            if (nodes_[i]->is_leader()) return i;
        }
        return -1;
    }

    RaftNode* leader_ptr() {
        int idx = leader_idx();
        return idx >= 0 ? nodes_[idx].get() : nullptr;
    }

    int leader_count() const {
        int cnt = 0;
        for (int i = 0; i < static_cast<int>(nodes_.size()); i++) {
            if (!active_[i] || isolated_[i]) continue;
            if (nodes_[i]->is_leader()) cnt++;
        }
        return cnt;
    }

    std::vector<LogIndex> all_commit_indices() const {
        std::vector<LogIndex> result;
        for (int i = 0; i < static_cast<int>(nodes_.size()); i++) {
            if (!active_[i] || isolated_[i]) continue;
            result.push_back(nodes_[i]->commit_index());
        }
        return result;
    }

    // 判断当前group中没有宕机的节点里面，是否最新的commit_idx都是大于等于target的
    bool all_committed_at_least(LogIndex target) const {
        for (int i = 0; i < static_cast<int>(nodes_.size()); i++) {
            if (!active_[i] || isolated_[i]) continue;
            if (nodes_[i]->commit_index() < target) return false;
        }
        return true;
    }

    int find_node(const ReplicaID& rid) const {
        auto it = id_to_idx_.find(rid);
        return (it != id_to_idx_.end()) ? it->second : -1;
    }

    ReplicaID replica_id(int idx) const { return all_members_[idx].replica_id; }

    Status drive_raft_effects(int node_idx, RaftEffects effects) {
        std::lock_guard lock(effect_mutex_);
        if (effects.entries_to_rewrite.has_value() &&
            !effects.entries_to_append.empty()) {
            return Status::INVALID_ARGUMENT(
                "cannot append and rewrite raft log in one effects batch");
        }
        if (effects.hard_state.has_value()) {
            RETURN_IF_INVALID_STATUS(
                persists_[node_idx]->save_raft_meta(*effects.hard_state))
        }
        if (effects.entries_to_rewrite.has_value()) {
            RETURN_IF_INVALID_STATUS(
                persists_[node_idx]->rewrite_wal(*effects.entries_to_rewrite))
        }
        if (!effects.entries_to_append.empty()) {
            RETURN_IF_INVALID_STATUS(persists_[node_idx]->append_wal_batch(
                effects.entries_to_append))
        }
        for (RaftMessage& msg : effects.messages) {
            effect_outboxes_[node_idx].push_back(std::move(msg));
        }
        return Status::OK();
    }

    std::pair<Status, LogIndex> propose(int node_idx, WriteOpType op_type,
                                        Key key, Value value) {
        RaftEffects effects;
        auto result = nodes_[node_idx]->propose(op_type, key, value, effects);
        if (result.first.fail()) return result;

        Status status = drive_raft_effects(node_idx, std::move(effects));
        if (status.fail()) return {status, -1};
        return result;
    }

    int route_messages_from(
        int source_idx,
        const std::function<bool(const RaftMessage&, int)>& should_route) {
        if (source_idx < 0 || source_idx >= static_cast<int>(nodes_.size())) {
            return 0;
        }
        if (!active_[source_idx] || isolated_[source_idx]) return 0;

        std::vector<int> routed_targets;
        std::vector<RaftMessage> messages;
        messages.swap(effect_outboxes_[source_idx]);
        effect_outboxes_[source_idx].clear();
        for (auto& msg : messages) {
            int target_idx = find_node(msg.target.replica_id);
            if (target_idx < 0 || !active_[target_idx] ||
                isolated_[target_idx]) {
                continue;
            }
            if (!should_route(msg, target_idx)) continue;

            route_message(source_idx, target_idx, msg);
            if (std::find(routed_targets.begin(), routed_targets.end(),
                          target_idx) == routed_targets.end()) {
                routed_targets.push_back(target_idx);
            }
        }
        return static_cast<int>(routed_targets.size());
    }

    // 提取出来source_idx发出的消息，指定类型是type
    // 提取出来的messages里面不满足的相当于就是直接剔除， 以后也不会出现了。
    int route_messages_from_to(int source_idx, RaftMessageType type,
                               const std::vector<int>& target_indices) {
        return route_messages_from(
            source_idx, [&](const RaftMessage& msg, int target_idx) {
                return msg.type == type &&
                       std::find(target_indices.begin(), target_indices.end(),
                                 target_idx) != target_indices.end();
            });
    }

    int drop_messages_from(int source_idx) {
        if (source_idx < 0 || source_idx >= static_cast<int>(nodes_.size())) {
            return 0;
        }
        int dropped = static_cast<int>(effect_outboxes_[source_idx].size());
        effect_outboxes_[source_idx].clear();
        return dropped;
    }

    static LogEntry make_entry(Term term, LogIndex index, WriteOpType op_type,
                               std::string key, std::string value) {
        return LogEntry{term, index, op_type, std::move(key), std::move(value)};
    }

    void cleanup_temp_dirs() {
        for (const auto& dir : temp_dirs_) {
            // TODO 这里有一个ec
            std::error_code ec;
            fs::remove_all(dir, ec);
        }
        temp_dirs_.clear();
    }

    ~RaftCluster() {
        nodes_.clear();
        persists_.clear();
        cleanup_temp_dirs();
    }

   private:
    void route_message(int source_idx, int target_idx, RaftMessage& msg) {
        switch (msg.type) {
            case RaftMessageType::REQUEST_VOTE: {
                RequestVoteResult result;
                RaftEffects target_effects;
                nodes_[target_idx]->handle_request_vote(msg.vote_param, result,
                                                        target_effects);
                Status target_status =
                    drive_raft_effects(target_idx, std::move(target_effects));
                ASSERT_TRUE(target_status.ok()) << target_status.to_string();
                RaftEffects response_effects;
                nodes_[source_idx]->handle_vote_response(
                    msg.target.replica_id, result, response_effects);
                Status status =
                    drive_raft_effects(source_idx, std::move(response_effects));
                ASSERT_TRUE(status.ok()) << status.to_string();
                break;
            }

            case RaftMessageType::APPEND_ENTRIES: {
                AppendEntriesResult result;
                RaftEffects effects;
                nodes_[target_idx]->handle_append_entries(msg.append_param,
                                                          result, effects);
                Status status =
                    drive_raft_effects(target_idx, std::move(effects));
                ASSERT_TRUE(status.ok()) << status.to_string();
                RaftEffects response_effects;
                IGNORE_RESULT(nodes_[source_idx]->handle_append_response(
                    msg.target.replica_id, msg.append_param, result,
                    response_effects));
                Status response_status =
                    drive_raft_effects(source_idx, std::move(response_effects));
                ASSERT_TRUE(response_status.ok())
                    << response_status.to_string();
                break;
            }

            case RaftMessageType::INSTALL_SNAPSHOT: {
                RaftEffects prepare_effects;
                Status s = nodes_[target_idx]->prepare_install_snapshot(
                    msg.snapshot_param.term, msg.snapshot_param.snapshot_index,
                    prepare_effects);
                Status prepare_status =
                    drive_raft_effects(target_idx, std::move(prepare_effects));
                ASSERT_TRUE(prepare_status.ok()) << prepare_status.to_string();

                InstallSnapshotResult result;
                result.term = nodes_[target_idx]->current_term();
                result.status = s;

                if (s.ok()) {
                    RaftEffects install_effects;
                    nodes_[target_idx]->install_leader_snapshot(
                        msg.snapshot_param.snapshot_index,
                        msg.snapshot_param.snapshot_term,
                        msg.snapshot_param.term, install_effects);
                    Status install_status = drive_raft_effects(
                        target_idx, std::move(install_effects));
                    ASSERT_TRUE(install_status.ok())
                        << install_status.to_string();
                }
                if (s.fail() && msg.snapshot_param.snapshot_index <=
                                    nodes_[target_idx]->snapshot_index()) {
                    result.snapshot_index =
                        nodes_[target_idx]->snapshot_index();
                    result.follower_snapshot_ahead = true;
                }

                RaftEffects response_effects;
                nodes_[source_idx]->handle_install_snapshot_response(
                    msg.target.replica_id, msg.snapshot_param, result,
                    response_effects);
                Status response_status =
                    drive_raft_effects(source_idx, std::move(response_effects));
                ASSERT_TRUE(response_status.ok())
                    << response_status.to_string();
                break;
            }
        }
    }

    void route_all_messages() {
        for (int i = 0; i < static_cast<int>(nodes_.size()); i++) {
            if (!active_[i] || isolated_[i]) continue;

            std::vector<RaftMessage> messages;
            messages.swap(effect_outboxes_[i]);
            effect_outboxes_[i].clear();
            for (auto& msg : messages) {
                int target = find_node(msg.target.replica_id);
                if (target < 0 || !active_[target] || isolated_[target])
                    continue;

                route_message(i, target, msg);
            }
        }
    }

    std::vector<std::unique_ptr<PersistEngine>> persists_;
    std::vector<std::unique_ptr<RaftNode>> nodes_;
    std::vector<PeerMember> all_members_;
    std::vector<bool> active_;  // TODO vector<bool>好像有问题来着
    std::vector<bool> isolated_;
    std::vector<std::vector<RaftMessage>> effect_outboxes_;
    std::vector<fs::path> temp_dirs_;
    std::mutex effect_mutex_;

    // 这个索引map是代表着replica_id对应的是这个group里的第几个node
    std::unordered_map<ReplicaID, int, ReplicaIDHash> id_to_idx_;
    int table_id_{0};
};

///////////////////////////////////////////////////////////////
///////// 开始gtest的部分

class RaftClusterTest : public ::testing::Test {
   protected:
    void TearDown() override { cluster_.cleanup_temp_dirs(); }

    // 创建一个节点个数是n的raft group集群，并且此时只有一个leader
    void create_and_stabilize(int num_nodes) {
        int tid = next_table_id_++;
        cluster_.create(num_nodes, tid);
        ASSERT_TRUE(cluster_.tick_until_stable());
    }

    void create_and_set_0_leader(int num_nodes) {
        int tid = next_table_id_++;
        cluster_.create(num_nodes, tid);
        tick_election(0);
        ASSERT_TRUE(cluster_.tick_until_stable());
    }

    void assert_single_leader() { ASSERT_EQ(cluster_.leader_count(), 1); }

    bool tick_until_all_committed(LogIndex target, int max_rounds = 100) {
        for (int r = 0; r < max_rounds; r++) {
            cluster_.tick_all();
            if (cluster_.all_committed_at_least(target)) return true;
        }
        return false;
    }

    void only_send_to_one(int node_count, int leader_idx, int node_idx,
                          WriteOpType op_type, Key key, Value value) {
        RaftNode* leader = cluster_.node_ptr(leader_idx);
        RaftNode* to = cluster_.node_ptr(node_idx);
        if ((!leader) or (!to)) return;
        for (int i = 0; i < node_count; i++) {
            if (i == leader_idx or i == node_idx) continue;
            cluster_.isolate(i);
        }
        cluster_.propose(leader_idx, op_type, key, value);
        cluster_.tick_n(20);
        for (int i = 0; i < node_count; i++) {
            if (i == leader_idx or i == node_idx) continue;
            cluster_.restore(i);
        }
    }

    void tick_election(int node_idx) {
        RaftEffects effects;
        cluster_.node_ptr(node_idx)->become_candidate(effects);
        Status status =
            cluster_.drive_raft_effects(node_idx, std::move(effects));
        ASSERT_TRUE(status.ok()) << status.to_string();
    }

    void tick_heartbeat(int node_idx) {
        cluster_.node_ptr(node_idx)->heartbeat_tick_trigger_.reset(0);
        cluster_.tick_node(node_idx);
        cluster_.node_ptr(node_idx)->heartbeat_tick_trigger_.reset(3);
    }

    void set_heartbeat_tick(int node_idx, int val) {
        cluster_.node_ptr(node_idx)->heartbeat_tick_trigger_.reset(val);
    }

    void drop_all_messages() {
        for (int i = 0; i < cluster_.size(); i++) {
            cluster_.drop_messages_from(i);
        }
    }

    // 断言， 给这些节点投票后会成为leader
    void elect_with_votes(int candidate_idx, const std::vector<int>& voters) {
        tick_election(candidate_idx);
        ASSERT_EQ(cluster_.route_messages_from_to(
                      candidate_idx, RaftMessageType::REQUEST_VOTE, voters),
                  static_cast<int>(voters.size()));
        ASSERT_EQ(cluster_.leader_idx(), candidate_idx);
    }

    // 断言， 给这些节点发起投票，但是不会当选leader
    void assert_not_elected_after_votes(int candidate_idx,
                                        const std::vector<int>& voters) {
        tick_election(candidate_idx);
        ASSERT_EQ(cluster_.route_messages_from_to(
                      candidate_idx, RaftMessageType::REQUEST_VOTE, voters),
                  static_cast<int>(voters.size()));
        ASSERT_NE(cluster_.leader_idx(), candidate_idx);
    }

    // 返回值是实际发送的节点的数量
    int replicate_once(int leader_idx, const std::vector<int>& followers) {
        if (followers.empty()) {
            return cluster_.drop_messages_from(leader_idx);
        }
        return cluster_.route_messages_from_to(
            leader_idx, RaftMessageType::APPEND_ENTRIES, followers);
    }

    // 进行写操作， 并且只把日志复制给followers
    LogIndex propose_and_replicate_once(int leader_idx,
                                        const std::vector<int>& followers,
                                        WriteOpType op_type, Key key,
                                        Value value) {
        auto [status, idx] = cluster_.propose(leader_idx, op_type, key, value);
        if (!status.ok()) {
            ADD_FAILURE() << status.to_string();
            return idx;
        }

        int routed_or_dropped = replicate_once(leader_idx, followers);
        if (followers.empty()) {
            EXPECT_GT(routed_or_dropped, 0);
        } else {
            EXPECT_EQ(routed_or_dropped, static_cast<int>(followers.size()));
        }
        return idx;
    }

    void replicate_until_committed(int leader_idx, LogIndex target_idx,
                                   const std::vector<int>& followers,
                                   int max_rounds = 50) {
        for (int i = 0;
             i < max_rounds &&
             cluster_.node_ptr(leader_idx)->commit_index() < target_idx;
             i++) {
            replicate_once(leader_idx, followers);
            tick_heartbeat(leader_idx);
        }
        ASSERT_GE(cluster_.node_ptr(leader_idx)->commit_index(), target_idx);
    }

    void assert_committed_at_least(const std::vector<int>& node_indices,
                                   LogIndex target_idx) {
        for (int idx : node_indices) {
            ASSERT_GE(cluster_.node_ptr(idx)->commit_index(), target_idx)
                << "node_idx=" << idx;
        }
    }

    std::vector<LogEntry> get_node_entries(int idx) {
        RaftNode* node = cluster_.node_ptr(idx);
        if (!node) return {};
        return node->log_entries_for_test();
    }

    std::pair<LogIndex, Term> get_node_snapshot(int idx) {
        RaftNode* node = cluster_.node_ptr(idx);
        if (!node) return {};
        return node->snapshot_for_test();
    }

    void set_node_next_index(int node_idx, const ReplicaID& target,
                             LogIndex val) {
        RaftNode* node = cluster_.node_ptr(node_idx);
        if (!node) return;
        node->set_next_index_for_test(target, val);
    }

    LogIndex get_node_next_index(int node_idx, const ReplicaID& target) {
        RaftNode* node = cluster_.node_ptr(node_idx);
        if (!node) return 0;
        return node->replication_.next_index(target);
    }

    LogIndex get_confirmed_snapshot_index(int node_idx,
                                          const ReplicaID& target) {
        RaftNode* node = cluster_.node_ptr(node_idx);
        if (!node) return 0;
        return node->replication_.confirmed_snapshot_index(target);
    }

    LogIndex get_inflight_snapshot_index(int node_idx,
                                         const ReplicaID& target) {
        RaftNode* node = cluster_.node_ptr(node_idx);
        if (!node) return 0;
        return node->replication_.inflight_snapshot_index(target);
    }

    RaftCluster cluster_;
    static inline int next_table_id_{
        500};  // TODO 这里是在做什么？为何要static + inline
};

//////////////////////////////////////////////////////////////
//
//
//  下面都是测试
//
//
//
////////////////////////////////////////////////////////////

// 测试基本情况，num个节点的group，最终会稳定下来
TEST_F(RaftClusterTest, SingleLeaderElection_3Nodes) {
    for (int num = 3; num <= 10; num++) {
        create_and_stabilize(num);
        assert_single_leader();

        int leader = cluster_.leader_idx();
        ASSERT_GE(leader, 0);

        for (int i = 0; i < 3; i++) {
            if (i == leader) continue;
            ASSERT_EQ(cluster_.node_ptr(i)->role(), ReplicaRole::FOLLOWER);
        }
    }

    // leader崩了， 测试是否可行，并且写入
    for (int round = 3; round < 10; round++) {
        create_and_stabilize(round);

        int lidx = cluster_.leader_idx();
        ASSERT_GE(lidx, 0) << "Round " << round << ": no leader found";

        cluster_.isolate(lidx);

        ASSERT_TRUE(cluster_.tick_until_stable());
        assert_single_leader();

        int new_leader_idx = cluster_.leader_idx();
        auto* nl = cluster_.node_ptr(new_leader_idx);
        ASSERT_NE(nl, nullptr);
        auto [status, idx] = cluster_.propose(new_leader_idx, WriteOpType::PUT,
                                              "round_" + std::to_string(round),
                                              "val_" + std::to_string(round));
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(tick_until_all_committed(idx));
    }
}

// 检测进行了写操作之后，正常情况下大家都是可以commit的
TEST_F(RaftClusterTest, BasicLogReplication_3Nodes) {
    create_and_stabilize(3);
    int leader_idx = cluster_.leader_idx();
    auto* leader = cluster_.leader_ptr();
    ASSERT_NE(leader, nullptr);
    {  // 先进行一次写操作试试
        auto [status, new_idx] = cluster_.propose(
            leader_idx, WriteOpType::PUT, Key("test_key"), Value("test_value"));
        ASSERT_TRUE(status.ok());
        ASSERT_GT(new_idx, 0);

        ASSERT_TRUE(tick_until_all_committed(new_idx));
    }
    {  // 进行多次
        LogIndex last_idx{0};
        for (int i = 0; i < 1000; i++) {
            Key kv = "k" + std::to_string(i);
            Value vv = "v" + std::to_string(i);
            auto [status, new_idx] = cluster_.propose(
                leader_idx, WriteOpType::PUT, Key(kv), Value(vv));
            ASSERT_TRUE(status.ok());
            last_idx = std::max(last_idx, new_idx);
        }
        ASSERT_TRUE(tick_until_all_committed(last_idx, 2000));
    }
}

// leader的写操作没有达到多数派，commit_idx不会被更新
TEST_F(RaftClusterTest, MinorityLeaderCannotCommitNewEntry) {
    create_and_set_0_leader(5);
    ASSERT_TRUE(tick_until_all_committed(1));
    drop_all_messages();

    LogIndex base_commit = cluster_.node_ptr(0)->commit_index();
    LogIndex uncommitted_idx =
        propose_and_replicate_once(0, {1}, WriteOpType::PUT, "111", "111");

    ASSERT_GT(uncommitted_idx, base_commit);
    ASSERT_EQ(cluster_.node_ptr(0)->commit_index(), base_commit);
    ASSERT_LT(cluster_.node_ptr(0)->commit_index(), uncommitted_idx);
}

// 对于写请求而言：如果当前 leader
// 已经接受了写入，但在这次返回前还没有达到多数派提交， 那么客户端应看到的是
// NOT_YET_COMMIT，而不是 OK。
TEST_F(RaftClusterTest, PutReturnsNotYetCommitBeforeQuorumCommit) {
    create_and_set_0_leader(5);
    ASSERT_TRUE(tick_until_all_committed(1));
    drop_all_messages();

    int leader_idx = cluster_.leader_idx();
    ASSERT_EQ(leader_idx, 0);

    RaftNode* leader = cluster_.node_ptr(leader_idx);
    ASSERT_NE(leader, nullptr);

    LogIndex base_commit = leader->commit_index();
    auto [status, target_idx] = cluster_.propose(leader_idx, WriteOpType::PUT,
                                                 "put-not-yet-commit", "v1");
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_GT(target_idx, base_commit);

    // 只把这条写复制给一个 follower，不足以形成多数派提交。
    ASSERT_EQ(replicate_once(leader_idx, {1}), 1);
    EXPECT_EQ(leader->commit_index(), base_commit);
    EXPECT_LT(leader->commit_index(), target_idx);

    // 这对应 Replica::put 的 NOT_YET_COMMIT 边界：leader
    // 已接受，但提交尚未确认。
    EXPECT_EQ(leader->commit_index() < target_idx, true);

    // 继续复制到多数派，确认它后续可以真正提交。
    replicate_until_committed(leader_idx, target_idx, {1, 2});
    EXPECT_GE(leader->commit_index(), target_idx);
}

// Delete 与 Put 的完成语义保持一致：没有在返回前观察到 committed，不能返回 OK。
TEST_F(RaftClusterTest, DeleteReturnsNotYetCommitBeforeQuorumCommit) {
    create_and_set_0_leader(5);
    ASSERT_TRUE(tick_until_all_committed(1));
    drop_all_messages();

    int leader_idx = cluster_.leader_idx();
    ASSERT_EQ(leader_idx, 0);

    RaftNode* leader = cluster_.node_ptr(leader_idx);
    ASSERT_NE(leader, nullptr);

    // 先写入一个已提交的 key，确保 delete 的目标对象存在。
    auto [put_status, put_idx] = cluster_.propose(
        leader_idx, WriteOpType::PUT, "delete-not-yet-commit", "v1");
    ASSERT_TRUE(put_status.ok()) << put_status.to_string();
    ASSERT_TRUE(tick_until_all_committed(put_idx));
    drop_all_messages();

    LogIndex base_commit = leader->commit_index();
    auto [status, target_idx] = cluster_.propose(leader_idx, WriteOpType::DEL,
                                                 "delete-not-yet-commit", "");
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_GT(target_idx, base_commit);

    ASSERT_EQ(replicate_once(leader_idx, {1}), 1);
    EXPECT_EQ(leader->commit_index(), base_commit);
    EXPECT_LT(leader->commit_index(), target_idx);

    // 这对应 Replica::del 的 NOT_YET_COMMIT 边界：删除已被 leader
    // 接受，但提交尚未确认。
    EXPECT_EQ(leader->commit_index() < target_idx, true);

    replicate_until_committed(leader_idx, target_idx, {1, 2});
    EXPECT_GE(leader->commit_index(), target_idx);
}

// 当leader宕机之后，选取新的leader，是否正常
TEST_F(RaftClusterTest, LeaderCrash_NewLeaderElected_NoLogLoss) {
    create_and_stabilize(3);
    auto* old_leader = cluster_.leader_ptr();
    int old_leader_idx = cluster_.leader_idx();
    ASSERT_NE(old_leader, nullptr);

    // 写入并确认提交
    auto [status, new_idx] =
        cluster_.propose(old_leader_idx, WriteOpType::PUT, "k1", "v1");
    ASSERT_TRUE(status.ok());

    tick_until_all_committed(new_idx, 100);
    LogIndex old_commit = old_leader->commit_index();
    ASSERT_GE(old_commit, new_idx) << "Leader should have committed the entry";

    // 崩溃旧 Leader
    cluster_.crash(old_leader_idx);

    // 等待新 Leader 选出
    ASSERT_TRUE(cluster_.tick_until_stable(300));

    int new_leader_idx = cluster_.leader_idx();
    ASSERT_NE(new_leader_idx, old_leader_idx);
    ASSERT_NE(new_leader_idx, -1);

    auto* new_leader = cluster_.leader_ptr();
    ASSERT_NE(new_leader, nullptr);
    ASSERT_EQ(new_leader->commit_index(), old_commit);

    // 这里刷新一下，new_leader会提交一个NONE的，所以会大于
    cluster_.tick_all();
    ASSERT_GT(new_leader->commit_index(), old_commit);
}

// 测试原本是leader的节点后来被更高的term被打回follower了，
// 然后集群又选出一个新的leader
TEST_F(RaftClusterTest, HigherTermVoteRequestForcesLeaderStepDown) {
    create_and_set_0_leader(3);
    ASSERT_TRUE(tick_until_all_committed(1));
    ASSERT_EQ(cluster_.leader_idx(), 0);

    Term higher_term = cluster_.node_ptr(0)->current_term() + 1;
    RequestVoteResult vote_result;
    RaftEffects vote_effects;
    cluster_.node_ptr(0)->handle_request_vote(
        RequestVoteParam{cluster_.replica_id(1), cluster_.replica_id(0),
                         higher_term, 0, 0},
        vote_result, vote_effects);
    Status status = cluster_.drive_raft_effects(0, std::move(vote_effects));
    ASSERT_TRUE(status.ok()) << status.to_string();

    ASSERT_EQ(cluster_.node_ptr(0)->current_term(), higher_term);
    ASSERT_EQ(cluster_.node_ptr(0)->role(), ReplicaRole::FOLLOWER);
    ASSERT_EQ(cluster_.leader_idx(), -1);

    elect_with_votes(2, {0, 1});
    ASSERT_EQ(cluster_.leader_idx(), 2);
}

TEST_F(RaftClusterTest, LaggingFollowersCatchUpCommittedLogs) {
    create_and_stabilize(5);
    cluster_.isolate(3);
    cluster_.isolate(4);
    cluster_.tick_until_stable();
    RaftNode* leader = cluster_.leader_ptr();
    int leader_idx = cluster_.leader_idx();
    ASSERT_NE(leader, nullptr);
    LogIndex last_idx{0};
    for (int i = 1; i <= 100; i++) {
        Key key = "key_" + std::to_string(i);
        Value value = "valye_" + std::to_string(i);
        auto [status, new_idx] =
            cluster_.propose(leader_idx, WriteOpType::PUT, key, value);
        ASSERT_TRUE(status.ok());
        last_idx = std::max(last_idx, new_idx);
    }
    cluster_.restore(3);
    cluster_.restore(4);
    ASSERT_NE(cluster_.node_ptr(3), nullptr);
    ASSERT_NE(cluster_.node_ptr(4), nullptr);
    ASSERT_GE(cluster_.node_ptr(3)->commit_index(), 0);  //
    ASSERT_GE(cluster_.node_ptr(4)->commit_index(), 0);

    ASSERT_LE(cluster_.node_ptr(3)->commit_index(), 1);  //
    ASSERT_LE(cluster_.node_ptr(4)->commit_index(), 1);
    ASSERT_TRUE(tick_until_all_committed(last_idx));
    ASSERT_EQ(cluster_.node_ptr(3)->commit_index(), last_idx);
    ASSERT_EQ(cluster_.node_ptr(4)->commit_index(), last_idx);
}

TEST_F(RaftClusterTest, UncommittedOldLeaderLogsAreOverwrittenByNewLeader) {
    create_and_stabilize(5);
    ASSERT_NE(cluster_.leader_ptr(), nullptr);
    int old_leader_idx = cluster_.leader_idx();
    int other_idx = (old_leader_idx == 0 ? 1 : 0);

    ASSERT_TRUE(tick_until_all_committed(1));

    for (int i = 0; i < 5; i++) {
        ASSERT_EQ(get_node_entries(i).size(), 1U);
        ASSERT_EQ(get_node_entries(i).back().op_type, WriteOpType::NONE);
    }

    {
        // 首先把三个节点宕机， 然后leader去写数据，然后tick
        for (int i = 0; i < 5; i++) {
            if (i == other_idx or i == old_leader_idx) continue;
            cluster_.isolate(i);
        }

        for (int i = 1; i <= 10; i++) {
            Key key = "key_" + std::to_string(i);
            Value value = "value_" + std::to_string(i);
            auto [status, new_idx] =
                cluster_.propose(old_leader_idx, WriteOpType::PUT, key, value);
            ASSERT_TRUE(status.ok());
        }

        cluster_.tick_until_stable();

        // leader-no-op + 10
        ASSERT_EQ(get_node_entries(other_idx).size(), 11U);
    }
    {
        // 然后把三个节点拉回来，另外两个节点宕机，然后选出来新的leader，然后写数据
        for (int i = 0; i < 5; i++) {
            if (i == other_idx or i == old_leader_idx) continue;
            cluster_.restore(i);
        }
        cluster_.isolate(other_idx);
        cluster_.isolate(old_leader_idx);
        ASSERT_TRUE(cluster_.tick_until_stable());
        ASSERT_NE(cluster_.leader_ptr(), nullptr);
        int new_leader_idx = cluster_.leader_idx();

        for (int i = 1; i <= 5; i++) {
            Key key = "new_key_" + std::to_string(i);
            Value value = "new_value_" + std::to_string(i);
            auto [status, new_idx] =
                cluster_.propose(new_leader_idx, WriteOpType::PUT, key, value);
            ASSERT_TRUE(status.ok());
        }
    }
    // 把那两个节点拉回来，然后tick，检测是否log正确

    std::vector<LogEntry> entries = get_node_entries(0);
    ASSERT_TRUE(!entries.empty());
    ASSERT_EQ(entries.back().key, "key_10");
    cluster_.restore(other_idx);
    cluster_.restore(old_leader_idx);
    ASSERT_TRUE(cluster_.tick_until_stable());

    ASSERT_TRUE(tick_until_all_committed(7));

    entries = get_node_entries(0);
    ASSERT_EQ(entries.empty(), false);
    // 不要求最后一条一定是 new_key_5，因为 restore
    // 后可能term过高从而重新选举，leader 会追加 no-op。
    auto has_key = [&](const std::string& key) {
        return std::any_of(
            entries.begin(), entries.end(), [&](const LogEntry& entry) {
                return entry.op_type == WriteOpType::PUT && entry.key == key;
            });
    };

    for (int i = 1; i <= 5; i++) {
        EXPECT_TRUE(has_key("new_key_" + std::to_string(i)));
    }

    for (int i = 1; i <= 10; i++) {
        EXPECT_FALSE(has_key("key_" + std::to_string(i)));
    }
}

//
// 节点 5 个，A先当leader，然后A先发给B，然后网络隔离。
// 然后E开始当leader， 自己给自己发了个数据， 然后网络隔离。
// A又回来了， 给C发送个数据， 然后网络隔离
// E又回来了当leader， 给B，D，E发了数据 ，然后可以成功的提交。
// 注意， commit_idx在A回来给C发送消息后是不可以被推到2的。

TEST_F(RaftClusterTest, Figure8_NoDirectCommitPreviousTerm) {
    create_and_set_0_leader(5);
    ASSERT_TRUE(tick_until_all_committed(1));
    ASSERT_EQ(cluster_.leader_idx(), 0);
    drop_all_messages();

    // 这里只给B发了消息， 然后
    LogIndex old_term_idx =
        propose_and_replicate_once(0, {1}, WriteOpType::PUT, "1", "1");
    ASSERT_EQ(old_term_idx, 2);
    ASSERT_EQ(cluster_.node_ptr(0)->commit_index(), 1);

    // A没了， E当选
    cluster_.isolate(0);
    elect_with_votes(4, {2, 3});
    {
        LogIndex e_idx =
            propose_and_replicate_once(4, {}, WriteOpType::PUT, "2", "2");
        ASSERT_EQ(e_idx, 3);
        cluster_.isolate(4);
    }
    {
        // 这里把A恢复过来
        cluster_.restore(0);

        // 这里发起一次投票，但是不会当上
        assert_not_elected_after_votes(0, {1, 2});

        // A这里应该会当上leader
        elect_with_votes(0, {1, 2});

        // A给C发no-op和之前的旧数据，然后这里的commit_idx还得是1
        ASSERT_EQ(replicate_once(0, {2}), 1);
        ASSERT_EQ(cluster_.node_ptr(0)->commit_index(), 1);

        LogIndex a_current_term_idx =
            propose_and_replicate_once(0, {2}, WriteOpType::PUT, "3", "3");
        ASSERT_EQ(a_current_term_idx, 4);
        ASSERT_EQ(get_node_entries(2).back().key, "3");
        ASSERT_EQ(cluster_.node_ptr(0)->commit_index(), 1);
        cluster_.isolate(0);
    }
    {
        cluster_.restore(4);
        assert_not_elected_after_votes(4, {1, 3});
        elect_with_votes(4, {1, 3});

        // 到这里， 是确保了E当上了leader

        LogIndex final_idx =
            propose_and_replicate_once(4, {1, 3}, WriteOpType::PUT, "4", "4");
        ASSERT_EQ(final_idx, 5);  // A:no-op + E:no-op + key=2 + E:no-op + key=4

        replicate_until_committed(4, final_idx, {1, 3});
        ASSERT_EQ(cluster_.node_ptr(4)->commit_index(), final_idx);
        ASSERT_LT(cluster_.node_ptr(1)->commit_index(), final_idx);
        ASSERT_LT(cluster_.node_ptr(3)->commit_index(), final_idx);

        // 再发一次心跳，把commit_idx传给followers
        tick_heartbeat(4);
        ASSERT_GE(replicate_once(4, {1, 3}), 2);
        assert_committed_at_least({1, 3}, final_idx);
    }

    {
        cluster_.restore(0);
        auto [status_5, idx] = cluster_.propose(4, WriteOpType::PUT, "5", "5");
        ASSERT_TRUE(status_5.ok());
        // 最后所有的会大于这个
        ASSERT_TRUE(tick_until_all_committed(idx));
        ASSERT_EQ(get_node_entries(0).back().key, "5");
    }
}

///////////
// 测试一下这个快照， 截断log是否可行
// 把一个落下的node给他传快照
TEST_F(RaftClusterTest, InstallSnapshot_Basic) {
    create_and_set_0_leader(3);
    ASSERT_TRUE(tick_until_all_committed(1));
    auto* leader = cluster_.node_ptr(0);
    ASSERT_TRUE(leader->is_leader());
    drop_all_messages();

    cluster_.isolate(2);

    // 写 5 条记录
    LogIndex last_idx = 0;
    for (int i = 0; i < 5; i++) {
        auto kv = "snap_k" + std::to_string(i);
        auto vv = "snap_v" + std::to_string(i);
        last_idx = propose_and_replicate_once(0, {1}, WriteOpType::PUT, Key(kv),
                                              Value(vv));
    }
    ASSERT_EQ(leader->commit_index(), last_idx);

    // 在所有节点上 advance last_applied
    for (int i = 0; i < cluster_.size(); i++) {
        if (!cluster_.is_active(i) or cluster_.is_isolate(i)) continue;
        cluster_.node_ptr(i)->advance_last_applied(last_idx);
    }

    // Leader 截断
    LogIndex snap_idx = 3;
    ASSERT_TRUE(leader->truncate_log(snap_idx).ok());
    ASSERT_EQ(leader->snapshot_index(), snap_idx);

    cluster_.restore(2);
    // 再 propose 一条新数据
    auto [s_new, idx_new] =
        cluster_.propose(0, WriteOpType::PUT, "new_k", "new_v");
    ASSERT_TRUE(s_new.ok());

    // Tick 到全部提交
    ASSERT_TRUE(tick_until_all_committed(idx_new));
    ASSERT_EQ(get_node_snapshot(2).first, 3);   // index
    ASSERT_EQ(get_node_snapshot(2).second, 1);  // term
}

// 测一下并发写leader的情况
TEST_F(RaftClusterTest, ConcurrentPropose_ThreadSafety) {
    create_and_stabilize(5);
    auto* leader = cluster_.leader_ptr();
    ASSERT_NE(leader, nullptr);

    constexpr int kThreads = 4;
    constexpr int kOpsPerThread = 25;
    std::atomic<int> success_count{0};
    std::vector<std::thread> threads;

    for (int t = 0; t < kThreads; t++) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < kOpsPerThread; i++) {
                auto kv = "concurrent_t" + std::to_string(t) + "_i" +
                          std::to_string(i);
                auto vv = "value";
                auto [status, idx] =
                    cluster_.propose(cluster_.leader_idx(), WriteOpType::PUT,
                                     Key(kv), Value(vv));
                if (status.ok()) success_count++;
            }
        });
    }

    for (auto& th : threads) th.join();

    ASSERT_EQ(success_count.load(), kThreads * kOpsPerThread);

    ASSERT_TRUE(tick_until_all_committed(leader->last_log_index(), 10000));
}

// 检测一个follower如果recovering的话， 是否表现正常
// 是否不会成为candidate // 是否投票会成功 // 是否能进行propose
TEST_F(RaftClusterTest, RecoveringFollowerCatchesUpByAppendEntries) {
    create_and_set_0_leader(3);
    ASSERT_TRUE(tick_until_all_committed(1));
    drop_all_messages();

    cluster_.isolate(1);
    LogIndex target_idx =
        propose_and_replicate_once(0, {2}, WriteOpType::PUT, "recover_k", "v");
    ASSERT_EQ(target_idx, 2);
    ASSERT_EQ(cluster_.node_ptr(0)->commit_index(), target_idx);

    cluster_.node_ptr(1)->enter_recovering();
    ASSERT_TRUE(cluster_.node_ptr(1)->is_recovering());

    // 检测他不会成为candidate的
    for (int i = 0; i < 50; i++) {
        cluster_.tick_node(1);
    }
    ASSERT_EQ(cluster_.node_ptr(1)->role(), ReplicaRole::FOLLOWER);

    // 无法进行propose操作， 虽然他本来就是follower没办法吧。。。
    auto [status, idx] = cluster_.propose(1, WriteOpType::PUT, "bad", "bad");
    ASSERT_TRUE(status.code() == StatusCode::IS_RECOVERING);
    ASSERT_EQ(idx, -1);

    // 投票也会失败的
    RequestVoteResult vote_result;
    RaftEffects vote_effects;
    cluster_.node_ptr(1)->handle_request_vote(
        RequestVoteParam{ReplicaID{500, 0, 2}, ReplicaID{500, 0, 1},
                         cluster_.node_ptr(1)->current_term(), 0, 0},
        vote_result, vote_effects);
    Status vote_status =
        cluster_.drive_raft_effects(1, std::move(vote_effects));
    ASSERT_TRUE(vote_status.ok()) << vote_status.to_string();
    ASSERT_FALSE(vote_result.vote_granted);

    cluster_.restore(1);
    tick_heartbeat(0);
    ASSERT_EQ(replicate_once(0, {1}), 1);

    ASSERT_EQ(cluster_.node_ptr(1)->commit_index(), target_idx);
    ASSERT_FALSE(cluster_.node_ptr(1)->is_recovering());
}

// 检测一个leader如果突然recovering的话， 表现是否正常
// leader进入recovering之后， 应该会退回follower， 不会继续进行心跳。
// 之后剩下的节点应该可以重新选出来新的leader。
TEST_F(RaftClusterTest, RecoveringLeaderStepsDownAndNewLeaderElected) {
    create_and_set_0_leader(3);
    ASSERT_TRUE(tick_until_all_committed(1));
    ASSERT_EQ(cluster_.leader_idx(), 0);
    drop_all_messages();

    cluster_.node_ptr(0)->enter_recovering();
    ASSERT_TRUE(cluster_.node_ptr(0)->is_recovering());
    ASSERT_EQ(cluster_.node_ptr(0)->role(), ReplicaRole::FOLLOWER);
    ASSERT_EQ(cluster_.leader_idx(), -1);

    auto [status, idx] = cluster_.propose(0, WriteOpType::PUT, "111", "111");
    ASSERT_TRUE(status.code() == StatusCode::IS_RECOVERING);
    ASSERT_EQ(idx, -1);

    // recovering的leader不会继续tick出来心跳，也不会重新成为candidate
    for (int i = 0; i < 50; i++) {
        cluster_.tick_node(0);
    }
    ASSERT_EQ(cluster_.drop_messages_from(0), 0);
    ASSERT_EQ(cluster_.node_ptr(0)->role(), ReplicaRole::FOLLOWER);

    // node0还在recovering，不参与投票；node1只需要node2的票即可当选
    elect_with_votes(1, {2});
    ASSERT_EQ(cluster_.leader_idx(), 1);
}

// 检测如果follower落后太久，leader已经把日志截断成snapshot了，
// 那么这个follower应该先收到snapshot， 再收到snapshot之后的entries，
// 最后追上leader的commit_index。
TEST_F(RaftClusterTest, LaggingFollowerCatchesUpBySnapshotThenEntries) {
    create_and_set_0_leader(3);
    ASSERT_TRUE(tick_until_all_committed(1));
    drop_all_messages();

    cluster_.isolate(2);

    LogIndex last_idx = 0;
    for (int i = 0; i < 5; i++) {
        last_idx = propose_and_replicate_once(
            0, {1}, WriteOpType::PUT, "snap_then_log_" + std::to_string(i),
            "v" + std::to_string(i));
    }
    ASSERT_EQ(last_idx, 6);
    ASSERT_EQ(cluster_.node_ptr(0)->commit_index(), last_idx);

    cluster_.node_ptr(0)->advance_last_applied(last_idx);
    ASSERT_TRUE(cluster_.node_ptr(0)->truncate_log(4).ok());
    ASSERT_EQ(cluster_.node_ptr(0)->snapshot_index(), 4);
    ASSERT_EQ(cluster_.node_ptr(2)->commit_index(), 1);

    cluster_.restore(2);

    tick_heartbeat(0);
    ASSERT_EQ(cluster_.route_messages_from_to(
                  0, RaftMessageType::INSTALL_SNAPSHOT, {2}),
              1);
    ASSERT_EQ(cluster_.node_ptr(2)->snapshot_index(), 4);
    ASSERT_EQ(cluster_.node_ptr(2)->commit_index(), 4);

    tick_heartbeat(0);
    ASSERT_EQ(replicate_once(0, {2}), 1);

    ASSERT_EQ(cluster_.node_ptr(2)->commit_index(), last_idx);
    ASSERT_EQ(get_node_entries(2).back().key, "snap_then_log_4");
}
// 一开始0是leader，然后被隔离了，2当了leader，在提交no-op之前的get操作会不行，当提交了no-op之后，commit_idx推进了之后，才可以进行get操作
TEST_F(RaftClusterTest, ReadIndexRequiresCommittedCurrentTermEntry) {
    create_and_set_0_leader(3);
    ASSERT_TRUE(tick_until_all_committed(1));
    ASSERT_EQ(cluster_.leader_idx(), 0);
    drop_all_messages();

    cluster_.isolate(0);
    elect_with_votes(2, {1});
    ASSERT_EQ(cluster_.leader_idx(), 2);

    RaftEffects read_effects;
    LogIndex read_index = 0;
    Term read_term = 0;
    Status status = cluster_.node_ptr(2)->build_append_entries_for_read(
        read_effects, read_index, read_term);

    EXPECT_EQ(status.code(), StatusCode::NOT_YET_COMMIT);
    EXPECT_TRUE(read_effects.messages.empty());

    LogIndex current_term_idx = cluster_.node_ptr(2)->last_log_index();
    replicate_until_committed(2, current_term_idx, {1});

    status = cluster_.node_ptr(2)->build_append_entries_for_read(
        read_effects, read_index, read_term);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(read_index, cluster_.node_ptr(2)->commit_index());
    EXPECT_EQ(read_term, cluster_.node_ptr(2)->current_term());
    EXPECT_FALSE(read_effects.messages.empty());
}

// 验证读一致性检查中，INSTALL_SNAPSHOT 消息也被计入 quorum。
// 之前只处理 APPEND_ENTRIES，遇到 INSTALL_SNAPSHOT 直接 continue 跳过，
// 导致我们会错误的返回一次NO_LEADER，然后等到心跳补上了快照之后才会OK
TEST_F(RaftClusterTest, ReadIndexCountsInstallSnapshotMessage) {
    create_and_set_0_leader(3);
    ASSERT_TRUE(tick_until_all_committed(1));
    ASSERT_EQ(cluster_.leader_idx(), 0);
    drop_all_messages();

    // 写入并提交，然后 leader 做 snapshot 截断
    LogIndex last_idx = 0;
    for (int i = 0; i < 5; i++) {
        last_idx = propose_and_replicate_once(0, {1, 2}, WriteOpType::PUT,
                                              "snap_read_" + std::to_string(i),
                                              "v" + std::to_string(i));
    }
    ASSERT_EQ(cluster_.node_ptr(0)->commit_index(), last_idx);

    cluster_.node_ptr(0)->advance_last_applied(last_idx);
    ASSERT_TRUE(cluster_.node_ptr(0)->truncate_log(4).ok());
    ASSERT_EQ(cluster_.node_ptr(0)->snapshot_index(), 4);

    // 强制 node 2 的 next_index 降到 snapshot_index 以下，
    // 模拟 follower 落后太多、leader 需要发快照的场景
    set_node_next_index(0, cluster_.replica_id(2), 3);

    RaftEffects read_effects;
    LogIndex read_index = 0;
    Term read_term = 0;
    Status status = cluster_.node_ptr(0)->build_append_entries_for_read(
        read_effects, read_index, read_term);
    ASSERT_TRUE(status.ok()) << status.to_string();

    // 验证同时存在 APPEND_ENTRIES 和 INSTALL_SNAPSHOT
    bool has_snapshot = false;
    bool has_append = false;
    for (const auto& msg : read_effects.messages) {
        if (msg.type == RaftMessageType::INSTALL_SNAPSHOT) has_snapshot = true;
        if (msg.type == RaftMessageType::APPEND_ENTRIES) has_append = true;
    }

    ASSERT_TRUE(has_snapshot);
    ASSERT_TRUE(has_append);

    // 模拟 Replica::check_self_leader_and_get_read_index 的计数逻辑
    int success_cnt = 1;  // self
    int limit = cluster_.node_ptr(0)->quorum_size();

    for (auto& msg : read_effects.messages) {
        int target = cluster_.find_node(msg.target.replica_id);
        ASSERT_GE(target, 0);

        if (msg.type == RaftMessageType::APPEND_ENTRIES) {
            AppendEntriesResult res;
            RaftEffects effects;
            cluster_.node_ptr(target)->handle_append_entries(msg.append_param,
                                                             res, effects);
            Status drive_status = cluster_.drive_raft_effects(target, effects);
            ASSERT_TRUE(drive_status.ok()) << drive_status.to_string();
            RaftEffects response_effects;
            IGNORE_RESULT(cluster_.node_ptr(0)->handle_append_response(
                msg.target.replica_id, msg.append_param, res,
                response_effects));
            Status response_status =
                cluster_.drive_raft_effects(0, std::move(response_effects));
            ASSERT_TRUE(response_status.ok()) << response_status.to_string();
            if (res.term == read_term) success_cnt++;
        } else if (msg.type == RaftMessageType::INSTALL_SNAPSHOT) {
            RaftEffects prepare_effects;
            Status ps = cluster_.node_ptr(target)->prepare_install_snapshot(
                msg.snapshot_param.term, msg.snapshot_param.snapshot_index,
                prepare_effects);
            Status prepare_status =
                cluster_.drive_raft_effects(target, std::move(prepare_effects));
            ASSERT_TRUE(prepare_status.ok()) << prepare_status.to_string();
            InstallSnapshotResult res;
            res.term = cluster_.node_ptr(target)->current_term();
            res.status = ps;
            if (ps.ok()) {
                RaftEffects install_effects;
                cluster_.node_ptr(target)->install_leader_snapshot(
                    msg.snapshot_param.snapshot_index,
                    msg.snapshot_param.snapshot_term, msg.snapshot_param.term,
                    install_effects);
                Status install_status = cluster_.drive_raft_effects(
                    target, std::move(install_effects));
                ASSERT_TRUE(install_status.ok()) << install_status.to_string();
            }
            if (ps.fail() && msg.snapshot_param.snapshot_index <=
                                 cluster_.node_ptr(target)->snapshot_index()) {
                res.snapshot_index =
                    cluster_.node_ptr(target)->snapshot_index();
                res.follower_snapshot_ahead = true;
            }
            RaftEffects response_effects;
            cluster_.node_ptr(0)->handle_install_snapshot_response(
                msg.target.replica_id, msg.snapshot_param, res,
                response_effects);
            Status response_status =
                cluster_.drive_raft_effects(0, std::move(response_effects));
            ASSERT_TRUE(response_status.ok()) << response_status.to_string();
            if (res.term == read_term) success_cnt++;
        }
    }

    EXPECT_GE(success_cnt, limit);
}

TEST_F(RaftClusterTest, InstallSnapshotSuccessUsesSentSnapshotIndex) {
    create_and_set_0_leader(3);
    ASSERT_TRUE(tick_until_all_committed(1));
    ASSERT_EQ(cluster_.leader_idx(), 0);
    drop_all_messages();

    LogIndex last_idx = 0;
    for (int i = 0; i < 5; i++) {
        last_idx = propose_and_replicate_once(0, {1, 2}, WriteOpType::PUT,
                                              "snap_ack_" + std::to_string(i),
                                              "v" + std::to_string(i));
    }
    ASSERT_EQ(cluster_.node_ptr(0)->commit_index(), last_idx);

    cluster_.node_ptr(0)->advance_last_applied(last_idx);
    ASSERT_TRUE(cluster_.node_ptr(0)->truncate_log(4).ok());
    ASSERT_EQ(cluster_.node_ptr(0)->snapshot_index(), 4);

    set_node_next_index(0, cluster_.replica_id(2), 3);

    RaftEffects read_effects;
    LogIndex read_index = 0;
    Term read_term = 0;
    Status status = cluster_.node_ptr(0)->build_append_entries_for_read(
        read_effects, read_index, read_term);
    ASSERT_TRUE(status.ok()) << status.to_string();

    bool found_snapshot = false;
    RaftMessage snapshot_msg;
    for (const auto& msg : read_effects.messages) {
        if (msg.type == RaftMessageType::INSTALL_SNAPSHOT &&
            msg.target.replica_id == cluster_.replica_id(2)) {
            snapshot_msg = msg;
            found_snapshot = true;
            break;
        }
    }
    ASSERT_TRUE(found_snapshot);
    ASSERT_EQ(snapshot_msg.snapshot_param.snapshot_index, 4);

    ASSERT_TRUE(cluster_.node_ptr(0)->truncate_log(last_idx).ok());
    ASSERT_EQ(cluster_.node_ptr(0)->snapshot_index(), last_idx);

    InstallSnapshotResult res;
    res.term = cluster_.node_ptr(0)->current_term();
    res.status = Status::OK();
    res.snapshot_index = 0;
    res.follower_snapshot_ahead = false;

    RaftEffects response_effects;
    cluster_.node_ptr(0)->handle_install_snapshot_response(
        snapshot_msg.target.replica_id, snapshot_msg.snapshot_param, res,
        response_effects);

    EXPECT_EQ(get_node_next_index(0, snapshot_msg.target.replica_id),
              snapshot_msg.snapshot_param.snapshot_index + 1);
    EXPECT_EQ(get_confirmed_snapshot_index(0, snapshot_msg.target.replica_id),
              snapshot_msg.snapshot_param.snapshot_index);
    EXPECT_EQ(get_inflight_snapshot_index(0, snapshot_msg.target.replica_id),
              0);
}
TEST_F(RaftClusterTest, InstallSnapshotEqualSnapshotFlagUpdatesProgress) {
    create_and_set_0_leader(3);
    ASSERT_TRUE(tick_until_all_committed(1));
    ASSERT_EQ(cluster_.leader_idx(), 0);
    drop_all_messages();

    LogIndex last_idx = 0;
    for (int i = 0; i < 5; i++) {
        last_idx = propose_and_replicate_once(0, {1, 2}, WriteOpType::PUT,
                                              "snap_equal_" + std::to_string(i),
                                              "v" + std::to_string(i));
    }
    ASSERT_EQ(cluster_.node_ptr(0)->commit_index(), last_idx);

    cluster_.node_ptr(0)->advance_last_applied(last_idx);
    ASSERT_TRUE(cluster_.node_ptr(0)->truncate_log(4).ok());
    ASSERT_EQ(cluster_.node_ptr(0)->snapshot_index(), 4);

    set_node_next_index(0, cluster_.replica_id(2), 3);

    RaftEffects read_effects;
    LogIndex read_index = 0;
    Term read_term = 0;
    Status status = cluster_.node_ptr(0)->build_append_entries_for_read(
        read_effects, read_index, read_term);
    ASSERT_TRUE(status.ok()) << status.to_string();

    bool found_snapshot = false;
    RaftMessage snapshot_msg;
    for (const auto& msg : read_effects.messages) {
        if (msg.type == RaftMessageType::INSTALL_SNAPSHOT &&
            msg.target.replica_id == cluster_.replica_id(2)) {
            snapshot_msg = msg;
            found_snapshot = true;
            break;
        }
    }
    ASSERT_TRUE(found_snapshot);
    ASSERT_EQ(snapshot_msg.snapshot_param.snapshot_index, 4);

    InstallSnapshotResult res;
    res.term = cluster_.node_ptr(0)->current_term();
    res.status = Status::ERROR("follower already has sent snapshot");
    res.snapshot_index = snapshot_msg.snapshot_param.snapshot_index;
    res.follower_snapshot_ahead = true;

    RaftEffects response_effects;
    cluster_.node_ptr(0)->handle_install_snapshot_response(
        snapshot_msg.target.replica_id, snapshot_msg.snapshot_param, res,
        response_effects);

    EXPECT_EQ(get_node_next_index(0, snapshot_msg.target.replica_id),
              snapshot_msg.snapshot_param.snapshot_index + 1);
    EXPECT_EQ(get_confirmed_snapshot_index(0, snapshot_msg.target.replica_id),
              snapshot_msg.snapshot_param.snapshot_index);
    EXPECT_EQ(get_inflight_snapshot_index(0, snapshot_msg.target.replica_id),
              0);
}

TEST_F(RaftClusterTest, InstallSnapshotResponseFollowerAheadUpdatesProgress) {
    create_and_set_0_leader(3);
    ASSERT_TRUE(tick_until_all_committed(1));
    ASSERT_EQ(cluster_.leader_idx(), 0);
    drop_all_messages();

    LogIndex last_idx = 0;
    for (int i = 0; i < 5; i++) {
        last_idx = propose_and_replicate_once(0, {1, 2}, WriteOpType::PUT,
                                              "snap_ahead_" + std::to_string(i),
                                              "v" + std::to_string(i));
    }
    ASSERT_EQ(cluster_.node_ptr(0)->commit_index(), last_idx);

    cluster_.node_ptr(0)->advance_last_applied(last_idx);
    ASSERT_TRUE(cluster_.node_ptr(0)->truncate_log(4).ok());
    ASSERT_EQ(cluster_.node_ptr(0)->snapshot_index(), 4);

    cluster_.node_ptr(2)->advance_last_applied(last_idx);
    ASSERT_TRUE(cluster_.node_ptr(2)->truncate_log(last_idx).ok());
    ASSERT_GT(cluster_.node_ptr(2)->snapshot_index(),
              cluster_.node_ptr(0)->snapshot_index());

    set_node_next_index(0, cluster_.replica_id(2), 3);

    RaftEffects read_effects;
    LogIndex read_index = 0;
    Term read_term = 0;
    Status status = cluster_.node_ptr(0)->build_append_entries_for_read(
        read_effects, read_index, read_term);
    ASSERT_TRUE(status.ok()) << status.to_string();

    bool found_snapshot = false;
    RaftMessage snapshot_msg;
    for (const auto& msg : read_effects.messages) {
        if (msg.type == RaftMessageType::INSTALL_SNAPSHOT &&
            msg.target.replica_id == cluster_.replica_id(2)) {
            snapshot_msg = msg;
            found_snapshot = true;
            break;
        }
    }
    ASSERT_TRUE(found_snapshot);
    ASSERT_EQ(snapshot_msg.snapshot_param.snapshot_index, 4);

    InstallSnapshotResult res;
    res.term = cluster_.node_ptr(0)->current_term();
    res.status = Status::ERROR("follower already has newer snapshot");
    res.snapshot_index = cluster_.node_ptr(2)->snapshot_index();
    res.follower_snapshot_ahead = true;

    RaftEffects response_effects;
    cluster_.node_ptr(0)->handle_install_snapshot_response(
        snapshot_msg.target.replica_id, snapshot_msg.snapshot_param, res,
        response_effects);

    EXPECT_EQ(get_node_next_index(0, snapshot_msg.target.replica_id),
              cluster_.node_ptr(2)->snapshot_index() + 1);
    EXPECT_EQ(get_confirmed_snapshot_index(0, snapshot_msg.target.replica_id),
              cluster_.node_ptr(2)->snapshot_index());
    EXPECT_EQ(get_inflight_snapshot_index(0, snapshot_msg.target.replica_id),
              0);
}

// 验证读一致性检查中，APPEND_ENTRIES 被 reject（success=false）但 term 匹配时，
// 仍然计入 quorum。
// 场景：A(term=1) 是 leader，B 先被隔离并落后；A 只把新日志复制给 C 并提交。
// 随后恢复 B，让 B 发起一次真实但失败的选举：A 收到 B 的更高 term RequestVote
// 后退为 follower，但因 B 日志落后拒绝投票。之后再次隔离 B，A 靠 C 重新
// 当选(term=3)。此时记录的 next_index[B] 就会比B实际的多，所以会 res.success =
// false
TEST_F(RaftClusterTest, ReadIndexCountsRejectedAppendEntriesWithMatchingTerm) {
    create_and_set_0_leader(3);
    ASSERT_TRUE(tick_until_all_committed(1));
    ASSERT_EQ(cluster_.leader_idx(), 0);
    drop_all_messages();

    // B 先被隔离并落后；A 只靠 self + C 提交一条 term=1 日志。
    cluster_.isolate(1);
    LogIndex idx =
        propose_and_replicate_once(0, {2}, WriteOpType::PUT, "k1", "v1");
    ASSERT_EQ(cluster_.node_ptr(0)->commit_index(), idx);
    ASSERT_LT(cluster_.node_ptr(1)->last_log_index(), idx);
    drop_all_messages();

    // 恢复 B，让真实存在且日志落后的 B 发起选举。A 会因为更高 term 退为
    // follower，但会因为 B 的日志不够新而拒绝投票，所以 B 不会当选。
    cluster_.restore(1);
    assert_not_elected_after_votes(1, {0});
    ASSERT_EQ(cluster_.node_ptr(0)->current_term(), 2);

    // 再次隔离 B，保证 A 重新当选和提交当前 term no-op 的过程只和 C 通信，
    // B 不会被顺便追平。
    cluster_.isolate(1);
    elect_with_votes(0, {2});
    ASSERT_EQ(cluster_.leader_idx(), 0);
    drop_all_messages();

    // 提交 A 的 no-op，确保 has_committed_current_term_entry 通过
    replicate_until_committed(0, cluster_.node_ptr(0)->last_log_index(), {2});

    // 恢复 B。B 仍然缺少 index=idx 的日志；等它收到 A 的 ReadIndex 探测时，
    // 会先被 AppendEntries 的 term 推进到 read_term，再因为 prev_log 不存在而
    // reject。
    cluster_.restore(1);
    ASSERT_LT(cluster_.node_ptr(1)->last_log_index(), idx);

    // 隔离 C，quorum 只能靠 B
    cluster_.isolate(2);

    RaftEffects read_effects;
    LogIndex read_index = 0;
    Term read_term = 0;
    Status status = cluster_.node_ptr(0)->build_append_entries_for_read(
        read_effects, read_index, read_term);
    ASSERT_TRUE(status.ok()) << status.to_string();

    int success_cnt = 1;
    int limit = cluster_.node_ptr(0)->quorum_size();

    for (auto& msg : read_effects.messages) {
        int target = cluster_.find_node(msg.target.replica_id);
        ASSERT_GE(target, 0);
        if (cluster_.is_isolate(target)) continue;

        ASSERT_EQ(target, 1);
        ASSERT_EQ(msg.type, RaftMessageType::APPEND_ENTRIES);
        EXPECT_GT(msg.append_param.prev_log_index,
                  cluster_.node_ptr(target)->last_log_index());
        AppendEntriesResult res;
        RaftEffects effects;
        cluster_.node_ptr(target)->handle_append_entries(msg.append_param, res,
                                                         effects);
        Status drive_status = cluster_.drive_raft_effects(target, effects);
        ASSERT_TRUE(drive_status.ok()) << drive_status.to_string();

        // B 缺少 prev_log_index 对应日志，所以 reject。
        EXPECT_FALSE(res.success);
        // 但 term 匹配 read_term
        EXPECT_EQ(res.term, read_term);

        RaftEffects response_effects;
        IGNORE_RESULT(cluster_.node_ptr(0)->handle_append_response(
            msg.target.replica_id, msg.append_param, res, response_effects));
        Status response_status =
            cluster_.drive_raft_effects(0, std::move(response_effects));
        ASSERT_TRUE(response_status.ok()) << response_status.to_string();
        if (res.term == read_term) success_cnt++;
    }

    EXPECT_GE(success_cnt, limit);
}

}  // namespace adviskv::storage
