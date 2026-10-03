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
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status.h"
#include "common/model/model.h"
#include "storage/model/param.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/core/raft_core.h"
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
        cores_.clear();
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

            auto node = std::make_unique<RaftCore>(all_members_[i].replica_id,
                                                   all_members_);

            persists_.push_back(std::move(persist));
            cores_.push_back(std::move(node));
            active_.push_back(true);
            isolated_.push_back(false);
            effect_outboxes_.emplace_back();
            id_to_idx_[all_members_[i].replica_id] = i;
        }
    }

    // 这里对于isolate的节点也会tick，其实应该分的再仔细一点
    // 区分开来是网络分区还是宕机了
    void tick_node(int node_idx) {
        if (node_idx < 0 || node_idx >= static_cast<int>(cores_.size())) {
            return;
        }
        if (!active_[node_idx]) return;

        RaftEffects effects;
        cores_[node_idx]->tick(effects);
        Status status = drive_raft_effects(node_idx, std::move(effects));
        ASSERT_TRUE(status.ok()) << status.to_string();
    }

    void tick_all() {
        for (int i = 0; i < static_cast<int>(cores_.size()); i++) {
            tick_node(i);
        }
        route_all_messages();
    }

    // 只推进本地 timer，不自动路由消息；用于需要精确控制投递顺序的协议测试。
    void tick_cores_only() {
        for (int i = 0; i < static_cast<int>(cores_.size()); i++) {
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
            for (int i = 0; i < static_cast<int>(cores_.size()); i++) {
                if (!active_[i] || isolated_[i]) continue;
                auto role = cores_[i]->role();
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

    RaftCore* core_ptr(int idx) {
        if (idx < 0 || idx >= static_cast<int>(cores_.size())) return nullptr;
        return cores_[idx].get();
    }

    int size() const { return (int)cores_.size(); }

    bool is_active(int idx) const { return active_[idx]; }

    bool is_isolate(int idx) const { return isolated_[idx]; }

    int leader_idx() const {
        for (int i = 0; i < static_cast<int>(cores_.size()); i++) {
            if (!active_[i] || isolated_[i]) continue;
            if (cores_[i]->is_leader()) return i;
        }
        return -1;
    }

    RaftCore* leader_ptr() {
        int idx = leader_idx();
        return idx >= 0 ? cores_[idx].get() : nullptr;
    }

    int leader_count() const {
        int cnt = 0;
        for (int i = 0; i < static_cast<int>(cores_.size()); i++) {
            if (!active_[i] || isolated_[i]) continue;
            if (cores_[i]->is_leader()) cnt++;
        }
        return cnt;
    }

    std::vector<LogIndex> all_commit_indices() const {
        std::vector<LogIndex> result;
        for (int i = 0; i < static_cast<int>(cores_.size()); i++) {
            if (!active_[i] || isolated_[i]) continue;
            result.push_back(cores_[i]->commit_index());
        }
        return result;
    }

    // 判断当前group中没有宕机的节点里面，是否最新的commit_idx都是大于等于target的
    bool all_committed_at_least(LogIndex target) const {
        for (int i = 0; i < static_cast<int>(cores_.size()); i++) {
            if (!active_[i] || isolated_[i]) continue;
            if (cores_[i]->commit_index() < target) return false;
        }
        return true;
    }

    int find_node(const ReplicaID& rid) const {
        auto it = id_to_idx_.find(rid);
        return (it != id_to_idx_.end()) ? it->second : -1;
    }

    ReplicaID replica_id(int idx) const { return all_members_[idx].replica_id; }

    PeerMember peer_member(int idx) const { return all_members_[idx]; }

    void add_node(const PeerMember& member,
                  const std::vector<PeerMember>& initial_members) {
        auto dir = adviskv::test::make_unique_test_dir(
            "raft_cluster_t" + std::to_string(table_id_) + "_n" +
                std::to_string(all_members_.size()),
            0);
        fs::create_directories(dir);
        temp_dirs_.push_back(dir);

        auto persist =
            std::make_unique<PersistEngine>(dir.string(), member.replica_id);
        Status s = persist->init();
        ASSERT_TRUE(s.ok()) << s.to_string();

        auto node =
            std::make_unique<RaftCore>(member.replica_id, initial_members);

        id_to_idx_[member.replica_id] = static_cast<int>(cores_.size());
        all_members_.push_back(member);
        persists_.push_back(std::move(persist));
        cores_.push_back(std::move(node));
        active_.push_back(true);
        isolated_.push_back(false);
        effect_outboxes_.emplace_back();
    }

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

    void apply_committed_entries(int node_idx) {
        std::vector<LogEntry> entries =
            cores_[node_idx]->extract_committed_entries();
        for (const LogEntry& entry : entries) {
            Status status = Status::OK();
            if (is_config_change_op(entry.op_type)) {
                status = cores_[node_idx]->apply_config_entry(entry);
            } else {
                cores_[node_idx]->advance_last_applied(entry.index);
            }
            ASSERT_TRUE(status.ok()) << status.to_string();
        }
    }

    void apply_all_committed_entries() {
        for (int i = 0; i < static_cast<int>(cores_.size()); i++) {
            if (!active_[i]) continue;
            apply_committed_entries(i);
        }
    }

    std::pair<Status, LogIndex> propose(int node_idx, WriteOpType op_type,
                                        Key key, Value value) {
        RaftEffects effects;
        auto result = cores_[node_idx]->propose(
            ProposeParam::write(op_type, std::move(key), std::move(value)),
            effects);
        if (result.first.fail()) return result;

        Status status = drive_raft_effects(node_idx, std::move(effects));
        if (status.fail()) return {status, -1};
        return result;
    }

    int route_messages_from(
        int source_idx,
        const std::function<bool(const RaftMessage&, int)>& should_route) {
        if (source_idx < 0 || source_idx >= static_cast<int>(cores_.size())) {
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
        if (source_idx < 0 || source_idx >= static_cast<int>(cores_.size())) {
            return 0;
        }
        int dropped = static_cast<int>(effect_outboxes_[source_idx].size());
        effect_outboxes_[source_idx].clear();
        return dropped;
    }

    // 只读观察 outbox（测试断言消息内容用，不消费）
    const std::vector<RaftMessage>& outbox_of(int node_idx) const {
        return effect_outboxes_[node_idx];
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
        cores_.clear();
        persists_.clear();
        cleanup_temp_dirs();
    }

   private:
    void route_message(int source_idx, int target_idx, RaftMessage& msg) {
        switch (msg.type) {
            case RaftMessageType::REQUEST_VOTE: {
                RequestVoteResult result;
                RaftEffects target_effects;
                cores_[target_idx]->handle_request_vote(msg.vote_param, result,
                                                        target_effects);
                Status target_status =
                    drive_raft_effects(target_idx, std::move(target_effects));
                ASSERT_TRUE(target_status.ok()) << target_status.to_string();
                RaftEffects response_effects;
                cores_[source_idx]->handle_vote_response(
                    msg.target.replica_id, result, response_effects);
                Status status =
                    drive_raft_effects(source_idx, std::move(response_effects));
                ASSERT_TRUE(status.ok()) << status.to_string();
                break;
            }

            case RaftMessageType::PRE_VOTE: {
                PreVoteResult result;
                RaftEffects target_effects;
                cores_[target_idx]->handle_pre_vote(msg.pre_vote_param, result,
                                                    target_effects);
                Status target_status =
                    drive_raft_effects(target_idx, std::move(target_effects));
                ASSERT_TRUE(target_status.ok()) << target_status.to_string();
                RaftEffects response_effects;
                cores_[source_idx]->handle_pre_vote_response(
                    msg.target.replica_id, result, response_effects);
                Status status =
                    drive_raft_effects(source_idx, std::move(response_effects));
                ASSERT_TRUE(status.ok()) << status.to_string();
                break;
            }

            case RaftMessageType::APPEND_ENTRIES: {
                AppendEntriesResult result;
                RaftEffects effects;
                cores_[target_idx]->handle_append_entries(msg.append_param,
                                                          result, effects);
                Status status =
                    drive_raft_effects(target_idx, std::move(effects));
                ASSERT_TRUE(status.ok()) << status.to_string();
                RaftEffects response_effects;
                IGNORE_RESULT(cores_[source_idx]->handle_append_response(
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
                Status s = cores_[target_idx]->prepare_install_snapshot(
                    msg.snapshot_param, prepare_effects);
                Status prepare_status =
                    drive_raft_effects(target_idx, std::move(prepare_effects));
                ASSERT_TRUE(prepare_status.ok()) << prepare_status.to_string();

                InstallSnapshotResult result;
                result.term = cores_[target_idx]->current_term();
                result.status = s;

                if (s.ok()) {
                    InstallSnapshotContext context;
                    context.snapshot_index = msg.snapshot_param.snapshot_index;
                    context.snapshot_term = msg.snapshot_param.snapshot_term;
                    RaftEffects install_effects;
                    cores_[target_idx]->commit_install_snapshot(
                        context, install_effects);
                    Status install_status = drive_raft_effects(
                        target_idx, std::move(install_effects));
                    ASSERT_TRUE(install_status.ok())
                        << install_status.to_string();
                }
                if (s.code() == StatusCode::ALREADY_EXIST ||
                    (s.fail() && msg.snapshot_param.snapshot_index <=
                                     cores_[target_idx]->snapshot_index())) {
                    if (s.code() == StatusCode::ALREADY_EXIST) {
                        result.snapshot_watermark =
                            msg.snapshot_param.snapshot_index;
                    } else {
                        result.snapshot_watermark =
                            cores_[target_idx]->snapshot_index();
                    }
                }

                RaftEffects response_effects;
                cores_[source_idx]->handle_install_snapshot_response(
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
        for (int i = 0; i < static_cast<int>(cores_.size()); i++) {
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
    std::vector<std::unique_ptr<RaftCore>> cores_;
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
        RaftCore* leader = cluster_.core_ptr(leader_idx);
        RaftCore* to = cluster_.core_ptr(node_idx);
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
        cluster_.core_ptr(node_idx)->become_candidate_for_test(effects);
        Status status =
            cluster_.drive_raft_effects(node_idx, std::move(effects));
        ASSERT_TRUE(status.ok()) << status.to_string();
    }

    void tick_heartbeat(int node_idx) {
        cluster_.core_ptr(node_idx)->reset_heartbeat_tick_for_test(0);
        cluster_.tick_node(node_idx);
        cluster_.core_ptr(node_idx)->reset_heartbeat_tick_for_test(3);
    }

    void set_heartbeat_tick(int node_idx, int val) {
        cluster_.core_ptr(node_idx)->reset_heartbeat_tick_for_test(val);
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
             cluster_.core_ptr(leader_idx)->commit_index() < target_idx;
             i++) {
            replicate_once(leader_idx, followers);
            tick_heartbeat(leader_idx);
        }
        ASSERT_GE(cluster_.core_ptr(leader_idx)->commit_index(), target_idx);
    }

    void assert_committed_at_least(const std::vector<int>& node_indices,
                                   LogIndex target_idx) {
        for (int idx : node_indices) {
            ASSERT_GE(cluster_.core_ptr(idx)->commit_index(), target_idx)
                << "node_idx=" << idx;
        }
    }

    std::vector<LogEntry> get_node_entries(int idx) {
        RaftCore* node = cluster_.core_ptr(idx);
        if (!node) return {};
        return node->log_entries_for_test();
    }

    std::pair<LogIndex, Term> get_node_snapshot(int idx) {
        RaftCore* node = cluster_.core_ptr(idx);
        if (!node) return {};
        return node->snapshot_for_test();
    }

    void set_node_next_index(int node_idx, const ReplicaID& target,
                             LogIndex val) {
        RaftCore* node = cluster_.core_ptr(node_idx);
        if (!node) return;
        node->set_next_index_for_test(target, val);
    }

    LogIndex get_node_next_index(int node_idx, const ReplicaID& target) {
        RaftCore* node = cluster_.core_ptr(node_idx);
        if (!node) return 0;
        return node->next_index_for_test(target);
    }

    LogIndex get_snapshot_watermark(int node_idx, const ReplicaID& target) {
        RaftCore* node = cluster_.core_ptr(node_idx);
        if (!node) return 0;
        return node->snapshot_watermark_for_test(target);
    }

    LogIndex get_inflight_snapshot_index(int node_idx,
                                         const ReplicaID& target) {
        RaftCore* node = cluster_.core_ptr(node_idx);
        if (!node) return 0;
        return node->inflight_snapshot_index_for_test(target);
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
            ASSERT_EQ(cluster_.core_ptr(i)->role(), ReplicaRole::FOLLOWER);
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
        auto* nl = cluster_.core_ptr(new_leader_idx);
        ASSERT_NE(nl, nullptr);
        auto [status, idx] = cluster_.propose(new_leader_idx, WriteOpType::PUT,
                                              "round_" + std::to_string(round),
                                              "val_" + std::to_string(round));
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(tick_until_all_committed(idx));
    }
}
// 正常 3 节点下，日志都一致，follower发起pre-vote后无法拿到quorum，保持PRE_CANDIDATE
TEST_F(RaftClusterTest, HealthyLeaderNotDisplacedByEqualLogPreVote) {
    create_and_set_0_leader(3);
    ASSERT_EQ(cluster_.leader_idx(), 0);

    tick_heartbeat(0);
    EXPECT_EQ(cluster_.route_messages_from_to(0, RaftMessageType::APPEND_ENTRIES, {1, 2}), 2);

    const Term leader_term = cluster_.core_ptr(0)->current_term();
    RaftEffects pre_vote_effects;
    cluster_.core_ptr(1)->become_pre_candidate_for_test(pre_vote_effects);
    ASSERT_TRUE(cluster_.drive_raft_effects(1, std::move(pre_vote_effects)).ok());

    EXPECT_EQ(cluster_.route_messages_from_to(1, RaftMessageType::PRE_VOTE, {0, 2}), 2);
    EXPECT_EQ(cluster_.core_ptr(0)->role(), ReplicaRole::LEADER);
    EXPECT_EQ(cluster_.core_ptr(0)->current_term(), leader_term);
    EXPECT_EQ(cluster_.core_ptr(1)->role(), ReplicaRole::PRE_CANDIDATE);
    EXPECT_EQ(cluster_.core_ptr(1)->current_term(), leader_term);
}

// 旧 Leader 被隔离后，多数派选出新 Leader；旧 Leader 恢复时应该因为收到
// 更高 term 的消息退回 follower，不能再触发一次无意义的切主。
TEST_F(RaftClusterTest, OldLeaderReturnsAfterMajorityFailoverWithoutSecondElection) {
    create_and_set_0_leader(3);
    ASSERT_TRUE(tick_until_all_committed(1));
    ASSERT_EQ(cluster_.leader_idx(), 0);

    constexpr int old_leader_idx = 0;
    const Term old_term = cluster_.core_ptr(old_leader_idx)->current_term();

    cluster_.isolate(old_leader_idx);
    ASSERT_TRUE(cluster_.tick_until_stable());

    const int new_leader_idx = cluster_.leader_idx();
    ASSERT_NE(new_leader_idx, -1);
    ASSERT_NE(new_leader_idx, old_leader_idx);
    const Term new_term = cluster_.core_ptr(new_leader_idx)->current_term();
    ASSERT_GT(new_term, old_term);

    auto [status, committed_idx] = cluster_.propose(
        new_leader_idx, WriteOpType::PUT, "majority-key", "majority-value");
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(tick_until_all_committed(committed_idx));

    // 旧 Leader 在网络分区期间仍可能保持 LEADER 身份，但不能自行抬高 term。
    for (int i = 0; i < 100; ++i) {
        cluster_.tick_node(old_leader_idx);
    }
    EXPECT_EQ(cluster_.core_ptr(old_leader_idx)->role(), ReplicaRole::LEADER);
    EXPECT_EQ(cluster_.core_ptr(old_leader_idx)->current_term(), old_term);

    cluster_.restore(old_leader_idx);
    bool old_leader_stepped_down = false;
    for (int i = 0; i < 30; ++i) {
        cluster_.tick_all();
        ASSERT_LE(cluster_.leader_count(), 1);
        if (cluster_.core_ptr(old_leader_idx)->role() == ReplicaRole::FOLLOWER) {
            old_leader_stepped_down = true;
        }
    }

    ASSERT_TRUE(old_leader_stepped_down);
    ASSERT_EQ(cluster_.leader_idx(), new_leader_idx);
    ASSERT_EQ(cluster_.core_ptr(new_leader_idx)->current_term(), new_term);
    ASSERT_EQ(cluster_.core_ptr(old_leader_idx)->role(), ReplicaRole::FOLLOWER);
}

// PreVote 真正保护的是隔离分区里的非 Leader 节点：它反复超时也只能发起
// PRE_VOTE，不能把 current_term 抬高；恢复后这些请求不会把多数派 Leader
// 打回 follower，也不会制造第二次选举。
TEST_F(RaftClusterTest, PartitionedFollowerPreVoteDoesNotDisturbMajorityLeader) {
    create_and_set_0_leader(3);
    ASSERT_TRUE(tick_until_all_committed(1));
    ASSERT_EQ(cluster_.leader_idx(), 0);

    constexpr int leader_idx = 0;
    constexpr int partitioned_follower_idx = 1;
    const Term stable_term = cluster_.core_ptr(leader_idx)->current_term();

    cluster_.isolate(partitioned_follower_idx);
    for (int i = 0; i < 200; ++i) {
        cluster_.tick_all();
    }

    ASSERT_EQ(cluster_.core_ptr(leader_idx)->role(), ReplicaRole::LEADER);
    ASSERT_EQ(cluster_.core_ptr(leader_idx)->current_term(), stable_term);
    ASSERT_EQ(cluster_.core_ptr(partitioned_follower_idx)->current_term(), stable_term);
    ASSERT_EQ(cluster_.core_ptr(partitioned_follower_idx)->role(), ReplicaRole::PRE_CANDIDATE);

    cluster_.restore(partitioned_follower_idx);
    for (int i = 0; i < 30; ++i) {
        cluster_.tick_all();
        ASSERT_EQ(cluster_.leader_count(), 1);
        ASSERT_EQ(cluster_.leader_idx(), leader_idx);
        ASSERT_EQ(cluster_.core_ptr(leader_idx)->current_term(), stable_term);
    }

    ASSERT_EQ(cluster_.core_ptr(partitioned_follower_idx)->role(), ReplicaRole::FOLLOWER);
    ASSERT_EQ(cluster_.core_ptr(partitioned_follower_idx)->current_term(), stable_term);
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

// 验证成员变更闭环：新节点先以 NON_MEMBER 创建，leader 加为 LEARNER，追齐日志后自动提升为 VOTER。
TEST_F(RaftClusterTest, AddLearnerCatchesUpAndPromotesToVoter) {
    create_and_stabilize(3);
    int leader_idx = cluster_.leader_idx();
    ASSERT_GE(leader_idx, 0);

    PeerMember new_member;
    new_member.node_id = "node_3";
    new_member.replica_id = ReplicaID{cluster_.replica_id(0).table_id, 0, 3};
    new_member.endpoint = {"127.0.0.1", 9003};
    std::vector<PeerMember> old_voters{
        cluster_.peer_member(0),
        cluster_.peer_member(1),
        cluster_.peer_member(2),
    };
    cluster_.add_node(new_member, old_voters);
    ASSERT_EQ(cluster_.core_ptr(3)->member_type(new_member.replica_id),
              RaftMemberType::NON_MEMBER);

    RaftEffects effects;
    Status status =
        cluster_.core_ptr(leader_idx)->ensure_add_learner(new_member, effects);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(
        cluster_.drive_raft_effects(leader_idx, std::move(effects)).ok());

    bool promoted = false;
    for (int i = 0; i < 200 && !promoted; i++) {
        cluster_.tick_all();
        cluster_.apply_all_committed_entries();
        promoted =
            cluster_.core_ptr(leader_idx)->member_type(new_member.replica_id) ==
                RaftMemberType::VOTER &&
            cluster_.core_ptr(3)->member_type(new_member.replica_id) ==
                RaftMemberType::VOTER;
    }

    ASSERT_TRUE(promoted);
}

// 当同一个 ADD_LEARNER 已经进入日志但还没 apply 时，重复提交应直接返回 OK。
TEST_F(RaftClusterTest, PendingSameAddLearnerReturnsOk) {
    create_and_stabilize(3);
    int leader_idx = cluster_.leader_idx();
    ASSERT_GE(leader_idx, 0);

    PeerMember new_member;
    new_member.node_id = "node_3";
    new_member.replica_id = ReplicaID{cluster_.replica_id(0).table_id, 0, 3};
    new_member.endpoint = {"127.0.0.1", 9003};

    RaftEffects first_effects;
    Status first_status =
        cluster_.core_ptr(leader_idx)->ensure_add_learner(new_member, first_effects);
    ASSERT_TRUE(first_status.ok()) << first_status.to_string();

    RaftEffects second_effects;
    Status second_status =
        cluster_.core_ptr(leader_idx)->ensure_add_learner(new_member, second_effects);
    EXPECT_TRUE(second_status.ok()) << second_status.to_string();
}

// 验证缩容成员变更：REMOVE_MEMBER 提交并 apply 后，被移除节点本地变为 NON_MEMBER。
TEST_F(RaftClusterTest, RemoveMemberTurnsTargetIntoNonMember) {
    create_and_stabilize(3);
    int leader_idx = cluster_.leader_idx();
    ASSERT_GE(leader_idx, 0);

    int victim_idx = (leader_idx == 0) ? 1 : 0;
    ReplicaID victim = cluster_.replica_id(victim_idx);
    ASSERT_EQ(cluster_.core_ptr(victim_idx)->member_type(victim),
              RaftMemberType::VOTER);

    RaftEffects effects;
    Status status =
        cluster_.core_ptr(leader_idx)->ensure_remove_member(victim, effects);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(
        cluster_.drive_raft_effects(leader_idx, std::move(effects)).ok());

    bool removed = false;
    for (int i = 0; i < 100 && !removed; i++) {
        cluster_.tick_all();
        cluster_.apply_all_committed_entries();
        removed = cluster_.core_ptr(victim_idx)->member_type(victim) ==
                  RaftMemberType::NON_MEMBER;
    }

    ASSERT_TRUE(removed);
}

// 当同一个 REMOVE_MEMBER 已经进入日志但还没 apply 时，重复提交应直接返回 OK。
TEST_F(RaftClusterTest, PendingSameRemoveMemberReturnsOk) {
    create_and_stabilize(3);
    int leader_idx = cluster_.leader_idx();
    ASSERT_GE(leader_idx, 0);

    int victim_idx = (leader_idx == 0) ? 1 : 0;
    ReplicaID victim = cluster_.replica_id(victim_idx);

    RaftEffects first_effects;
    Status first_status =
        cluster_.core_ptr(leader_idx)->ensure_remove_member(victim, first_effects);
    ASSERT_TRUE(first_status.ok()) << first_status.to_string();

    RaftEffects second_effects;
    Status second_status =
        cluster_.core_ptr(leader_idx)->ensure_remove_member(victim, second_effects);
    EXPECT_TRUE(second_status.ok()) << second_status.to_string();
}

// 当已有别的 config change 在处理中时，新的不同成员变更应返回 RETRY_ERROR。
TEST_F(RaftClusterTest, DifferentConfigChangeReturnsRetryErrorWhenAnotherIsPending) {
    create_and_stabilize(3);
    int leader_idx = cluster_.leader_idx();
    ASSERT_GE(leader_idx, 0);

    PeerMember new_member;
    new_member.node_id = "node_3";
    new_member.replica_id = ReplicaID{cluster_.replica_id(0).table_id, 0, 3};
    new_member.endpoint = {"127.0.0.1", 9003};

    RaftEffects add_effects;
    Status add_status =
        cluster_.core_ptr(leader_idx)->ensure_add_learner(new_member, add_effects);
    ASSERT_TRUE(add_status.ok()) << add_status.to_string();

    int victim_idx = (leader_idx == 0) ? 1 : 0;
    ReplicaID victim = cluster_.replica_id(victim_idx);

    RaftEffects remove_effects;
    Status remove_status =
        cluster_.core_ptr(leader_idx)->ensure_remove_member(victim, remove_effects);
    EXPECT_EQ(remove_status.code(), StatusCode::RETRY_ERROR)
        << remove_status.to_string();
}

// leader的写操作没有达到多数派，commit_idx不会被更新
TEST_F(RaftClusterTest, MinorityLeaderCannotCommitNewEntry) {
    create_and_set_0_leader(5);
    ASSERT_TRUE(tick_until_all_committed(1));
    drop_all_messages();

    LogIndex base_commit = cluster_.core_ptr(0)->commit_index();
    LogIndex uncommitted_idx =
        propose_and_replicate_once(0, {1}, WriteOpType::PUT, "111", "111");

    ASSERT_GT(uncommitted_idx, base_commit);
    ASSERT_EQ(cluster_.core_ptr(0)->commit_index(), base_commit);
    ASSERT_LT(cluster_.core_ptr(0)->commit_index(), uncommitted_idx);
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

    RaftCore* leader = cluster_.core_ptr(leader_idx);
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

    RaftCore* leader = cluster_.core_ptr(leader_idx);
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

    Term higher_term = cluster_.core_ptr(0)->current_term() + 1;
    RequestVoteResult vote_result;
    RaftEffects vote_effects;
    cluster_.core_ptr(0)->handle_request_vote(
        RequestVoteParam{cluster_.replica_id(1), cluster_.replica_id(0),
                         higher_term, 0, 0},
        vote_result, vote_effects);
    Status status = cluster_.drive_raft_effects(0, std::move(vote_effects));
    ASSERT_TRUE(status.ok()) << status.to_string();

    ASSERT_EQ(cluster_.core_ptr(0)->current_term(), higher_term);
    ASSERT_EQ(cluster_.core_ptr(0)->role(), ReplicaRole::FOLLOWER);
    ASSERT_EQ(cluster_.leader_idx(), -1);

    elect_with_votes(2, {0, 1});
    ASSERT_EQ(cluster_.leader_idx(), 2);
}

TEST_F(RaftClusterTest, LaggingFollowersCatchUpCommittedLogs) {
    create_and_stabilize(5);
    cluster_.isolate(3);
    cluster_.isolate(4);
    cluster_.tick_until_stable();
    RaftCore* leader = cluster_.leader_ptr();
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
    ASSERT_NE(cluster_.core_ptr(3), nullptr);
    ASSERT_NE(cluster_.core_ptr(4), nullptr);
    ASSERT_GE(cluster_.core_ptr(3)->commit_index(), 0);  //
    ASSERT_GE(cluster_.core_ptr(4)->commit_index(), 0);

    ASSERT_LE(cluster_.core_ptr(3)->commit_index(), 1);  //
    ASSERT_LE(cluster_.core_ptr(4)->commit_index(), 1);
    ASSERT_TRUE(tick_until_all_committed(last_idx));
    ASSERT_EQ(cluster_.core_ptr(3)->commit_index(), last_idx);
    ASSERT_EQ(cluster_.core_ptr(4)->commit_index(), last_idx);
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
    ASSERT_EQ(cluster_.core_ptr(0)->commit_index(), 1);

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
        ASSERT_EQ(cluster_.core_ptr(0)->commit_index(), 1);

        LogIndex a_current_term_idx =
            propose_and_replicate_once(0, {2}, WriteOpType::PUT, "3", "3");
        ASSERT_EQ(a_current_term_idx, 4);
        ASSERT_EQ(get_node_entries(2).back().key, "3");
        ASSERT_EQ(cluster_.core_ptr(0)->commit_index(), 1);
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
        ASSERT_EQ(cluster_.core_ptr(4)->commit_index(), final_idx);
        ASSERT_LT(cluster_.core_ptr(1)->commit_index(), final_idx);
        ASSERT_LT(cluster_.core_ptr(3)->commit_index(), final_idx);

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
    auto* leader = cluster_.core_ptr(0);
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
        cluster_.core_ptr(i)->advance_last_applied(last_idx);
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
    int leader_idx = cluster_.leader_idx();
    ASSERT_GE(leader_idx, 0);

    constexpr int kThreads = 4;
    constexpr int kOpsPerThread = 25;
    std::atomic<int> success_count{0};
    std::vector<std::thread> threads;
    std::mutex raft_core_mutex;

    for (int t = 0; t < kThreads; t++) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < kOpsPerThread; i++) {
                auto kv = "concurrent_t" + std::to_string(t) + "_i" +
                          std::to_string(i);
                auto vv = "value";
                Status status;
                {
                    // RaftCore 本身要求外层持有 raft_core_mutex_；这里模拟
                    // Replica/ReplicaLoop 的真实同步边界。
                    std::lock_guard lock(raft_core_mutex);
                    std::tie(status, std::ignore) = cluster_.propose(
                        leader_idx, WriteOpType::PUT, Key(kv), Value(vv));
                }
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
    ASSERT_EQ(cluster_.core_ptr(0)->commit_index(), target_idx);

    cluster_.core_ptr(1)->enter_recovering();
    ASSERT_TRUE(cluster_.core_ptr(1)->is_recovering());

    // 检测他不会成为candidate的
    for (int i = 0; i < 50; i++) {
        cluster_.tick_node(1);
    }
    ASSERT_EQ(cluster_.core_ptr(1)->role(), ReplicaRole::FOLLOWER);

    // 无法进行propose操作， 虽然他本来就是follower没办法吧。。。
    auto [status, idx] = cluster_.propose(1, WriteOpType::PUT, "bad", "bad");
    ASSERT_TRUE(status.code() == StatusCode::IS_RECOVERING);
    ASSERT_EQ(idx, -1);

    // 投票也会失败的
    RequestVoteResult vote_result;
    RaftEffects vote_effects;
    cluster_.core_ptr(1)->handle_request_vote(
        RequestVoteParam{ReplicaID{500, 0, 2}, ReplicaID{500, 0, 1},
                         cluster_.core_ptr(1)->current_term(), 0, 0},
        vote_result, vote_effects);
    Status vote_status =
        cluster_.drive_raft_effects(1, std::move(vote_effects));
    ASSERT_TRUE(vote_status.ok()) << vote_status.to_string();
    ASSERT_FALSE(vote_result.vote_granted);

    cluster_.restore(1);
    tick_heartbeat(0);
    ASSERT_EQ(replicate_once(0, {1}), 1);

    ASSERT_EQ(cluster_.core_ptr(1)->commit_index(), target_idx);
    ASSERT_FALSE(cluster_.core_ptr(1)->is_recovering());
}

// 检测一个leader如果突然recovering的话， 表现是否正常
// leader进入recovering之后， 应该会退回follower， 不会继续进行心跳。
// 之后剩下的节点应该可以重新选出来新的leader。
TEST_F(RaftClusterTest, RecoveringLeaderStepsDownAndNewLeaderElected) {
    create_and_set_0_leader(3);
    ASSERT_TRUE(tick_until_all_committed(1));
    ASSERT_EQ(cluster_.leader_idx(), 0);
    drop_all_messages();

    cluster_.core_ptr(0)->enter_recovering();
    ASSERT_TRUE(cluster_.core_ptr(0)->is_recovering());
    ASSERT_EQ(cluster_.core_ptr(0)->role(), ReplicaRole::FOLLOWER);
    ASSERT_EQ(cluster_.leader_idx(), -1);

    auto [status, idx] = cluster_.propose(0, WriteOpType::PUT, "111", "111");
    ASSERT_TRUE(status.code() == StatusCode::IS_RECOVERING);
    ASSERT_EQ(idx, -1);

    // recovering的leader不会继续tick出来心跳，也不会重新成为candidate
    for (int i = 0; i < 50; i++) {
        cluster_.tick_node(0);
    }
    ASSERT_EQ(cluster_.drop_messages_from(0), 0);
    ASSERT_EQ(cluster_.core_ptr(0)->role(), ReplicaRole::FOLLOWER);

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
    ASSERT_EQ(cluster_.core_ptr(0)->commit_index(), last_idx);

    cluster_.core_ptr(0)->advance_last_applied(last_idx);
    ASSERT_TRUE(cluster_.core_ptr(0)->truncate_log(4).ok());
    ASSERT_EQ(cluster_.core_ptr(0)->snapshot_index(), 4);
    ASSERT_EQ(cluster_.core_ptr(2)->commit_index(), 1);

    cluster_.restore(2);

    tick_heartbeat(0);
    ASSERT_EQ(cluster_.route_messages_from_to(
                  0, RaftMessageType::INSTALL_SNAPSHOT, {2}),
              1);
    ASSERT_EQ(cluster_.core_ptr(2)->snapshot_index(), 4);
    ASSERT_EQ(cluster_.core_ptr(2)->commit_index(), 4);

    tick_heartbeat(0);
    ASSERT_EQ(replicate_once(0, {2}), 1);

    ASSERT_EQ(cluster_.core_ptr(2)->commit_index(), last_idx);
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
    std::vector<ReplicaID> voters;
    Status status = cluster_.core_ptr(2)->build_read_probe(
        read_effects, read_index, read_term, voters, /*read_probe_seq=*/1);

    EXPECT_EQ(status.code(), StatusCode::READ_INDEX_NOT_READY);
    EXPECT_TRUE(read_effects.messages.empty());

    LogIndex current_term_idx = cluster_.core_ptr(2)->last_log_index();
    replicate_until_committed(2, current_term_idx, {1});

    status = cluster_.core_ptr(2)->build_read_probe(
        read_effects, read_index, read_term, voters, /*read_probe_seq=*/2);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(read_index, cluster_.core_ptr(2)->commit_index());
    EXPECT_EQ(read_term, cluster_.core_ptr(2)->current_term());
    EXPECT_FALSE(read_effects.messages.empty());
}

// 纯探测不变量（F2/F3/F15，取代旧的"INSTALL_SNAPSHOT 计票"语义）：
// follower 落后到需要快照时，读探测依然以 heartbeat 形参到达它并拿到
// term 匹配的响应（拒绝 prev 不对齐同样是任期确认证据）；探测绝不产出
// INSTALL_SNAPSHOT、绝不触碰 next/match 进度——快照追赶只属于复制路径。
TEST_F(RaftClusterTest, ReadProbeReachesLaggingVoterWithoutSnapshotMsg) {
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
    ASSERT_EQ(cluster_.core_ptr(0)->commit_index(), last_idx);

    cluster_.core_ptr(0)->advance_last_applied(last_idx);
    ASSERT_TRUE(cluster_.core_ptr(0)->truncate_log(4).ok());
    ASSERT_EQ(cluster_.core_ptr(0)->snapshot_index(), 4);

    // node 2 严重落后（其 next 在快照边界之下）。读探测无视 next_index，
    // 直接 heartbeat 形参覆盖全部 voter。
    set_node_next_index(0, cluster_.replica_id(2), 3);
    const LogIndex next_before = get_node_next_index(0, cluster_.replica_id(2));
    const LogIndex inflight_before =
        get_inflight_snapshot_index(0, cluster_.replica_id(2));

    RaftEffects read_effects;
    LogIndex read_index = 0;
    Term read_term = 0;
    std::vector<ReplicaID> voters;
    Status status = cluster_.core_ptr(0)->build_read_probe(
        read_effects, read_index, read_term, voters, /*read_probe_seq=*/1);
    ASSERT_TRUE(status.ok()) << status.to_string();

    int probe_count = 0;
    int confirmed = 1;  // self 天然确认
    const int limit = cluster_.core_ptr(0)->quorum_size();
    for (const RaftMessage& msg : read_effects.messages) {
        ASSERT_EQ(msg.type, RaftMessageType::APPEND_ENTRIES)
            << "pure probe must never emit INSTALL_SNAPSHOT (F3)";
        ASSERT_TRUE(msg.append_param.read_probe_seq != 0);
        probe_count++;

        int target = cluster_.find_node(msg.target.replica_id);
        ASSERT_GE(target, 0);
        AppendEntriesResult res;
        RaftEffects effects;
        cluster_.core_ptr(target)->handle_append_entries(msg.append_param, res, effects);
        Status drive_status = cluster_.drive_raft_effects(target, effects);
        ASSERT_TRUE(drive_status.ok()) << drive_status.to_string();
        // 落后的 voter 会拒绝 prev 对账，但 term 匹配的响应仍是任期确认
        //（F15：计票看 term，不看 success）。
        if (res.term == read_term) confirmed++;
    }

    // 全部 2 个 follower 都被探测到，self + 它们凑满多数派。
    EXPECT_EQ(probe_count, 2);
    EXPECT_GE(confirmed, limit);
    // 探测不污染复制进度（C.7 回归断言）。
    EXPECT_EQ(get_node_next_index(0, cluster_.replica_id(2)), next_before);
    EXPECT_EQ(get_inflight_snapshot_index(0, cluster_.replica_id(2)), inflight_before);
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
    ASSERT_EQ(cluster_.core_ptr(0)->commit_index(), last_idx);

    cluster_.core_ptr(0)->advance_last_applied(last_idx);
    ASSERT_TRUE(cluster_.core_ptr(0)->truncate_log(4).ok());
    ASSERT_EQ(cluster_.core_ptr(0)->snapshot_index(), 4);

    set_node_next_index(0, cluster_.replica_id(2), 3);

    // 心跳驱动复制路径产出 INSTALL_SNAPSHOT（读探测不产快照，F3；旧的
    // build_append_entries_for_read 已删除，快照消息只属于复制路径）。
    tick_heartbeat(0);

    bool found_snapshot = false;
    RaftMessage snapshot_msg;
    for (const auto& msg : cluster_.outbox_of(0)) {
        if (msg.type == RaftMessageType::INSTALL_SNAPSHOT &&
            msg.target.replica_id == cluster_.replica_id(2)) {
            snapshot_msg = msg;
            found_snapshot = true;
            break;
        }
    }
    ASSERT_TRUE(found_snapshot);
    ASSERT_EQ(snapshot_msg.snapshot_param.snapshot_index, 4);

    ASSERT_TRUE(cluster_.core_ptr(0)->truncate_log(last_idx).ok());
    ASSERT_EQ(cluster_.core_ptr(0)->snapshot_index(), last_idx);

    InstallSnapshotResult res;
    res.term = cluster_.core_ptr(0)->current_term();
    res.status = Status::OK();
    res.snapshot_watermark = 0;

    RaftEffects response_effects;
    cluster_.core_ptr(0)->handle_install_snapshot_response(
        snapshot_msg.target.replica_id, snapshot_msg.snapshot_param, res,
        response_effects);

    EXPECT_EQ(get_node_next_index(0, snapshot_msg.target.replica_id),
              snapshot_msg.snapshot_param.snapshot_index + 1);
    EXPECT_EQ(get_snapshot_watermark(0, snapshot_msg.target.replica_id),
              snapshot_msg.snapshot_param.snapshot_index);
    EXPECT_EQ(get_inflight_snapshot_index(0, snapshot_msg.target.replica_id),
              0);
}
TEST_F(RaftClusterTest, InstallSnapshotCoveredWatermarkUpdatesProgress) {
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
    ASSERT_EQ(cluster_.core_ptr(0)->commit_index(), last_idx);

    cluster_.core_ptr(0)->advance_last_applied(last_idx);
    ASSERT_TRUE(cluster_.core_ptr(0)->truncate_log(4).ok());
    ASSERT_EQ(cluster_.core_ptr(0)->snapshot_index(), 4);

    set_node_next_index(0, cluster_.replica_id(2), 3);

    // 心跳驱动复制路径产出 INSTALL_SNAPSHOT（同 InstallSnapshotSuccessUsesSentSnapshotIndex）。
    tick_heartbeat(0);

    bool found_snapshot = false;
    RaftMessage snapshot_msg;
    for (const auto& msg : cluster_.outbox_of(0)) {
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
    res.term = cluster_.core_ptr(0)->current_term();
    res.status = Status::ALREADY_EXIST("follower already has sent snapshot");
    res.snapshot_watermark = snapshot_msg.snapshot_param.snapshot_index;

    RaftEffects response_effects;
    cluster_.core_ptr(0)->handle_install_snapshot_response(
        snapshot_msg.target.replica_id, snapshot_msg.snapshot_param, res,
        response_effects);

    EXPECT_EQ(get_node_next_index(0, snapshot_msg.target.replica_id),
              snapshot_msg.snapshot_param.snapshot_index + 1);
    EXPECT_EQ(get_snapshot_watermark(0, snapshot_msg.target.replica_id),
              snapshot_msg.snapshot_param.snapshot_index);
    EXPECT_EQ(get_inflight_snapshot_index(0, snapshot_msg.target.replica_id),
              0);
}

TEST_F(RaftClusterTest, InstallSnapshotHigherWatermarkUpdatesProgress) {    create_and_set_0_leader(3);
    ASSERT_TRUE(tick_until_all_committed(1));
    ASSERT_EQ(cluster_.leader_idx(), 0);
    drop_all_messages();

    LogIndex last_idx = 0;
    for (int i = 0; i < 5; i++) {
        last_idx = propose_and_replicate_once(0, {1, 2}, WriteOpType::PUT,
                                              "snap_ahead_" + std::to_string(i),
                                              "v" + std::to_string(i));
    }
    ASSERT_EQ(cluster_.core_ptr(0)->commit_index(), last_idx);

    cluster_.core_ptr(0)->advance_last_applied(last_idx);
    ASSERT_TRUE(cluster_.core_ptr(0)->truncate_log(4).ok());
    ASSERT_EQ(cluster_.core_ptr(0)->snapshot_index(), 4);

    cluster_.core_ptr(2)->advance_last_applied(last_idx);
    ASSERT_TRUE(cluster_.core_ptr(2)->truncate_log(last_idx).ok());
    ASSERT_GT(cluster_.core_ptr(2)->snapshot_index(),
              cluster_.core_ptr(0)->snapshot_index());

    set_node_next_index(0, cluster_.replica_id(2), 3);

    // 心跳驱动复制路径产出 INSTALL_SNAPSHOT（同 InstallSnapshotSuccessUsesSentSnapshotIndex）。
    tick_heartbeat(0);

    bool found_snapshot = false;
    RaftMessage snapshot_msg;
    for (const auto& msg : cluster_.outbox_of(0)) {
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
    res.term = cluster_.core_ptr(0)->current_term();
    res.status = Status::ALREADY_EXIST("follower already has newer snapshot");
    res.snapshot_watermark = cluster_.core_ptr(2)->snapshot_index();

    RaftEffects response_effects;
    cluster_.core_ptr(0)->handle_install_snapshot_response(
        snapshot_msg.target.replica_id, snapshot_msg.snapshot_param, res,
        response_effects);

    EXPECT_EQ(get_node_next_index(0, snapshot_msg.target.replica_id),
              cluster_.core_ptr(2)->snapshot_index() + 1);
    EXPECT_EQ(get_snapshot_watermark(0, snapshot_msg.target.replica_id),
              cluster_.core_ptr(2)->snapshot_index());
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
    ASSERT_EQ(cluster_.core_ptr(0)->commit_index(), idx);
    ASSERT_LT(cluster_.core_ptr(1)->last_log_index(), idx);
    drop_all_messages();

    // 恢复 B，让真实存在且日志落后的 B 发起选举。A 会因为更高 term 退为
    // follower，但会因为 B 的日志不够新而拒绝投票，所以 B 不会当选。
    cluster_.restore(1);
    assert_not_elected_after_votes(1, {0});
    ASSERT_EQ(cluster_.core_ptr(0)->current_term(), 2);

    // 再次隔离 B，保证 A 重新当选和提交当前 term no-op 的过程只和 C 通信，
    // B 不会被顺便追平。
    cluster_.isolate(1);
    elect_with_votes(0, {2});
    ASSERT_EQ(cluster_.leader_idx(), 0);
    drop_all_messages();

    // 提交 A 的 no-op，确保 has_committed_current_term_entry 通过
    replicate_until_committed(0, cluster_.core_ptr(0)->last_log_index(), {2});

    // 恢复 B。B 仍然缺少 index=idx 的日志；等它收到 A 的 ReadIndex 探测时，
    // 会先被 AppendEntries 的 term 推进到 read_term，再因为 prev_log 不存在而
    // reject。
    cluster_.restore(1);
    ASSERT_LT(cluster_.core_ptr(1)->last_log_index(), idx);

    // 隔离 C，quorum 只能靠 B
    cluster_.isolate(2);

    RaftEffects read_effects;
    LogIndex read_index = 0;
    Term read_term = 0;
    std::vector<ReplicaID> voters;
    Status status = cluster_.core_ptr(0)->build_read_probe(
        read_effects, read_index, read_term, voters, /*read_probe_seq=*/1);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const LogIndex next_before = get_node_next_index(0, cluster_.replica_id(1));

    int success_cnt = 1;
    int limit = cluster_.core_ptr(0)->quorum_size();

    for (const RaftMessage& msg : read_effects.messages) {
        int target = cluster_.find_node(msg.target.replica_id);
        ASSERT_GE(target, 0);
        if (cluster_.is_isolate(target)) continue;

        ASSERT_EQ(target, 1);
        ASSERT_EQ(msg.type, RaftMessageType::APPEND_ENTRIES);
        ASSERT_TRUE(msg.append_param.read_probe_seq != 0);
        EXPECT_GT(msg.append_param.prev_log_index,
                  cluster_.core_ptr(target)->last_log_index());
        AppendEntriesResult res;
        RaftEffects effects;
        cluster_.core_ptr(target)->handle_append_entries(msg.append_param, res,
                                                         effects);
        Status drive_status = cluster_.drive_raft_effects(target, effects);
        ASSERT_TRUE(drive_status.ok()) << drive_status.to_string();

        // B 缺少 prev_log_index 对应日志，所以 reject。
        EXPECT_FALSE(res.success);
        // 但 term 匹配 read_term：F15，reject 但任期匹配的响应仍是任期确认证据。
        EXPECT_EQ(res.term, read_term);
        if (res.term == read_term) success_cnt++;
    }

    EXPECT_GE(success_cnt, limit);
    // 探测响应只计票，绝不推进复制进度（不经过 handle_append_response）。
    EXPECT_EQ(get_node_next_index(0, cluster_.replica_id(1)), next_before);
}

// 不变量2：本地快照完成只推进压缩边界，绝不把快照捕获时的旧成员配置装回 Raft。
//
// 已由 install_local_snapshot（本地路径）与 install_snapshot（远端路径）
// 显式分家落地：本地路径不调 membership_.update_raft_members，远端路径才
// 装 snapshot_members。本测试是回归保护——若未来有人把两条路径重新合并，
// 这里会红。
TEST_F(RaftClusterTest, LocalSnapshotFinishDoesNotRollbackMembership) {
    create_and_set_0_leader(3);
    ASSERT_TRUE(tick_until_all_committed(1));
    ASSERT_EQ(cluster_.leader_idx(), 0);
    drop_all_messages();

    // leader 提交 3 条 KV 日志并 apply，为本地快照做准备
    LogIndex last_idx = 0;
    for (int i = 0; i < 3; ++i) {
        last_idx = propose_and_replicate_once(0, {1, 2}, WriteOpType::PUT,
                                              "snap_member_" + std::to_string(i),
                                              "v" + std::to_string(i));
    }
    cluster_.core_ptr(0)->advance_last_applied(last_idx);
    cluster_.core_ptr(1)->advance_last_applied(last_idx);
    cluster_.core_ptr(2)->advance_last_applied(last_idx);
    drop_all_messages();

    // 捕获快照：索引 last_idx、成员配置 = 旧三 voter（不含新成员）
    const std::vector<RaftMember> members_at_capture =
        cluster_.core_ptr(0)->raft_members();
    InstallSnapshotContext local_context;
    local_context.snapshot_index = last_idx;
    local_context.snapshot_term =
        cluster_.core_ptr(0)->last_log_term();
    local_context.snapshot_members = members_at_capture;

    // 快照写盘期间，apply 了索引 last_idx+1 的 ADD_LEARNER 成员变更
    PeerMember new_member;
    new_member.node_id = "node_3";
    new_member.replica_id =
        ReplicaID{cluster_.replica_id(0).table_id, 0, 3};
    new_member.endpoint = {"127.0.0.1", 9003};
    LogEntry config_entry{};
    config_entry.term = cluster_.core_ptr(0)->current_term();
    config_entry.index = last_idx + 1;
    config_entry.op_type = WriteOpType::ADD_LEARNER;
    config_entry.config_member = new_member;
    LogEntry config_entry_copy = config_entry;
    cluster_.core_ptr(0)->update_log_entries({config_entry_copy});
    cluster_.core_ptr(0)->advance_last_applied(last_idx + 1);
    ASSERT_TRUE(cluster_.core_ptr(0)->apply_config_entry(config_entry_copy).ok());
    ASSERT_EQ(cluster_.core_ptr(0)->member_type(new_member.replica_id),
              RaftMemberType::LEARNER);

    // 本地快照完成：当前实现会调 install_snapshot 装回旧成员配置
    cluster_.core_ptr(0)->install_local_snapshot(local_context);

    // 期望：成员配置保持捕获之后的最新值（新 learner 仍在），不被快照回滚
    EXPECT_EQ(cluster_.core_ptr(0)->member_type(new_member.replica_id),
              RaftMemberType::LEARNER)
        << "local snapshot finish must not rollback membership";
    EXPECT_EQ(cluster_.core_ptr(0)->snapshot_index(), last_idx);
    EXPECT_EQ(cluster_.core_ptr(0)->last_applied(), last_idx + 1);
}

// ========== 批次 0：快照发送"未送达"必须清 inflight（F2） ==========
//
// 不变量：INSTALL_SNAPSHOT 一旦构造就打了 mark_snapshot_inflight，唯一清除
// 途径是发送回执或 send-failed 事件。消息被丢（dispatch 拒绝、RPC 失败）
// 时若不清标记，该 follower 的快照追赶永久停滞：下一次 tick 的
// build_append_entries_message 会因 inflight>0 返回 nullopt，永远不再发。
//
// 场景（协议层确定性构造，不依赖真实 RPC）：
//   1. 3 节点，node0 当选 leader；
//   2. node1 落后到快照边界之下，leader 本地打快照（truncate_log 造出
//      snapshot_index），next_index[node1] 压到 1；
//   3. leader tick 产出 INSTALL_SNAPSHOT → inflight 标记置位（断言非零）；
//   4. 发送"失败"——模拟 dispatcher 丢失/网络不可达：直接调用
//      handle_install_snapshot_send_failed（dispatcher 的 send_task 失败
//      回调最终走的就是它；async_send_one 的 submit 拒绝路径也回投同一
//      事件）；
//   5. 断言 inflight 归零，且下一次 tick 能重新产出 INSTALL_SNAPSHOT
//      （快照追赶恢复，而不是永久卡死）。
TEST_F(RaftClusterTest, SnapshotSendFailedClearsInflightAndRecovers) {
    create_and_set_0_leader(3);
    ASSERT_TRUE(tick_until_all_committed(1));
    ASSERT_EQ(cluster_.leader_idx(), 0);
    drop_all_messages();

    // leader 与 node2 提交 4 条日志并 apply；node1 保持落后
    LogIndex last_idx = 0;
    for (int i = 0; i < 4; ++i) {
        last_idx = propose_and_replicate_once(0, {2}, WriteOpType::PUT,
                                              "orphan_k" + std::to_string(i),
                                              "v" + std::to_string(i));
    }
    ASSERT_EQ(cluster_.core_ptr(0)->commit_index(), last_idx);
    cluster_.core_ptr(0)->advance_last_applied(last_idx);
    cluster_.core_ptr(2)->advance_last_applied(last_idx);

    // leader 本地打快照：snapshot_index = 2（快照边界之下只剩 entry 3、4）
    ASSERT_TRUE(cluster_.core_ptr(0)->truncate_log(2).ok());
    ASSERT_EQ(cluster_.core_ptr(0)->snapshot_index(), 2);
    drop_all_messages();

    // node1 的 next_index 压到 1：prev(0) < snapshot_index(2)，
    // 下一次复制构造必然走 INSTALL_SNAPSHOT 分支
    set_node_next_index(0, cluster_.replica_id(1), 1);

    // 立即触发一次心跳广播（心跳也走 broadcast_append_entries →
    // build_append_entries_message），产出 INSTALL_SNAPSHOT，inflight 置位
    tick_heartbeat(0);
    const ReplicaID node1_id = cluster_.replica_id(1);
    ASSERT_EQ(get_inflight_snapshot_index(0, node1_id), 2)
        << "heartbeat must mark snapshot inflight for lagging follower";

    // 提取被丢弃的 INSTALL_SNAPSHOT（不投递，模拟消息在 dispatcher 层丢失）
    const int dropped = cluster_.drop_messages_from(0);
    EXPECT_GT(dropped, 0) << "expected INSTALL_SNAPSHOT in outbox";

    // 发送失败回执：dispatcher send_task 失败 / async_send_one submit 拒绝
    // 都回投同一事件，loop 最终调用 handle_install_snapshot_send_failed
    {
        InstallSnapshotParam sent_param;
        sent_param.from_replica_id = cluster_.replica_id(0);
        sent_param.to_replica_id = node1_id;
        sent_param.term = cluster_.core_ptr(0)->current_term();
        sent_param.snapshot_index = 2;
        sent_param.snapshot_term = cluster_.core_ptr(0)->snapshot_term();
        RaftEffects effects;
        cluster_.core_ptr(0)->handle_install_snapshot_send_failed(
            node1_id, sent_param, Status::ERROR("rpc submit rejected"));
        ASSERT_TRUE(cluster_.drive_raft_effects(0, std::move(effects)).ok());
    }

    // 断言1：inflight 已清零
    EXPECT_EQ(get_inflight_snapshot_index(0, node1_id), 0)
        << "send-failed event must clear snapshot inflight";

    // 断言2：下一次心跳能重新产出 INSTALL_SNAPSHOT（追赶恢复）
    drop_all_messages();
    tick_heartbeat(0);
    bool found_retry = false;
    for (const RaftMessage& msg : cluster_.outbox_of(0)) {
        if (msg.type == RaftMessageType::INSTALL_SNAPSHOT &&
            msg.target.replica_id == node1_id) {
            found_retry = true;
            break;
        }
    }
    EXPECT_TRUE(found_retry)
        << "after send-failed clears inflight, next heartbeat must rebuild "
           "INSTALL_SNAPSHOT (orphan marker would block it forever)";
}

// ========== 方案 E 批次 D（§3-D）：apply 收敛与发布的机械层 ==========

// D-1 有界提取（治 F4）：单次提取受 budget_entries 约束，不整体拷贝
// backlog；has_more 精确指示剩余；逐轮提取最终消化全部日志；config entry
// 不被 KV 预算阻断（跟随所在批次顺序放行）。
// 确定性：协议层直接构造 3 节点集群与 100 条 backlog，逐轮预算提取并
// 断言每轮条数与 has_more 语义——无并发窗口。
TEST_F(RaftClusterTest, BoundedExtractRespectsEntryBudgetAndConfigNotBlocked) {
    create_and_set_0_leader(3);
    ASSERT_TRUE(tick_until_all_committed(1));
    ASSERT_EQ(cluster_.leader_idx(), 0);
    drop_all_messages();

    // 6 条 KV backlog（不驱动复制，commit 停在本地 propose？——单节点
    // quorum 会立即 commit；这里用 propose_and_replicate_once 逐条提交
    // 后统一只让 node0 保留未 apply 的 backlog）。
    for (int i = 0; i < 6; ++i) {
        propose_and_replicate_once(0, {1, 2}, WriteOpType::PUT,
                                   "budget_k" + std::to_string(i), "v");
    }
    const LogIndex last_idx = cluster_.core_ptr(0)->commit_index();
    ASSERT_EQ(last_idx, 7);  // 1 noop + 6 kv

    // budget=4：首轮最多 4 条（NONE/noop 与 KV 都占条数预算；config 才免费）。
    bool has_more = false;
    std::vector<LogEntry> first =
        cluster_.core_ptr(0)->extract_committed_entries(4, 0, &has_more);
    ASSERT_EQ(first.size(), 4u);
    ASSERT_TRUE(has_more) << "6 kv backlog must have more after budget=4";

    // 应用首轮（推进 last_applied），再提下一轮：剩余 3 条。
    for (const LogEntry& e : first) {
        cluster_.core_ptr(0)->advance_last_applied(e.index);
    }
    std::vector<LogEntry> second =
        cluster_.core_ptr(0)->extract_committed_entries(4, 0, &has_more);
    ASSERT_EQ(second.size(), 3u);
    ASSERT_FALSE(has_more) << "backlog exhausted, has_more must be false";

    // 提取序 = 日志序。
    LogIndex expect_index = first.front().index;
    for (const LogEntry& e : first) {
        ASSERT_EQ(e.index, expect_index++);
    }
    for (const LogEntry& e : second) {
        ASSERT_EQ(e.index, expect_index++);
    }

    // config entry 不被 KV 预算阻断：ensure_add_learner（config）紧随 KV
    // backlog，budget=0（拦一切 KV）时 config 仍按日志序放行。
    // 先把现有 backlog 全部应用掉，得到干净起点。
    while (true) {
        bool more = false;
        std::vector<LogEntry> drain =
            cluster_.core_ptr(0)->extract_committed_entries(128, 0, &more);
        if (drain.empty()) break;
        for (const LogEntry& e : drain) {
            cluster_.core_ptr(0)->advance_last_applied(e.index);
        }
        if (!more) break;
    }

    drop_all_messages();
    propose_and_replicate_once(0, {1, 2}, WriteOpType::PUT, "after_cfg_k", "v");
    PeerMember cfg_member;
    cfg_member.node_id = "cfg_learner";
    cfg_member.replica_id = ReplicaID{cluster_.replica_id(0).table_id, 0, 9};
    cfg_member.endpoint = {"127.0.0.1", 9009};
    RaftEffects cfg_effects;
    Status cfg_status = cluster_.core_ptr(0)->ensure_add_learner(cfg_member, cfg_effects);
    ASSERT_TRUE(cfg_status.ok()) << cfg_status.to_string();
    ASSERT_TRUE(cluster_.drive_raft_effects(0, std::move(cfg_effects)).ok());
    replicate_until_committed(0, cluster_.core_ptr(0)->last_log_index(), {1, 2});
    has_more = false;
    std::vector<LogEntry> cfg_batch =
        cluster_.core_ptr(0)->extract_committed_entries(0, 0, &has_more);
    ASSERT_EQ(cfg_batch.size(), 1u) << "budget=0 blocks the KV, config flows";
    ASSERT_EQ(cfg_batch.back().op_type, WriteOpType::ADD_LEARNER)
        << "config entry must not be blocked by KV budget";
}

// D-1 超大单条放行：budget_bytes 拦不住"空 KV 批起步的大 entry"，进度不死
// 锁，记指标。
TEST_F(RaftClusterTest, BoundedExtractPassesOversizedSingleAndConfigEntries) {
    create_and_set_0_leader(3);
    ASSERT_TRUE(tick_until_all_committed(1));
    drop_all_messages();

    // 应用掉当选 no-op，得到干净起点（last_applied == commit）。
    cluster_.core_ptr(0)->advance_last_applied(cluster_.core_ptr(0)->commit_index());

    // 大 value 单条（2000 字节）。
    const std::string big_value(2000, 'x');
    propose_and_replicate_once(0, {1, 2}, WriteOpType::PUT, "big_k", big_value);

    // budget_bytes=100 拦不住"KV 批内第一条"（空 KV 批起步放行，防超大单条
    // 饿死进度）。
    bool has_more = false;
    std::vector<LogEntry> first =
        cluster_.core_ptr(0)->extract_committed_entries(10, 100, &has_more);
    ASSERT_EQ(first.size(), 1u);
    ASSERT_EQ(first.front().key, "big_k");
    ASSERT_EQ(first.front().value, big_value);
    ASSERT_FALSE(has_more);
}

// ========== 方案 E 批次 C 补充（§8.2）：读索引恰在快照边界 ==========

// 不变量：commit == snapshot_index（WAL 已全部压缩）时，纯探测仍必须可构造
// 且合法——prev_log_index/prev_log_term 取压缩边界（snapshot_index /
// snapshot_term），探测消息不产生 INSTALL_SNAPSHOT、不触 inflight；follower
// 收到后按协议应答，读流程可完成。
//
// 确定性：协议层直构。leader 打快照把 WAL 压到 commit 位置（last_log_index
// == snapshot_index），再构造探测断言四元组取值与零副作用。
TEST_F(RaftClusterTest, ReadProbeAtSnapshotBoundaryUsesCompactionTerm) {
    create_and_set_0_leader(3);
    ASSERT_TRUE(tick_until_all_committed(1));
    drop_all_messages();

    // 复制两条 KV 并让 followers 确认，leader commit 到 3。
    for (int i = 0; i < 2; ++i) {
        propose_and_replicate_once(0, {1, 2}, WriteOpType::PUT,
                                   "boundary_k" + std::to_string(i), "v");
    }
    const LogIndex committed = cluster_.core_ptr(0)->commit_index();
    ASSERT_EQ(committed, 3);  // noop + 2 kv

    // leader 打快照到 commit 位置：先 apply 再 truncate（快照边界不能超过
    // last_applied），WAL 全部压缩，last_log_index == snapshot_index。
    cluster_.core_ptr(0)->advance_last_applied(committed);
    ASSERT_TRUE(cluster_.core_ptr(0)->truncate_log(committed).ok());
    const auto [snap_index, snap_term] = cluster_.core_ptr(0)->snapshot_for_test();
    ASSERT_EQ(snap_index, committed);
    ASSERT_EQ(cluster_.core_ptr(0)->last_log_index(), snap_index)
        << "WAL fully compacted: read_index == snapshot_index boundary";

    // 读探测在此边界上构造：prev 取压缩边界值，零 INSTALL_SNAPSHOT / inflight。
    RaftEffects read_effects;
    LogIndex read_index = 0;
    Term read_term = 0;
    std::vector<ReplicaID> voters;
    Status status = cluster_.core_ptr(0)->build_read_probe(
        read_effects, read_index, read_term, voters, /*read_probe_seq=*/1);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(read_index, snap_index);
    ASSERT_EQ(voters.size(), 3u);  // self + 2 followers，learner 不存在

    int probe_count = 0;
    for (const RaftMessage& msg : read_effects.messages) {
        ASSERT_EQ(msg.type, RaftMessageType::APPEND_ENTRIES)
            << "pure probe must never emit INSTALL_SNAPSHOT (F3)";
        ASSERT_TRUE(msg.append_param.read_probe_seq != 0);
        // §8.2：压缩后 prev 可给出的值就是边界值。
        EXPECT_EQ(msg.append_param.prev_log_index, snap_index);
        EXPECT_EQ(msg.append_param.prev_log_term, snap_term);
        EXPECT_TRUE(msg.append_param.entries.empty());
        ++probe_count;
    }
    ASSERT_EQ(probe_count, 2) << "probe must cover every voter except self";

    // 零 inflight：探测前后 leader 对任何 peer 都没有快照发送标记（F2）。
    for (int node = 1; node < 3; ++node) {
        EXPECT_EQ(cluster_.core_ptr(0)->inflight_snapshot_index_for_test(
                      cluster_.replica_id(node)),
                  0);
    }

    // follower 收到边界探测：以自身 term 应答（真实链路里该回执计票）。
    for (const RaftMessage& msg : read_effects.messages) {
        int target = cluster_.find_node(msg.target.replica_id);
        ASSERT_GE(target, 0);
        AppendEntriesResult res;
        RaftEffects follower_effects;
        cluster_.core_ptr(target)->handle_append_entries(msg.append_param, res,
                                                         follower_effects);
        ASSERT_TRUE(cluster_.drive_raft_effects(target, std::move(follower_effects)).ok());
        EXPECT_GT(res.term, 0) << "follower must answer the boundary probe with its term";
    }
}

}  // namespace adviskv::storage
