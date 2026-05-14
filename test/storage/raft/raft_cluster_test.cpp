#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
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
                                                   all_members_, persist.get());

            persists_.push_back(std::move(persist));
            nodes_.push_back(std::move(node));
            active_.push_back(true);
            isolated_.push_back(false);
            id_to_idx_[all_members_[i].replica_id] = i;
        }
    }

    // 这里对于isolate的节点也会tick，其实应该分的再仔细一点
    // 区分开来是网络分区还是宕机了
    void tick_all() {
        for (int i = 0; i < static_cast<int>(nodes_.size()); i++) {
            if (!active_[i]) continue;
            nodes_[i]->tick();
        }

        route_all_messages();
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

    static LogEntry make_entry(Term term, LogIndex index, WriteOpType op_type,
                               std::string key, std::string value) {
        return LogEntry{.term = term,
                        .index = index,
                        .op_type = op_type,
                        .key = std::move(key),
                        .value = std::move(value)};
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
    void route_all_messages() {
        for (int i = 0; i < static_cast<int>(nodes_.size()); i++) {
            if (!active_[i] || isolated_[i]) continue;

            auto messages = nodes_[i]->extract_messages();
            for (auto& msg : messages) {
                int target = find_node(msg.target.replica_id);
                if (target < 0 || !active_[target] || isolated_[target])
                    continue;

                switch (msg.type) {
                    case RaftMessageType::REQUEST_VOTE: {
                        RequestVoteResult result;
                        nodes_[target]->handle_request_vote(msg.vote_param,
                                                            result);
                        nodes_[i]->handle_vote_response(msg.target.replica_id,
                                                        result);
                        break;
                    }

                    case RaftMessageType::APPEND_ENTRIES: {
                        AppendEntriesResult result;
                        nodes_[target]->handle_append_entries(msg.append_param,
                                                              result);
                        nodes_[i]->handle_append_response(msg.target.replica_id,
                                                          result);
                        break;
                    }

                    case RaftMessageType::INSTALL_SNAPSHOT: {
                        Status s = nodes_[target]->prepare_install_snapshot(
                            msg.snapshot_param.term,
                            msg.snapshot_param.snapshot_index);

                        InstallSnapshotResult result;
                        result.term = nodes_[target]->current_term();
                        result.success = s.ok();

                        if (s.ok()) {
                            nodes_[target]->install_snapshot(
                                msg.snapshot_param.snapshot_index,
                                msg.snapshot_param.snapshot_term,
                                msg.snapshot_param.term);
                        }

                        nodes_[i]->handle_install_snapshot_response(
                            msg.target.replica_id, result);
                        break;
                    }
                }
            }
        }
    }

    std::vector<std::unique_ptr<PersistEngine>> persists_;
    std::vector<std::unique_ptr<RaftNode>> nodes_;
    std::vector<PeerMember> all_members_;
    std::vector<bool> active_;  // TODO vector<bool>好像有问题来着
    std::vector<bool> isolated_;
    std::vector<fs::path> temp_dirs_;

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

    void assert_single_leader() { ASSERT_EQ(cluster_.leader_count(), 1); }

    bool tick_until_all_committed(LogIndex target, int max_rounds = 100) {
        for (int r = 0; r < max_rounds; r++) {
            cluster_.tick_all();
            if (cluster_.all_committed_at_least(target)) return true;
        }
        return false;
    }

    std::vector<LogEntry> get_node_entries(int idx) {
        RaftNode* node = cluster_.node_ptr(idx);
        if (!node) return {};
        return node->log_entries_;
    }

    RaftCluster cluster_;
    static inline int next_table_id_{
        500};  // TODO 这里是在做什么？为何要static + inline
};

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
}

// 检测进行了写操作之后，正常情况下大家都是可以commit的
TEST_F(RaftClusterTest, BasicLogReplication_3Nodes) {
    create_and_stabilize(3);
    auto* leader = cluster_.leader_ptr();
    ASSERT_NE(leader, nullptr);
    {  // 先进行一次写操作试试
        auto [status, new_idx] = leader->propose(
            WriteOpType::PUT, Key("test_key"), Value("test_value"));
        ASSERT_TRUE(status.ok());
        ASSERT_GT(new_idx, 0);

        ASSERT_TRUE(tick_until_all_committed(new_idx));
    }
    {  // 进行多次
        LogIndex last_idx{0};
        for (int i = 0; i < 1000; i++) {
            Key kv = "k" + std::to_string(i);
            Value vv = "v" + std::to_string(i);
            auto [status, new_idx] =
                leader->propose(WriteOpType::PUT, Key(kv), Value(vv));
            ASSERT_TRUE(status.ok());
            last_idx = std::max(last_idx, new_idx);
        }
        ASSERT_TRUE(tick_until_all_committed(last_idx, 2000));
    }
}

// 当leader宕机之后，选取新的leader，是否正常
TEST_F(RaftClusterTest, LeaderCrash_NewLeaderElected_NoLogLoss) {
    create_and_stabilize(3);
    auto* old_leader = cluster_.leader_ptr();
    int old_leader_idx = cluster_.leader_idx();
    ASSERT_NE(old_leader, nullptr);

    // 写入并确认提交
    auto [status, new_idx] = old_leader->propose(WriteOpType::PUT, "k1", "v1");
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

// 检测当一些节点没有收到WAL的时候，最终是能够WAL同步的
TEST_F(RaftClusterTest, test001) {
    create_and_stabilize(5);
    cluster_.isolate(3);
    cluster_.isolate(4);
    cluster_.tick_until_stable();
    RaftNode* leader = cluster_.leader_ptr();
    ASSERT_NE(leader, nullptr);
    LogIndex last_idx{0};
    for (int i = 1; i <= 100; i++) {
        Key key = "key_" + std::to_string(i);
        Value value = "valye_" + std::to_string(i);
        auto [status, new_idx] = leader->propose(WriteOpType::PUT, key, value);
        ASSERT_TRUE(status.ok());
        last_idx = std::max(last_idx, new_idx);
    }
    cluster_.restore(3);
    cluster_.restore(4);
    ASSERT_NE(cluster_.node_ptr(3), nullptr);
    ASSERT_NE(cluster_.node_ptr(4), nullptr);
    ASSERT_EQ(cluster_.node_ptr(3)->commit_index(), 0);
    ASSERT_EQ(cluster_.node_ptr(4)->commit_index(), 0);
    ASSERT_TRUE(tick_until_all_committed(last_idx));
    ASSERT_EQ(cluster_.node_ptr(3)->commit_index(), last_idx);
    ASSERT_EQ(cluster_.node_ptr(4)->commit_index(), last_idx);
}

// num: 5 然后leader:node0只发给了一个node1，然后宕机了
// 然后另外三个又好了，
// 然后这三个有一个当了leader，发送消息，把node1的log截断了。
TEST_F(RaftClusterTest, test002) {
    create_and_stabilize(5);
    RaftNode* old_leader = cluster_.leader_ptr();
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
                old_leader->propose(WriteOpType::PUT, key, value);
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
        RaftNode* new_leader = cluster_.leader_ptr();

        for (int i = 1; i <= 5; i++) {
            Key key = "new_key_" + std::to_string(i);
            Value value = "new_value_" + std::to_string(i);
            auto [status, new_idx] =
                new_leader->propose(WriteOpType::PUT, key, value);
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
    ASSERT_EQ(entries.back().key, "new_key_5");
}

}  // namespace adviskv::storage