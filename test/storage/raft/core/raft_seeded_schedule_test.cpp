#include <algorithm>
#include <charconv>
#include <cstdlib>
#include <exception>
#include <map>
#include <optional>
#include <string>
#include <system_error>
#include <vector>

#include <gtest/gtest.h>

#include "test/storage/raft/core/raft_test_harness.h"
#include "test/storage/raft/core/raft_test_scheduler.h"

namespace adviskv::storage::test {
namespace {

const LogEntry* entry_at(const RaftCore& core, LogIndex index) {
    const auto& entries = core.log_entries_for_test();
    auto it = std::find_if(entries.begin(), entries.end(),
                           [index](const LogEntry& entry) { return entry.index == index; });
    return it == entries.end() ? nullptr : &*it;
}

std::string failure_context(uint64_t seed, const RaftTestScheduler& scheduler, const RaftTestCluster& cluster) {
    return "seed=" + std::to_string(seed) + "\nscheduler trace:\n" + scheduler.trace() + "cluster trace:\n" +
           cluster.trace();
}

class RaftSafetyMonitor {
public:
    ::testing::AssertionResult check(const RaftTestCluster& cluster) {
        for (TestNodeId node = 0; node < cluster.node_count(); ++node) {
            const RaftCore& core = cluster.core(node);
            if (core.snapshot_index() > core.last_applied() || core.last_applied() > core.commit_index() ||
                core.commit_index() > core.last_log_index()) {
                return ::testing::AssertionFailure()
                       << "node " << node
                       << " violates snapshot <= applied <= commit <= last_log: " << core.snapshot_index()
                       << " <= " << core.last_applied() << " <= " << core.commit_index()
                       << " <= " << core.last_log_index();
            }

            const ObservedState current{core.current_term(), core.snapshot_index(), core.last_applied(),
                                        core.commit_index()};
            if (auto previous = previous_by_node_.find(node); previous != previous_by_node_.end()) {
                if (current.term < previous->second.term || current.snapshot_index < previous->second.snapshot_index ||
                    current.last_applied < previous->second.last_applied ||
                    current.commit_index < previous->second.commit_index) {
                    return ::testing::AssertionFailure()
                           << "node " << node << " regressed state: term " << previous->second.term << " -> "
                           << current.term << ", snapshot " << previous->second.snapshot_index << " -> "
                           << current.snapshot_index << ", applied " << previous->second.last_applied << " -> "
                           << current.last_applied << ", commit " << previous->second.commit_index << " -> "
                           << current.commit_index;
                }
            }
            previous_by_node_[node] = current;

            for (LogIndex index = core.snapshot_index() + 1; index <= core.commit_index(); ++index) {
                const LogEntry* entry = entry_at(core, index);
                if (entry == nullptr) {
                    return ::testing::AssertionFailure() << "node " << node << " is missing committed entry " << index;
                }
                auto [committed, inserted] = committed_by_index_.emplace(index, *entry);
                if (!inserted && committed->second != *entry) {
                    return ::testing::AssertionFailure()
                           << "committed entry " << index << " changed after it was first observed";
                }
            }

            if (core.role() == ReplicaRole::LEADER) {
                auto [it, inserted] = leader_by_term_.emplace(core.current_term(), node);
                if (!inserted && it->second != node) {
                    return ::testing::AssertionFailure() << "nodes " << it->second << " and " << node
                                                         << " became leaders in term " << core.current_term();
                }
            }
        }

        for (TestNodeId left = 0; left < cluster.node_count(); ++left) {
            for (TestNodeId right = left + 1; right < cluster.node_count(); ++right) {
                const RaftCore& left_core = cluster.core(left);
                const RaftCore& right_core = cluster.core(right);
                const LogIndex common_commit = std::min(left_core.commit_index(), right_core.commit_index());
                const LogIndex first_visible = std::max(left_core.snapshot_index(), right_core.snapshot_index()) + 1;
                for (LogIndex index = first_visible; index <= common_commit; ++index) {
                    const LogEntry* left_entry = entry_at(left_core, index);
                    const LogEntry* right_entry = entry_at(right_core, index);
                    if (left_entry == nullptr || right_entry == nullptr) {
                        return ::testing::AssertionFailure()
                               << "committed entry " << index << " is missing on nodes " << left << " or " << right;
                    }
                    if (*left_entry != *right_entry) {
                        return ::testing::AssertionFailure()
                               << "committed entry " << index << " differs between nodes " << left << " and " << right;
                    }
                }
            }
        }
        return ::testing::AssertionSuccess();
    }

private:
    struct ObservedState {
        Term term{0};
        LogIndex snapshot_index{0};
        LogIndex last_applied{0};
        LogIndex commit_index{0};
    };

    std::map<TestNodeId, ObservedState> previous_by_node_;
    std::map<LogIndex, LogEntry> committed_by_index_;
    std::map<Term, TestNodeId> leader_by_term_;
};

RaftTestCluster single_node_cluster_with_value(const std::string& value) {
    RaftTestCluster cluster = RaftTestCluster::voters(1, 758);
    cluster.campaign(0);
    cluster.deliver_all();
    RaftTestProposalResult proposal = cluster.propose(0, ProposeParam::write(WriteOpType::PUT, "history-key", value));
    if (proposal.status.fail()) throw std::runtime_error(proposal.status.to_string());
    cluster.deliver_all();
    cluster.apply_all();
    return cluster;
}

::testing::AssertionResult run_steps_and_check(RaftTestCluster& cluster, RaftTestScheduler& scheduler,
                                               RaftSafetyMonitor& safety, const RaftTestSchedulePolicy& policy,
                                               size_t step_limit) {
    for (size_t step = 0; step < step_limit && !cluster.pending().empty(); ++step) {
        try {
            scheduler.step(cluster, policy);
        } catch (const std::exception& error) {
            return ::testing::AssertionFailure() << error.what() << '\n'
                                                 << failure_context(scheduler.seed(), scheduler, cluster);
        }
        if (auto result = safety.check(cluster); !result) {
            return ::testing::AssertionFailure() << result.message() << '\n'
                                                 << failure_context(scheduler.seed(), scheduler, cluster);
        }
    }
    return ::testing::AssertionSuccess();
}

::testing::AssertionResult drain_and_check(RaftTestCluster& cluster, RaftTestScheduler& scheduler,
                                           RaftSafetyMonitor& safety, const RaftTestSchedulePolicy& policy,
                                           size_t max_steps = 10000) {
    size_t steps = 0;
    while (!cluster.pending().empty()) {
        if (steps++ >= max_steps) {
            return ::testing::AssertionFailure() << "schedule did not drain within " << max_steps << " steps\n"
                                                 << failure_context(scheduler.seed(), scheduler, cluster);
        }
        if (auto result = run_steps_and_check(cluster, scheduler, safety, policy, 1); !result) return result;
    }
    return ::testing::AssertionSuccess();
}

::testing::AssertionResult elect_with_loss(RaftTestCluster& cluster, RaftTestScheduler& scheduler,
                                           RaftSafetyMonitor& safety, TestNodeId candidate,
                                           const RaftTestSchedulePolicy& policy) {
    for (int attempt = 0; attempt < 32; ++attempt) {
        cluster.campaign(candidate);
        if (auto result = drain_and_check(cluster, scheduler, safety, policy); !result) return result;
        if (cluster.core(candidate).role() == ReplicaRole::LEADER) return ::testing::AssertionSuccess();
    }
    return ::testing::AssertionFailure() << "candidate " << candidate << " did not become leader\n"
                                         << failure_context(scheduler.seed(), scheduler, cluster);
}

::testing::AssertionResult advance_until_committed(RaftTestCluster& cluster, RaftTestScheduler& scheduler,
                                                   RaftSafetyMonitor& safety, TestNodeId leader,
                                                   const RaftTestSchedulePolicy& policy) {
    for (int round = 0; round < 128; ++round) {
        if (cluster.core(leader).commit_index() == cluster.core(leader).last_log_index()) {
            return ::testing::AssertionSuccess();
        }
        cluster.tick(leader);
        if (auto result = drain_and_check(cluster, scheduler, safety, policy); !result) return result;
    }
    return ::testing::AssertionFailure() << "leader " << leader << " did not commit its log\n"
                                         << failure_context(scheduler.seed(), scheduler, cluster);
}

bool converged(const RaftTestCluster& cluster, TestNodeId leader) {
    for (TestNodeId node = 0; node < cluster.node_count(); ++node) {
        if (cluster.core(node).log_entries_for_test() != cluster.core(leader).log_entries_for_test()) return false;
        if (cluster.core(node).commit_index() != cluster.core(leader).commit_index()) return false;
        if (cluster.kv(node) != cluster.kv(leader)) return false;
    }
    return true;
}

TEST(RaftSafetyMonitorTest, RejectsCommitAndAppliedRegressionAcrossChecks) {
    RaftSafetyMonitor safety;
    RaftTestCluster advanced = single_node_cluster_with_value("committed");
    ASSERT_TRUE(safety.check(advanced));

    RaftTestCluster behind = RaftTestCluster::voters(1, 758);
    behind.campaign(0);
    behind.deliver_all();
    behind.apply_all();

    const ::testing::AssertionResult result = safety.check(behind);
    EXPECT_FALSE(result);
    EXPECT_NE(std::string(result.message()).find("regressed state"), std::string::npos);
}

TEST(RaftSafetyMonitorTest, RejectsReplacementOfPreviouslyCommittedEntry) {
    RaftSafetyMonitor safety;
    RaftTestCluster first = single_node_cluster_with_value("first");
    ASSERT_TRUE(safety.check(first));

    RaftTestCluster replacement = single_node_cluster_with_value("replacement");
    const ::testing::AssertionResult result = safety.check(replacement);
    EXPECT_FALSE(result);
    EXPECT_NE(std::string(result.message()).find("changed after it was first observed"), std::string::npos);
}

::testing::AssertionResult converge_cluster(RaftTestCluster& cluster, RaftTestScheduler& scheduler,
                                            RaftSafetyMonitor& safety, TestNodeId leader) {
    const RaftTestSchedulePolicy reliable_reordering{0, 5};
    cluster.heal_all();
    for (int round = 0; round < 128; ++round) {
        cluster.tick(leader);
        if (auto result = drain_and_check(cluster, scheduler, safety, reliable_reordering); !result) return result;
        cluster.apply_all();
        if (auto result = safety.check(cluster); !result) {
            return ::testing::AssertionFailure() << result.message() << '\n'
                                                 << failure_context(scheduler.seed(), scheduler, cluster);
        }
        if (converged(cluster, leader)) return ::testing::AssertionSuccess();
    }
    return ::testing::AssertionFailure() << "cluster did not converge on leader " << leader << '\n'
                                         << failure_context(scheduler.seed(), scheduler, cluster);
}

void run_partition_scenario(uint64_t seed) {
    SCOPED_TRACE("raft schedule seed=" + std::to_string(seed));
    RaftTestCluster cluster = RaftTestCluster::voters(3, 760);
    RaftTestScheduler scheduler(seed);
    RaftSafetyMonitor safety;
    const RaftTestSchedulePolicy reordered{0, 3};
    const RaftTestSchedulePolicy lossy{5, 4};

    cluster.campaign(0);
    ASSERT_TRUE(drain_and_check(cluster, scheduler, safety, reordered));
    ASSERT_EQ(cluster.core(0).role(), ReplicaRole::LEADER) << failure_context(seed, scheduler, cluster);

    for (int operation = 0; operation < 16; ++operation) {
        RaftTestProposalResult result =
                cluster.propose(0, ProposeParam::write(WriteOpType::PUT, "before-" + std::to_string(operation),
                                                       "value-" + std::to_string(operation)));
        ASSERT_TRUE(result.status.ok()) << result.status.to_string();
        ASSERT_TRUE(run_steps_and_check(cluster, scheduler, safety, lossy, scheduler.choose(4)));
    }
    ASSERT_TRUE(drain_and_check(cluster, scheduler, safety, lossy));
    ASSERT_TRUE(advance_until_committed(cluster, scheduler, safety, 0, lossy));
    ASSERT_TRUE(converge_cluster(cluster, scheduler, safety, 0));

    cluster.isolate(0);
    ASSERT_TRUE(elect_with_loss(cluster, scheduler, safety, 1, lossy));
    ASSERT_EQ(cluster.core(1).role(), ReplicaRole::LEADER) << failure_context(seed, scheduler, cluster);

    for (int operation = 0; operation < 16; ++operation) {
        RaftTestProposalResult result =
                cluster.propose(1, ProposeParam::write(WriteOpType::PUT, "after-" + std::to_string(operation),
                                                       "value-" + std::to_string(operation)));
        ASSERT_TRUE(result.status.ok()) << result.status.to_string();
        ASSERT_TRUE(run_steps_and_check(cluster, scheduler, safety, lossy, scheduler.choose(4)));
    }
    ASSERT_TRUE(drain_and_check(cluster, scheduler, safety, lossy));
    ASSERT_TRUE(advance_until_committed(cluster, scheduler, safety, 1, lossy));
    ASSERT_TRUE(converge_cluster(cluster, scheduler, safety, 1));
    EXPECT_EQ(cluster.core(0).role(), ReplicaRole::FOLLOWER) << failure_context(seed, scheduler, cluster);
}

// 场景：创建两个完全相同的 3 节点集群，并给两个 scheduler 配置同一个 seed。
// 过程：两边同时让节点 0 campaign，再由 scheduler 独立乱序投递全部消息。
// 预期：消息轨迹、集群轨迹和各节点最终状态逐项相同，证明 seed 可复现调度。
TEST(RaftSeededSchedulerTest, SameSeedReplaysIdenticalMessageSchedule) {
    RaftTestCluster left = RaftTestCluster::voters(3, 759);
    RaftTestCluster right = RaftTestCluster::voters(3, 759);
    RaftTestScheduler left_scheduler(0x5eedULL);
    RaftTestScheduler right_scheduler(0x5eedULL);
    const RaftTestSchedulePolicy policy{0, 3};

    left.campaign(0);
    right.campaign(0);
    left_scheduler.drain(left, policy);
    right_scheduler.drain(right, policy);

    EXPECT_EQ(left_scheduler.trace(), right_scheduler.trace());
    EXPECT_EQ(left.trace(), right.trace());
    for (TestNodeId node = 0; node < left.node_count(); ++node) {
        EXPECT_EQ(left.core(node).role(), right.core(node).role());
        EXPECT_EQ(left.core(node).current_term(), right.core(node).current_term());
        EXPECT_EQ(left.core(node).log_entries_for_test(), right.core(node).log_entries_for_test());
        EXPECT_EQ(left.core(node).commit_index(), right.core(node).commit_index());
    }
}

// 场景：用固定 seed 0x5eed 初始化调度器。
// 过程：连续 8 次在 [0, 1000) 中选择随机位置。
// 预期：选择序列保持固定，使失败 seed 在代码变更后仍能按原顺序回放。
TEST(RaftSeededSchedulerTest, KnownSeedKeepsStableRandomSequence) {
    RaftTestScheduler scheduler(0x5eedULL);
    std::vector<size_t> actual;
    for (int i = 0; i < 8; ++i)
        actual.push_back(scheduler.choose(1000));

    EXPECT_EQ(actual, (std::vector<size_t>{52, 5, 891, 197, 693, 130, 368, 705}));
}

class RaftSeededNetworkTest : public ::testing::TestWithParam<uint64_t> {};

// 场景：每个固定 seed 驱动一个 3 节点集群，网络会乱序、丢包，并在中途隔离旧 leader。
// 过程：旧 leader 提交一批写入，隔离后由节点 1 当选并继续写，最后恢复网络并收敛。
// 预期：全过程无同 term 双 leader、无已提交日志分叉；恢复后所有日志、commit 和 KV 一致。
TEST_P(RaftSeededNetworkTest, LossDuplicationPartitionAndRecoveryPreserveSafety) { run_partition_scenario(GetParam()); }

INSTANTIATE_TEST_SUITE_P(FixedSeeds, RaftSeededNetworkTest,
                         ::testing::Values<uint64_t>(0, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987,
                                                     0x5eedULL, 0xdeadbeefULL, 0x123456789abcdef0ULL,
                                                     0xffffffffffffffffULL),
                         [](const ::testing::TestParamInfo<uint64_t>& info) {
                             return "Seed" + std::to_string(info.param);
                         });

// 场景：开发者通过 ADVISKV_RAFT_SCHEDULE_SEED 提供一次失败运行记录下来的 seed。
// 过程：解析环境变量并完整重跑分区、丢包、重新选举和网络恢复场景。
// 预期：合法 seed 可精确回放；未提供变量时跳过，非法值则明确报错。
TEST(RaftSeededSchedulerTest, ReplaySeedFromEnvironment) {
    const char* raw_seed = std::getenv("ADVISKV_RAFT_SCHEDULE_SEED");
    if (raw_seed == nullptr) GTEST_SKIP() << "set ADVISKV_RAFT_SCHEDULE_SEED to replay a schedule";

    uint64_t seed{0};
    const char* end = raw_seed + std::char_traits<char>::length(raw_seed);
    auto [parsed_end, error] = std::from_chars(raw_seed, end, seed);
    ASSERT_EQ(error, std::errc{}) << "invalid ADVISKV_RAFT_SCHEDULE_SEED: " << raw_seed;
    ASSERT_EQ(parsed_end, end) << "invalid ADVISKV_RAFT_SCHEDULE_SEED: " << raw_seed;
    run_partition_scenario(seed);
}

}  // namespace
}  // namespace adviskv::storage::test
