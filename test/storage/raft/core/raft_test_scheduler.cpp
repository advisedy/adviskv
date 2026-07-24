#include "test/storage/raft/core/raft_test_scheduler.h"

#include <sstream>
#include <stdexcept>

#include <fmt/format.h>

#include "test/storage/raft/core/raft_test_harness.h"

namespace adviskv::storage::test {

RaftTestScheduler::RaftTestScheduler(uint64_t seed) : seed_(seed), state_(seed) {}

uint64_t RaftTestScheduler::seed() const { return seed_; }

const RaftTestScheduleStats& RaftTestScheduler::stats() const { return stats_; }

uint64_t RaftTestScheduler::next_random() {
    state_ += 0x9e3779b97f4a7c15ULL;
    uint64_t value = state_;
    value = (value ^ (value >> 30U)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27U)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31U);
}

size_t RaftTestScheduler::choose(size_t upper_bound) {
    if (upper_bound == 0) {
        throw std::invalid_argument("seeded raft scheduler cannot choose from an empty range");
    }
    return static_cast<size_t>(next_random() % upper_bound);
}

bool RaftTestScheduler::one_in(uint32_t denominator) { return denominator > 0 && choose(denominator) == 0; }

bool RaftTestScheduler::step(RaftTestCluster& cluster, const RaftTestSchedulePolicy& policy) {
    if (cluster.pending().empty()) return false;

    const TestMessageId selected = cluster.pending()[choose(cluster.pending().size())].id;
    if (one_in(policy.drop_one_in)) {
        if (!cluster.drop(selected)) {
            throw std::runtime_error(fmt::format("seeded raft scheduler lost pending message {}", selected));
        }
        ++stats_.dropped;
        record(fmt::format("step={} drop #{}", stats_.steps, selected));
    } else {
        if (one_in(policy.duplicate_one_in)) {
            const TestMessageId duplicate_id = cluster.duplicate(selected);
            ++stats_.duplicated;
            record(fmt::format("step={} duplicate #{} as #{}", stats_.steps, selected, duplicate_id));
        }

        Status status = cluster.deliver(selected);
        if (status.code() == StatusCode::INVALID_ARGUMENT) {
            throw std::runtime_error(fmt::format("seeded raft scheduler failed to deliver message {}: {}", selected,
                                                 status.to_string()));
        }
        ++stats_.delivered;
        record(fmt::format("step={} deliver #{} status={}", stats_.steps, selected, static_cast<int>(status.code())));
    }
    ++stats_.steps;
    return true;
}

void RaftTestScheduler::drain(RaftTestCluster& cluster, const RaftTestSchedulePolicy& policy, size_t max_steps) {
    const size_t first_step = stats_.steps;
    while (!cluster.pending().empty()) {
        if (stats_.steps - first_step >= max_steps) {
            throw std::runtime_error(
                    fmt::format("seeded raft scheduler exceeded {} steps, seed={}\n{}\ncluster trace:\n{}", max_steps,
                                seed_, trace(), cluster.trace()));
        }
        step(cluster, policy);
    }
}

std::string RaftTestScheduler::trace() const {
    std::ostringstream out;
    out << "seed=" << seed_ << " steps=" << stats_.steps << " delivered=" << stats_.delivered
        << " dropped=" << stats_.dropped << " duplicated=" << stats_.duplicated << '\n';
    for (const std::string& event : trace_)
        out << event << '\n';
    return out.str();
}

void RaftTestScheduler::record(std::string event) { trace_.push_back(std::move(event)); }

}  // namespace adviskv::storage::test
