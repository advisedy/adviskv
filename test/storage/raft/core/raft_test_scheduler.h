#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace adviskv::storage::test {

class RaftTestCluster;

struct RaftTestSchedulePolicy {
    uint32_t drop_one_in{0};
    uint32_t duplicate_one_in{0};
};

struct RaftTestScheduleStats {
    size_t steps{0};
    size_t delivered{0};
    size_t dropped{0};
    size_t duplicated{0};
};

class RaftTestScheduler {
public:
    explicit RaftTestScheduler(uint64_t seed);

    uint64_t seed() const;
    const RaftTestScheduleStats& stats() const;
    size_t choose(size_t upper_bound);
    bool one_in(uint32_t denominator);

    bool step(RaftTestCluster& cluster, const RaftTestSchedulePolicy& policy = {});
    void drain(RaftTestCluster& cluster, const RaftTestSchedulePolicy& policy = {}, size_t max_steps = 10000);
    std::string trace() const;

private:
    uint64_t next_random();
    void record(std::string event);

    uint64_t seed_{0};
    uint64_t state_{0};
    RaftTestScheduleStats stats_;
    std::vector<std::string> trace_;
};

}  // namespace adviskv::storage::test
