#pragma once

#include <algorithm>
#include <cstdint>
#include <mutex>
#include <utility>
#include <vector>

namespace adviskv {

class LatencyRecorder {
public:
    void record(int64_t latency_us) {
        std::lock_guard<std::mutex> lock(mutex_);
        latencies_us_.push_back(latency_us);
    }

    struct LatencySummary {
        int64_t count{0};
        int64_t sum_us{0};
        int64_t min_us{0};
        int64_t max_us{0};
        double avg_us{0.0};
        int64_t p50_us{0};
        int64_t p95_us{0};
        int64_t p99_us{0};
    };
    LatencySummary summary() const {
        std::vector<int64_t> values = snapshot();

        if (values.empty()) {
            return {};
        }

        std::sort(values.begin(), values.end());
        int64_t total_us = 0;
        for (const int64_t value : values) {
            total_us += value;
        }

        LatencySummary summary;
        summary.count = static_cast<int64_t>(values.size());
        summary.sum_us = total_us;
        summary.min_us = values.front();
        summary.max_us = values.back();
        summary.avg_us = static_cast<double>(total_us) / static_cast<double>(values.size());
        summary.p50_us = latency_percentile_us(values, 50);
        summary.p95_us = latency_percentile_us(values, 95);
        summary.p99_us = latency_percentile_us(values, 99);
        return summary;
    }

    std::vector<int64_t> snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return latencies_us_;
    }

private:
    int64_t latency_percentile_us(const std::vector<int64_t>& sorted, int32_t pct) const {
        if (sorted.empty()) {
            return 0;
        }
        const size_t index = (static_cast<size_t>(pct) * (sorted.size() - 1) + 99) / 100;
        return sorted[index];
    }

    mutable std::mutex mutex_;
    std::vector<int64_t> latencies_us_;
};

}  // namespace adviskv