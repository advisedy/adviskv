#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

#include "common/define.h"
#include "common/metrics/latency_recorder.h"
#include "common/status.h"

namespace adviskv {

struct MetricsOptions {
    bool http_enable{false};
    std::string http_host{"127.0.0.1"};
    int32_t http_port{0};
    std::string http_path{"/metrics"};
};

class AdvisMetrics {
public:
    static AdvisMetrics& get_instance();

    Status init(const MetricsOptions& options);
    void stop();
    void record_latency(const std::string& name, int64_t latency_us);
    void record_counter(const std::string& name, int64_t delta = 1);
    void reset();
    std::string dump_prometheus() const;

private:
    class Registry;
    class HttpServer;

    AdvisMetrics();
    ~AdvisMetrics();

    DISALLOW_COPY_AND_ASSIGN(AdvisMetrics)

    bool initialized_{false};
    std::unique_ptr<Registry> registry_;
    std::unique_ptr<HttpServer> http_server_;
};

class ScopedMetricsTimer {
public:
    explicit ScopedMetricsTimer(std::string name);
    ~ScopedMetricsTimer();

    DISALLOW_COPY_AND_ASSIGN(ScopedMetricsTimer)

private:
    std::string name_;
    std::chrono::steady_clock::time_point start_;
};

}  // namespace adviskv

#define ADVISKV_CONCAT_INNER(x, y) x##y
#define ADVISKV_CONCAT(x, y) ADVISKV_CONCAT_INNER(x, y)
#define ADVISKV_METRICS_COUNTER(...) ::adviskv::AdvisMetrics::get_instance().record_counter(__VA_ARGS__)
#define ADVISKV_METRICS_TIMER(name) ::adviskv::ScopedMetricsTimer ADVISKV_CONCAT(adviskv_metrics_timer_, __LINE__)(name)
