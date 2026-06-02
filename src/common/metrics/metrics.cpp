#include "common/metrics/metrics.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>
#include <sstream>
#include <thread>
#include <utility>
#include <vector>

namespace adviskv {
namespace {

struct HistogramSnapshot {
    std::vector<std::pair<int64_t, int64_t>> buckets;
    int64_t count{0};
    int64_t sum_us{0};
};

struct MetricsSnapshot {
    std::unordered_map<std::string, HistogramSnapshot> latencies;
    std::unordered_map<std::string, int64_t> counters;
};

const std::vector<int64_t>& latency_buckets_us() {
    static const std::vector<int64_t> buckets = {
        100,    250,    500,    1000,   2000,   5000,  10000,
        20000,  50000,  100000, 250000, 500000, 1000000,
        std::numeric_limits<int64_t>::max(),
    };
    return buckets;
}

std::string http_response(const std::string& status,
                          const std::string& content_type,
                          const std::string& body) {
    std::ostringstream out;
    out << "HTTP/1.1 " << status << "\r\n"
        << "Content-Type: " << content_type << "\r\n"
        << "Content-Length: " << body.size() << "\r\n"
        << "Connection: close\r\n\r\n"
        << body;
    return out.str();
}

bool request_matches_path(const std::string& request, const std::string& path) {
    const std::string prefix = "GET " + path + " ";
    return request.rfind(prefix, 0) == 0;
}

void close_fd(int* fd) {
    if (*fd >= 0) {
        ::close(*fd);
        *fd = -1;
    }
}

std::string prometheus_bucket_label(int64_t upper_bound) {
    if (upper_bound == std::numeric_limits<int64_t>::max()) {
        return "+Inf";
    }
    return std::to_string(upper_bound);
}

}  // namespace

class AdvisMetrics::Registry {
   public:
    Registry() : bucket_bounds_(latency_buckets_us()) {}

    void record_latency(const std::string& name, int64_t latency_us) {
        std::lock_guard lock(mutex_);
        Histogram& histogram = latencies_[name];
        if (histogram.bucket_counts.empty()) {
            histogram.bucket_counts.assign(bucket_bounds_.size(), 0);
        }
        histogram.count += 1;
        histogram.sum_us += latency_us;
        for (size_t i = 0; i < bucket_bounds_.size(); ++i) {
            if (latency_us <= bucket_bounds_[i]) {
                histogram.bucket_counts[i] += 1;
                break;
            }
        }
    }

    void record_counter(const std::string& name, int64_t delta) {
        std::lock_guard lock(mutex_);
        counters_[name] += delta;
    }

    void reset() {
        std::lock_guard lock(mutex_);
        latencies_.clear();
        counters_.clear();
    }

    MetricsSnapshot snapshot() const {
        std::lock_guard lock(mutex_);
        MetricsSnapshot snapshot;
        for (const auto& [name, histogram] : latencies_) {
            HistogramSnapshot histogram_snapshot;
            histogram_snapshot.count = histogram.count;
            histogram_snapshot.sum_us = histogram.sum_us;
            histogram_snapshot.buckets.reserve(bucket_bounds_.size());

            int64_t cumulative = 0;
            for (size_t i = 0; i < bucket_bounds_.size(); ++i) {
                int64_t count = 0;
                if (i < histogram.bucket_counts.size()) {
                    count = histogram.bucket_counts[i];
                }
                cumulative += count;
                histogram_snapshot.buckets.emplace_back(bucket_bounds_[i],
                                                        cumulative);
            }
            snapshot.latencies[name] = std::move(histogram_snapshot);
        }
        snapshot.counters = counters_;
        return snapshot;
    }

   private:
    struct Histogram {
        std::vector<int64_t> bucket_counts;
        int64_t count{0};
        int64_t sum_us{0};
    };

    const std::vector<int64_t>& bucket_bounds_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, Histogram> latencies_;
    std::unordered_map<std::string, int64_t> counters_;
};

class AdvisMetrics::HttpServer {
   public:
    explicit HttpServer(MetricsOptions options)
        : options_(std::move(options)) {}

    Status start() {
        if (options_.http_port <= 0) {
            return Status::INVALID_ARGUMENT("metrics http port invalid");
        }

        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ < 0) {
            return Status::ERROR("failed to create metrics http socket");
        }

        int reuse = 1;
        IGNORE_RESULT(::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse,
                                   sizeof(reuse)))

        sockaddr_in addr{};
#ifdef __APPLE__
        addr.sin_len = sizeof(addr);
#endif
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(options_.http_port));
        if (::inet_pton(AF_INET, options_.http_host.c_str(), &addr.sin_addr) !=
            1) {
            close_fd(&listen_fd_);
            return Status::INVALID_ARGUMENT("metrics http host invalid");
        }

        if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr),
                   sizeof(addr)) != 0) {
            std::string msg =
                std::string("failed to bind metrics http socket: ") +
                std::strerror(errno);
            close_fd(&listen_fd_);
            return Status::ERROR(msg);
        }

        if (::listen(listen_fd_, 16) != 0) {
            close_fd(&listen_fd_);
            return Status::ERROR("failed to listen metrics http socket");
        }

        running_.store(true);
        thread_ = std::thread([this]() { serve_loop(); });
        return Status::OK();
    }

    void stop() {
        running_.store(false);
        if (listen_fd_ >= 0) {
            IGNORE_RESULT(::shutdown(listen_fd_, SHUT_RDWR))
            close_fd(&listen_fd_);
        }
        if (thread_.joinable()) {
            thread_.join();
        }
    }

   private:
    void serve_loop() {
        while (running_.load()) {
            sockaddr_in client_addr{};
            socklen_t client_len = sizeof(client_addr);
            int client_fd =
                ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&client_addr),
                         &client_len);
            if (client_fd < 0) {
                continue;
            }
            handle_client(client_fd);
            ::close(client_fd);
        }
    }

    void handle_client(int client_fd) const {
        char buffer[4096];
        const ssize_t n = ::recv(client_fd, buffer, sizeof(buffer), 0);
        if (n <= 0) {
            return;
        }

        const std::string request(buffer, static_cast<size_t>(n));
        const std::string response = build_response(request);
        const char* data = response.data();
        size_t remain = response.size();
        while (remain > 0) {
            const ssize_t sent = ::send(client_fd, data, remain, 0);
            if (sent <= 0) {
                return;
            }
            data += sent;
            remain -= static_cast<size_t>(sent);
        }
    }

    std::string build_response(const std::string& request) const {
        if (request_matches_path(request, options_.http_path)) {
            return http_response("200 OK",
                                 "text/plain; version=0.0.4; charset=utf-8",
                                 AdvisMetrics::get_instance().dump_prometheus());
        }
        return http_response("404 Not Found", "text/plain; charset=utf-8",
                             "not found\n");
    }

    MetricsOptions options_;
    std::atomic<bool> running_{false};
    std::thread thread_;
    int listen_fd_{-1};
};

AdvisMetrics& AdvisMetrics::get_instance() {
    static AdvisMetrics metrics;
    return metrics;
}

AdvisMetrics::AdvisMetrics() : registry_(std::make_unique<Registry>()) {}

AdvisMetrics::~AdvisMetrics() { stop(); }

Status AdvisMetrics::init(const MetricsOptions& options) {
    if (initialized_) {
        return Status::OK();
    }

    if (options.http_enable) {
        http_server_ = std::make_unique<HttpServer>(options);
        Status status = http_server_->start();
        if (status.fail()) {
            http_server_.reset();
            return status;
        }
    }

    initialized_ = true;
    return Status::OK();
}

void AdvisMetrics::stop() {
    if (http_server_) {
        http_server_->stop();
        http_server_.reset();
    }
    initialized_ = false;
}

void AdvisMetrics::record_latency(const std::string& name, int64_t latency_us) {
    registry_->record_latency(name, latency_us);
}

void AdvisMetrics::record_counter(const std::string& name, int64_t delta) {
    registry_->record_counter(name, delta);
}

void AdvisMetrics::reset() { registry_->reset(); }

std::string AdvisMetrics::dump_prometheus() const {
    const MetricsSnapshot snapshot = registry_->snapshot();

    std::vector<std::string> latency_names;
    latency_names.reserve(snapshot.latencies.size());
    for (const auto& [name, _] : snapshot.latencies) {
        latency_names.push_back(name);
    }
    std::sort(latency_names.begin(), latency_names.end());

    std::vector<std::string> counter_names;
    counter_names.reserve(snapshot.counters.size());
    for (const auto& [name, _] : snapshot.counters) {
        counter_names.push_back(name);
    }
    std::sort(counter_names.begin(), counter_names.end());

    std::ostringstream out;
    for (const std::string& raw_name : latency_names) {
        const std::string metric_name = "adviskv_" + raw_name + "_latency_us";
        const HistogramSnapshot& histogram =
            snapshot.latencies.at(raw_name);

        out << "# TYPE " << metric_name << " histogram\n";
        for (const auto& [upper_bound, cumulative_count] : histogram.buckets) {
            out << metric_name << "_bucket{le=\""
                << prometheus_bucket_label(upper_bound) << "\"} "
                << cumulative_count << "\n";
        }
        out << metric_name << "_count " << histogram.count << "\n"
            << metric_name << "_sum " << histogram.sum_us << "\n";
    }

    for (const std::string& raw_name : counter_names) {
        const std::string metric_name = "adviskv_" + raw_name + "_total";
        out << "# TYPE " << metric_name << " counter\n"
            << metric_name << " " << snapshot.counters.at(raw_name) << "\n";
    }

    return out.str();
}

ScopedMetricsTimer::ScopedMetricsTimer(std::string name)
    : name_(std::move(name)), start_(std::chrono::steady_clock::now()) {}

ScopedMetricsTimer::~ScopedMetricsTimer() {
    const auto end = std::chrono::steady_clock::now();
    const int64_t latency_us =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start_)
            .count();
    AdvisMetrics::get_instance().record_latency(name_, latency_us);
}

}  // namespace adviskv