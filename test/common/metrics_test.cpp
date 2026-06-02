#include "common/metrics/metrics.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdint>
#include <string>

namespace adviskv {
namespace {

int find_free_port() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_GE(fd, 0);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;

    EXPECT_EQ(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);

    socklen_t len = sizeof(addr);
    // 查询 fd 当前绑定的本地地址
    EXPECT_EQ(::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len), 0);
    const int port = ntohs(addr.sin_port);
    ::close(fd);
    return port;
}

std::string http_get(const std::string& host, int port,
                     const std::string& path) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_GE(fd, 0);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    EXPECT_EQ(::inet_pton(AF_INET, host.c_str(), &addr.sin_addr), 1);

    EXPECT_EQ(::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)),
              0);

    const std::string request =
        "GET " + path + " HTTP/1.1\r\nHost: " + host + "\r\n\r\n";
    EXPECT_EQ(::send(fd, request.data(), request.size(), 0),
              static_cast<ssize_t>(request.size()));

    std::string response;
    char buf[4096];
    while (true) {
        const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        if (n <= 0) break;
        response.append(buf, static_cast<size_t>(n));
    }
    ::close(fd);
    return response;
}

TEST(AdvisMetricsTest, DumpsPrometheusTextFormat) {
    AdvisMetrics::get_instance().stop();
    AdvisMetrics::get_instance().reset();

    AdvisMetrics::get_instance().record_latency("storage_put_handler", 100);
    AdvisMetrics::get_instance().record_latency("storage_put_handler", 300);
    AdvisMetrics::get_instance().record_counter("storage_put_success", 2);

    const std::string output = AdvisMetrics::get_instance().dump_prometheus();

    EXPECT_NE(
        output.find("# TYPE adviskv_storage_put_handler_latency_us summary"),
        std::string::npos);
    EXPECT_NE(output.find("adviskv_storage_put_handler_latency_us_count 2"),
              std::string::npos);
    EXPECT_NE(output.find("adviskv_storage_put_handler_latency_us_sum 400"),
              std::string::npos);
    EXPECT_NE(
        output.find(
            "adviskv_storage_put_handler_latency_us{quantile=\"0.95\"} 300"),
        std::string::npos);
    EXPECT_NE(output.find("# TYPE adviskv_storage_put_success_total counter"),
              std::string::npos);
    EXPECT_NE(output.find("adviskv_storage_put_success_total 2"),
              std::string::npos);
}

TEST(AdvisMetricsTest, ServesPrometheusMetricsOverHttp) {
    AdvisMetrics::get_instance().stop();
    AdvisMetrics::get_instance().reset();
    AdvisMetrics::get_instance().record_counter("storage_get_success", 7);

    MetricsOptions options;
    options.http_enable = true;
    options.http_host = "127.0.0.1";
    options.http_port = find_free_port();
    options.http_path = "/metrics";

    ASSERT_TRUE(AdvisMetrics::get_instance().init(options).ok());

    const std::string response =
        http_get(options.http_host, options.http_port, options.http_path);

    AdvisMetrics::get_instance().stop();

    EXPECT_NE(response.find("HTTP/1.1 200 OK"), std::string::npos);
    EXPECT_NE(response.find("Content-Type: text/plain; version=0.0.4"),
              std::string::npos);
    EXPECT_NE(response.find("adviskv_storage_get_success_total 7"),
              std::string::npos);
}

}  // namespace
}  // namespace adviskv