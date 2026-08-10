#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>

#include <poll.h>
#include <unistd.h>

#include "common/log.h"
#include "maelstrom_node.h"

namespace {

std::string default_data_root() {
    if (const char* env = std::getenv("ADVISKV_MAELSTROM_DATA")) {
        return env;
    }
    return (std::filesystem::temp_directory_path() / "adviskv_maelstrom").string();
}

}  // namespace

int main() {
    // Maelstrom protocol uses stdout for network messages. Keep application
    // logs on files (and never write non-JSON to stdout).
    adviskv::LogConfig log_config;
    log_config.logger_name = "maelstrom_raft";
    log_config.log_dir = default_data_root() + "/logs";
    log_config.log_filename = "maelstrom_raft.log";
    log_config.log_level = "info";
    log_config.log_to_console = false;
    log_config.log_to_file = true;
    try {
        adviskv::Logger::get_instance().init(log_config);
    } catch (const std::exception& ex) {
        std::fprintf(stderr, "logger init failed: %s\n", ex.what());
        return 1;
    }

    adviskv::maelstrom::MaelstromRaftNode node(default_data_root());

    std::string line;
    line.reserve(4096);

    constexpr int kTickMs = 20;
    while (true) {
        pollfd pfd{};
        pfd.fd = STDIN_FILENO;
        pfd.events = POLLIN;

        const int pr = ::poll(&pfd, 1, kTickMs);
        if (pr < 0) {
            if (errno == EINTR) continue;
            LOG_ERROR("poll failed errno={}", errno);
            break;
        }

        if (pr == 0) {
            node.tick();
            continue;
        }

        // POLLIN and POLLHUP often arrive together when the writer closes the
        // pipe. Drain readable data first, then treat hangup as EOF.
        if (pfd.revents & POLLIN) {
            char buf[8192];
            const ssize_t n = ::read(STDIN_FILENO, buf, sizeof(buf));
            if (n < 0) {
                if (errno == EINTR) continue;
                LOG_ERROR("read stdin failed errno={}", errno);
                break;
            }
            if (n == 0) {
                if (!line.empty()) {
                    node.handle_line(line);
                    line.clear();
                }
                LOG_INFO("stdin eof, exiting");
                break;
            }
            for (ssize_t i = 0; i < n; ++i) {
                if (buf[i] == '\n') {
                    node.handle_line(line);
                    line.clear();
                } else {
                    line.push_back(buf[i]);
                }
            }
        } else if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
            if (!line.empty()) {
                node.handle_line(line);
                line.clear();
            }
            LOG_INFO("stdin closed, exiting");
            break;
        }

        // Always tick after I/O so RPC replies can advance commit promptly.
        node.tick();
    }

    return 0;
}
