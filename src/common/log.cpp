#include "log.h"

#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/hourly_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cassert>
#include <cctype>
#include <filesystem>
#include <stdexcept>
#include <vector>

namespace adviskv {

namespace {

spdlog::level::level_enum ParseLogLevel(const std::string& level) {
    std::string normalized = level;
    std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                   [](unsigned char c) { return std::tolower(c); });

    if (normalized == "trace") {
        return spdlog::level::trace;
    }
    if (normalized == "debug") {
        return spdlog::level::debug;
    }
    if (normalized == "info") {
        return spdlog::level::info;
    }
    if (normalized == "warn" || normalized == "warning") {
        return spdlog::level::warn;
    }
    if (normalized == "error" || normalized == "err") {
        return spdlog::level::err;
    }
    if (normalized == "critical") {
        return spdlog::level::critical;
    }
    throw std::invalid_argument("invalid log level: " + level);
}

}  // namespace

void Logger::init(const LogConfig& config) {
    if (init_flag_) {
        return;
    }

    std::vector<spdlog::sink_ptr> sinks;

    if (config.log_to_console) {
        auto console_sink =
            std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        sinks.push_back(console_sink);
    }

    if (config.log_to_file) {
        if (!std::filesystem::is_directory(config.log_dir)) {
            if (std::filesystem::exists(config.log_dir)) {
                throw std::runtime_error(
                    "logger init failed: log_dir exists but is not a "
                    "directory");
            } else {
                std::filesystem::create_directories(config.log_dir);
            }
        }

        std::string log_path = config.log_dir + "/" + config.log_filename;
        auto file_sink =
            std::make_shared<spdlog::sinks::hourly_file_sink_mt>(log_path);
        sinks.push_back(file_sink);
    }

    if (sinks.empty()) {
        throw std::runtime_error("logger init failed: no sink enabled");
    }

    logger_ = std::make_shared<spdlog::logger>(config.logger_name,
                                               sinks.begin(), sinks.end());

    logger_->set_level(ParseLogLevel(config.log_level));
    logger_->set_pattern(
        "[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [%n] [tid:%t] [%s:%#] %v");
    logger_->flush_on(spdlog::level::info);

    init_flag_ = true;
}

std::shared_ptr<spdlog::logger> Logger::get_logger() {
    assert(init_flag_ && logger_ != nullptr);
    return logger_;
}

}  // namespace adviskv
