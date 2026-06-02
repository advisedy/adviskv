#pragma once

#include <memory>
#include <string>

#include "common/define.h"
#include "spdlog/logger.h"
#include "spdlog/spdlog.h"

namespace adviskv {

struct LogConfig {
    std::string logger_name;
    std::string log_dir;
    std::string log_filename;
    std::string log_level;
    bool log_to_console{true};
    bool log_to_file{true};
};

class Logger {
   public:
    void init(const LogConfig& config);

    static Logger& get_instance() {
        static Logger instance;
        return instance;
    }

    std::shared_ptr<spdlog::logger> get_logger();

    DISALLOW_COPY_AND_ASSIGN(Logger)

   private:
    Logger() = default;
    std::shared_ptr<spdlog::logger> logger_{nullptr};
    bool init_flag_{false};
};

#define LOG_DEBUG(...)                                           \
    adviskv::Logger::get_instance().get_logger()->log(           \
        spdlog::source_loc{__FILE__, __LINE__, SPDLOG_FUNCTION}, \
        spdlog::level::debug, __VA_ARGS__);
#define LOG_INFO(...)                                            \
    adviskv::Logger::get_instance().get_logger()->log(           \
        spdlog::source_loc{__FILE__, __LINE__, SPDLOG_FUNCTION}, \
        spdlog::level::info, __VA_ARGS__);
#define LOG_WARN(...)                                            \
    adviskv::Logger::get_instance().get_logger()->log(           \
        spdlog::source_loc{__FILE__, __LINE__, SPDLOG_FUNCTION}, \
        spdlog::level::warn, __VA_ARGS__);
#define LOG_ERROR(...)                                           \
    adviskv::Logger::get_instance().get_logger()->log(           \
        spdlog::source_loc{__FILE__, __LINE__, SPDLOG_FUNCTION}, \
        spdlog::level::err, __VA_ARGS__);

}  // namespace adviskv
