#pragma once

#include <string>
#include <memory>
#include "spdlog/logger.h"
#include "spdlog/spdlog.h"

namespace adviskv::common{

struct LogConfig {
    std::string logger_name;
    std::string log_dir;
    std::string log_filename;
    std::string log_level;
    bool log_to_console {true};
    bool log_to_file {true};
};

class Logger {

public:
    void init(const LogConfig& config);

    static Logger& get_instance() {
        static Logger instance;
        return instance;
    }

    std::shared_ptr<spdlog::logger> get_logger();
    
    Logger &operator=(const Logger&) = delete;
    Logger(const Logger&) = delete;

private:
    Logger() = default;
    std::shared_ptr<spdlog::logger> logger_{nullptr};
    bool init_flag_{false};
};

#define LOG_DEBUG(...)  adviskv::common::Logger::get_instance().get_logger()->debug(__VA_ARGS__);
#define LOG_INFO(...)  adviskv::common::Logger::get_instance().get_logger()->info(__VA_ARGS__);
#define LOG_WARN(...)  adviskv::common::Logger::get_instance().get_logger()->warn(__VA_ARGS__);
#define LOG_ERROR(...) adviskv::common::Logger::get_instance().get_logger()->error(__VA_ARGS__);


}
