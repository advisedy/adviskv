#pragma once

#include <string>
#include <string_view>

#include <fmt/format.h>

#include "sdk/config.h"

namespace adviskv::sdk {

inline std::string_view level_to_string(LogLevel level) {
    switch (level) {
        case LogLevel::DEBUG:
            return "DEBUG";
        case LogLevel::INFO:
            return "INFO";
        case LogLevel::WARN:
            return "WARN";
        case LogLevel::ERROR:
            return "ERROR";
    }
    return "UNKNOWN";
}

inline bool should_log(const KVClientConf& conf, LogLevel level) {
    return conf.log.callback && static_cast<int>(level) >= static_cast<int>(conf.log.level);
}

}  // namespace adviskv::sdk

#define ADVISKV_SDK_LOG(level, ...)                                                            \
    do {                                                                                       \
        const auto& adviskv_sdk_log_conf = (conf_);                                            \
        const auto adviskv_sdk_log_level = (level);                                            \
        if (::adviskv::sdk::should_log(adviskv_sdk_log_conf, adviskv_sdk_log_level)) {         \
            const std::string adviskv_sdk_log_message = fmt::format(__VA_ARGS__);              \
            adviskv_sdk_log_conf.log.callback(adviskv_sdk_log_level, adviskv_sdk_log_message); \
        }                                                                                      \
    } while (0)
