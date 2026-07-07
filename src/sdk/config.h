#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

#include "common/define.h"
#include "common/status.h"

namespace adviskv::sdk {

enum class LogLevel : int32_t {
    DEBUG = 0,
    INFO = 1,
    WARN = 2,
    ERROR = 3,
};

using LogCallback = std::function<void(LogLevel, std::string_view)>;

struct LogOptions {
    LogLevel level{LogLevel::INFO};
    LogCallback callback;
};

struct KVClientConf {
    std::string db_name;
    std::string table_name;
    std::string sdm_host{"127.0.0.1"};
    int32_t sdm_port{50051};
    int32_t sdm_timeout_ms{2000};
    int32_t storage_timeout_ms{3000};
    int32_t route_shard_refresh_interval_ms{3000}; // 后台task刷新路由缓存的interval
    LogOptions log;

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!sdm_host.empty(),
                                    "sdm_host should not empty")
        RETURN_IF_INVALID_CONDITION(sdm_port > 0, "sdm_port should > 0")
        RETURN_IF_INVALID_CONDITION(sdm_timeout_ms > 0,
                                    "sdm_timeout_ms should > 0")
        RETURN_IF_INVALID_CONDITION(storage_timeout_ms > 0,
                                    "storage_timeout_ms should > 0")
        RETURN_IF_INVALID_CONDITION(route_shard_refresh_interval_ms >= 0,
                                    "route_shard_refresh_interval_ms should >= 0")
        return Status::OK();
    }
};

}  // namespace adviskv::sdk