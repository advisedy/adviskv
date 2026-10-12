#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

#include "common/constants.h"
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
    std::string meta_host{K_DEFAULT_SDK_META_HOST};
    int32_t meta_port{K_DEFAULT_SDK_META_PORT};

    int32_t rpc_timeout_ms{K_DEFAULT_SDK_RPC_TIMEOUT_MS};  // 单次 RPC 超时时间
    int32_t write_timeout_ms{K_DEFAULT_SDK_WRITE_TIMEOUT_MS};  // 一次 put/del（包含路由刷新和重试）的总耗时
    int32_t read_timeout_ms{K_DEFAULT_SDK_READ_TIMEOUT_MS};  // 一次 get（包含路由刷新和重试）的总耗时
    int32_t max_retries{K_DEFAULT_SDK_MAX_RETRIES};  // 一次 put/del/get 的额外重试次数
    int32_t route_max_retries{K_DEFAULT_SDK_ROUTE_MAX_RETRIES};  // 一次请求中最多重新拉取 route 的次数
    int32_t retry_delay_ms{K_DEFAULT_SDK_RETRY_DELAY_MS};  // 第一次重试前的等待时间，后续每次翻倍
    int32_t route_shard_refresh_interval_ms{K_DEFAULT_SDK_ROUTE_SHARD_REFRESH_INTERVAL_MS};  // 后台刷新路由缓存的间隔
    LogOptions log;

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!meta_host.empty(), "meta_host should not empty")
        RETURN_IF_INVALID_CONDITION(meta_port > 0, "meta_port should > 0")
        RETURN_IF_INVALID_CONDITION(rpc_timeout_ms > 0, "rpc_timeout_ms should > 0")
        RETURN_IF_INVALID_CONDITION(write_timeout_ms > 0, "write_timeout_ms should > 0")
        RETURN_IF_INVALID_CONDITION(read_timeout_ms > 0, "read_timeout_ms should > 0")
        RETURN_IF_INVALID_CONDITION(max_retries >= 0, "max_retries should >= 0")
        RETURN_IF_INVALID_CONDITION(route_max_retries >= 0, "route_max_retries should >= 0")
        RETURN_IF_INVALID_CONDITION(retry_delay_ms >= 0, "retry_delay_ms should >= 0")
        RETURN_IF_INVALID_CONDITION(route_shard_refresh_interval_ms >= 0, "route_shard_refresh_interval_ms should >= 0")
        return Status::OK();
    }
};

}  // namespace adviskv::sdk
