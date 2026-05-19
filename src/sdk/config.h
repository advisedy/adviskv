#pragma once

#include <cstddef>
#include <string>

#include "common/define.h"
#include "common/status.h"

namespace adviskv::sdk {

struct KVClientConf {
    std::string db_name;
    std::string table_name;
    std::string sdm_host{"127.0.0.1"};
    int32_t sdm_port{50051};
    int32_t sdm_timeout_ms{2000};
    int32_t storage_timeout_ms{3000};
    int64_t route_cache_ttl_ms{8000};
    size_t route_cache_capacity{1024};

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!sdm_host.empty(),
                                    "sdm_host should not empty")
        RETURN_IF_INVALID_CONDITION(sdm_port > 0, "sdm_port should > 0")
        RETURN_IF_INVALID_CONDITION(sdm_timeout_ms > 0,
                                    "sdm_timeout_ms should > 0")
        RETURN_IF_INVALID_CONDITION(storage_timeout_ms > 0,
                                    "storage_timeout_ms should > 0")
        RETURN_IF_INVALID_CONDITION(route_cache_ttl_ms >= 0,
                                    "route_cache_ttl_ms should >= 0")
        RETURN_IF_INVALID_CONDITION(route_cache_capacity > 0,
                                    "route_cache_capacity should > 0")
        return Status::OK();
    }
};

}  // namespace adviskv::sdk
