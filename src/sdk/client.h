#pragma once

#include <string>

#include "common/status.h"
#include "common/type.h"
#include "sdk/config.h"
#include "sdk/model.h"
#include "sdk/sdm_route_client.h"
#include "sdk/storage_client.h"

namespace adviskv::sdk {

class KVClient {
   public:
    explicit KVClient(const KVClientConf& conf);

    Status put(const std::string& db_name, const std::string& table_name,
               const Key& key, const Value& value);
    Status get(const std::string& db_name, const std::string& table_name,
               const Key& key, Value* value);

   private:
    static bool should_invalidate_route(const Status& status);
    Status resolve_route(const RouteCacheKey& cache_key, RouteInfo* route);

    KVClientConf conf_;
    SdmRouteClient sdm_route_client_;
    StorageClient storage_client_;
};

}  // namespace adviskv::sdk
