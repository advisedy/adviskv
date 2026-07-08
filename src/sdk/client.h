#pragma once

#include <string>

#include "common/model/type.h"
#include "common/status.h"
#include "sdk/config.h"
#include "sdk/model.h"
#include "sdk/route_cache.h"
#include "sdk/sdm_route_client.h"
#include "sdk/storage_client.h"

namespace adviskv::sdk {

class KVClient {
public:
    explicit KVClient(const KVClientConf& conf);

    Status put(const Key& key, const Value& value);
    Status get(const Key& key, Value* value);
    Status del(const Key& key);

private:
    static bool should_invalidate_route(const Status& status);
    Status resolve_route(const Key& key, RouteInfo* route);
    Status refresh_route(const Key& key, RouteInfo* route);

    KVClientConf conf_;
    SdmRouteClient sdm_route_client_;
    RouteCache route_cache_;
    StorageClient storage_client_;
};

}  // namespace adviskv::sdk
