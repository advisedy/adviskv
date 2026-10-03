#pragma once

#include "common/model/model.h"
#include "common/status.h"
#include "meta/model/model.h"

namespace adviskv::meta {

class MetaStore;
class RouteService;

class RouteReconciler {
private:
    friend class RouteService;

    explicit RouteReconciler(MetaStore* store);

    Status run();
    Status check_shard_route(const Table& table, ShardIndex shard_index);

    MetaStore* store_{nullptr};
};

}  // namespace adviskv::meta
