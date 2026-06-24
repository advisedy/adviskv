#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "common/status.h"
#include "common/type.h"
#include "sdk/config.h"
#include "sdk/model.h"
#include "storage.grpc.pb.h"

namespace adviskv::sdk {

class StorageClient {
   public:
    explicit StorageClient(const KVClientConf& conf);

    Status put(const RouteInfo& route, const Key& key,
               const Value& value) const;
    Status get(const RouteInfo& route, const Key& key, Value* value) const;
    Status del(const RouteInfo& route, const Key& key) const;

   private:
    static Status select_leader_replica(const RouteInfo& route,
                                        RouteReplica* replica);
    rpc::StorageService::Stub* make_stub(const Endpoint& endpoint) const;

    KVClientConf conf_;
    mutable std::mutex mutex_;
    mutable std::unordered_map<std::string,
                               std::unique_ptr<rpc::StorageService::Stub>>
        stub_cache_;
};

}  // namespace adviskv::sdk
