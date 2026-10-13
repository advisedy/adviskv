#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include "common/model/model.h"
#include "common/status.h"
#include "sdk/config.h"
#include "sdk/model.h"
#include "storage.grpc.pb.h"

namespace adviskv::sdk {

class StorageClient {
public:
    explicit StorageClient(const KVClientConf& conf);

    Status put(const ShardRoute& route, const Key& key, const Value& value, const std::string& request_id,
               Milliseconds timeout = Milliseconds::zero()) const;
    Status get(const ShardRoute& route, const Key& key, Value* value,
               Milliseconds timeout = Milliseconds::zero()) const;
    Status del(const ShardRoute& route, const Key& key, const std::string& request_id,
               Milliseconds timeout = Milliseconds::zero()) const;
    Status cas(const ShardRoute& route, const Key& key, const Value& expected, const Value& value,
               const std::string& request_id, Milliseconds timeout = Milliseconds::zero()) const;

private:
    static Status select_leader_replica(const ShardRoute& route, RouteReplica* replica);
    rpc::StorageService::Stub* make_stub(const Endpoint& endpoint) const;


    template <typename Response, typename Func>
    Status call_leader(const ShardRoute& route, std::string_view op_name, const Key& key, Milliseconds timeout,
                       Response* response, Func&& send_rpc) const;

    Milliseconds effective_timeout(Milliseconds timeout) const;

    KVClientConf conf_;
    mutable std::mutex mutex_;
    mutable std::unordered_map<std::string, std::unique_ptr<rpc::StorageService::Stub>> stub_cache_;
};

}  // namespace adviskv::sdk
