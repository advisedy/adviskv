#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "common/define.h"
#include "common/status.h"
#include "common/type.h"
#include "storage.grpc.pb.h"

namespace adviskv::cli {

struct StorageCliTarget {
    Endpoint endpoint;
    TableID table_id{-1};
    ShardIndex shard_id{-1};
    int32_t timeout_ms{3000};

    Status validate() const {
        RETURN_IF_INVALID_CONDITION(!endpoint.ip.empty(),
                                    "endpoint.ip should not empty")
        RETURN_IF_INVALID_CONDITION(endpoint.port > 0,
                                    "endpoint.port should > 0")
        RETURN_IF_INVALID_CONDITION(table_id >= 0, "table_id should >= 0")
        RETURN_IF_INVALID_CONDITION(shard_id >= 0, "shard_id should >= 0")
        RETURN_IF_INVALID_CONDITION(timeout_ms > 0, "timeout_ms should > 0")
        return Status::OK();
    }
};

class DirectStorageClient {
   public:
    Status put(const StorageCliTarget& target, const Key& key,
               const Value& value) const;
    Status get(const StorageCliTarget& target, const Key& key,
               Value* value) const;

   private:
    rpc::StorageService::Stub* make_stub(const Endpoint& endpoint) const;

    mutable std::mutex mutex_;
    mutable std::unordered_map<std::string,
                               std::unique_ptr<rpc::StorageService::Stub>>
        stub_cache_;
};

}  // namespace adviskv::cli
