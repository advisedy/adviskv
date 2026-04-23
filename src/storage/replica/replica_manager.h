#pragma once

#include <google/protobuf/stubs/port.h>

#include <cstdint>
#include <map>
#include <memory>
#include <shared_mutex>
#include <unordered_map>

#include "common/status.h"
#include "storage/replica/replica.h"

namespace adviskv::storage {

class ReplicaManager {
   public:
    Replica* get_replica_by_id(const ReplicaID& replica_id) const;
    Replica* get_replica_by_shard(const ShardID& shard_id) const;
    Status add_replica(const ReplicaInitParam& param);

   private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<ReplicaID, std::unique_ptr<Replica>, ReplicaIDHash>
        replica_map_;
    std::unordered_map<ShardID, ReplicaID, ShardIDHash> shard_primary_index_;
};

}  // namespace adviskv::storage
