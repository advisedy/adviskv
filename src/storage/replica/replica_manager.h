#pragma once

#include "storage/replica/replica.h"
#include "common/status.h"
#include <cstdint>
#include <google/protobuf/stubs/port.h>
#include <map>
#include <memory>
#include <unordered_map>
#include <shared_mutex>

namespace adviskv::storage{

class ReplicaManager{
    
public:
    
    Replica* get_replica(const ShardID& shard_id) const;
    // common::Status AddReplica(); TODO
    
    Status add_replica(const ReplicaInitParam& params);
    
private:
    mutable std::shared_mutex replica_map_mtx_;
    std::unordered_map<ShardKey, std::unique_ptr<Replica>, ShardKeyHash> replica_map_;
    
};


}
