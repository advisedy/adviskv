#pragma once

#include "storage/replica/replica.h"
#include "common/status.h"
#include <cstdint>
#include <google/protobuf/stubs/port.h>
#include <map>
#include <memory>
#include <unordered_map>
#include <shared_mutex>

namespace adviskv{

class ReplicaManager{
    
public:
    
    Replica* get_replica(int32_t table_id, int32_t shard_id) const;
    // common::Status AddReplica(); TODO
    
    Status add_replica(const ReplicaInitParam& params);
    
private:
    mutable std::shared_mutex replica_map_mtx_;
    std::unordered_map<ReplicaID, std::unique_ptr<Replica>, ReplicaIDHash> replica_map_;
    
};


}