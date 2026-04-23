#include "storage/replica/replica_manager.h"
#include "common/status.h"
#include "common/type.h"
#include "storage/replica/replica.h"
#include <cstdint>
#include <fmt/format.h>
#include <memory>
#include <mutex>
#include <shared_mutex>

namespace adviskv::storage{


Replica* ReplicaManager::get_replica(const ShardID& shard_id) const{
    std::shared_lock<std::shared_mutex> locker(replica_map_mtx_);
    auto it = replica_map_.find(shard_id);
    if(it == replica_map_.end()){
        return nullptr;
    }
    return it->second.get();
}

Status ReplicaManager::add_replica(const ReplicaInitParam& param){
    auto&& replica = std::make_unique<Replica>();
    Status status = replica->init(param);
    if(!status.ok()){
        return status;
    }
    std::scoped_lock<std::shared_mutex> locker(replica_map_mtx_);
    if(replica_map_.count(replica->get_shard_key())){
        const ShardID shard_id = replica->get_shard_id();
        return Status{StatusCode::INVALID_ARGUMENT,
                      fmt::format("table_id: {}, shard_index: {} has been exist",
                                  shard_id.table_id, shard_id.shard_index)};
    }
    replica_map_.insert({replica->get_shard_key(), std::move(replica)});
    return Status::OK();
}


}
