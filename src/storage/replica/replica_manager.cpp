#include "storage/replica/replica_manager.h"
#include "common/status.h"
#include "storage/replica/replica.h"
#include <cstdint>
#include <fmt/format.h>
#include <memory>
#include <mutex>
#include <shared_mutex>

namespace adviskv{


Replica* ReplicaManager::get_replica(int32_t table_id, int32_t shard_id) const{
    std::shared_lock<std::shared_mutex> locker(replica_map_mtx_);
    auto it = replica_map_.find({table_id, shard_id});
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
    if(replica_map_.count(replica->get_replica_id())){
        return Status{StatusCode::INVALID_ARGUMENT, fmt::format("table_id: {}, shard_id: {} has been exist", replica->get_table_id(), replica->get_shard_id())};
    }
    replica_map_.insert({replica->get_replica_id(), std::move(replica)});
    return Status::OK();
}


}
