#pragma once

#include <google/protobuf/stubs/port.h>

#include <cstdint>
#include <map>
#include <memory>
#include <shared_mutex>
#include <unordered_map>

#include "common/status.h"
#include "storage/raft/raft_background_task.h"
#include "storage/replica/replica.h"
namespace adviskv::storage {

class ReplicaManager {
   public:
    Replica* get_replica_by_id(const ReplicaID& replica_id) const;
    Replica* get_replica_by_shard(const ShardID& shard_id) const;
    Status add_replica(const ReplicaInitParam& param);
    std::vector<Replica*> get_replicas() const;
    void start_tick();  // 记得调用这个，不然没有tick了
    void recover();

   private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<ReplicaID, std::unique_ptr<Replica>, ReplicaIDHash>
        replica_map_;
    std::unordered_map<ShardID, ReplicaID, ShardIDHash> shard_primary_index_;

    std::unique_ptr<RaftTickTask>
        raft_tick_task_;  // 这个放到最后面，到时候先析构他。
};

}  // namespace adviskv::storage
