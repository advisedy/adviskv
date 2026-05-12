#pragma once

#include <google/protobuf/stubs/port.h>

#include <cstdint>
#include <map>
#include <memory>
#include <shared_mutex>
#include <unordered_map>

#include "common/status.h"
#include "storage/persist/replica_meta_persist_engine.h"
#include "storage/raft/raft_background_task.h"
#include "storage/replica/replica.h"
namespace adviskv::storage {

class ReplicaManager {
   public:
    ReplicaManager(std::string data_dir)
        : data_dir_(std::move(data_dir)), meta_persist_(data_dir_) {}
    Replica* get_replica_by_id(const ReplicaID& replica_id) const;
    Replica* get_replica_by_shard(const ShardID& shard_id) const;
    Status add_replica(const ReplicaInitParam& param);
    Status delete_replica(const ReplicaID& replica_id);
    std::vector<Replica*> get_replicas() const;
    const std::string& get_data_dir() const;
    void start_tick(); // 记得要调用这个，不然就没有tick了
    void recover();

   private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<ReplicaID, std::unique_ptr<Replica>, ReplicaIDHash>
        replica_map_;
    std::unordered_map<ShardID, ReplicaID, ShardIDHash> shard_primary_index_;

    std::unique_ptr<RaftTickTask>
        raft_tick_task_; // 这个放最后面，到时候先析构他

    std::string data_dir_; // 这个是replica存放数据的目录
    ReplicaMetaPersistEngine meta_persist_;
};

}  // namespace adviskv::storage
