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
    explicit ReplicaManager(ReplicaRuntimeOptions runtime)
        : runtime_options_(std::move(runtime)),
          meta_persist_(runtime_options_.data_dir) {}
    ReplicaManager(std::string data_dir, int32 raft_rpc_timeout_ms = 1000)
        : ReplicaManager(ReplicaRuntimeOptions{std::move(data_dir),
                                               raft_rpc_timeout_ms}) {}
    ReplicaPtr get_replica_by_id(const ReplicaID& replica_id) const;
    ReplicaPtr get_replica_by_shard(const ShardID& shard_id) const;
    Status add_replica(const ReplicaInitParam& param);
    Status delete_replica(const ReplicaID& replica_id);
    std::vector<ReplicaPtr> get_replicas() const;
    const std::string& get_data_dir() const;
    void start_tick();  // 记得要调用这个，不然就没有tick了
    void recover();

   private:
    ReplicaInitParam fill_param_runtime(ReplicaInitParam param) const;

    mutable std::shared_mutex mutex_;
    std::unordered_map<ReplicaID, ReplicaPtr, ReplicaIDHash> replica_map_;
    std::unordered_map<ShardID, ReplicaID, ShardIDHash> shard_primary_index_;

    std::unique_ptr<RaftTickTask>
        raft_tick_task_;  // 这个放最后面，到时候先析构他

    ReplicaRuntimeOptions runtime_options_;
    ReplicaMetaPersistEngine meta_persist_;
};

}  // namespace adviskv::storage
