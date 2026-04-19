#pragma once

#include <mutex>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "common/thread_pool.h"
#include "sdsdk/istorage_callback.h"
#include "sdsdk/type.h"

namespace adviskv::sdsdk {

class ReplicaController {
   public:
    ReplicaController() = default;

    Status init(StorageCallbackPtr callback, int32_t worker_count);

    Status apply_desired_set(const std::vector<DesiredReplicaSpec>& desired_set);

    Status collect_cached_replica_reports(std::vector<ReplicaReport>& out) const;

    bool all_ready() const;
    bool has_non_follower_replica() const;

    void stop();

   private:
    struct LocalReplica {
        ReplicaKey key;
        pb::ReplicaRole role{pb::ReplicaRole::FOLLOWER};
        pb::ReplicaStatus status{pb::ReplicaStatus::ADDING};
        Endpoint endpoint;
        bool exists_locally{false};
        bool is_updating{false};
    };

    void schedule_create(const DesiredReplicaSpec& spec);
    void schedule_delete(const ReplicaKey& key);
    void schedule_change_role(const ReplicaKey& key,
                              pb::ReplicaRole old_role,
                              pb::ReplicaRole new_role);

   private:
    mutable std::mutex mutex_;
    std::unordered_map<ReplicaKey, LocalReplica, ReplicaKeyHash> replicas_;
    StorageCallbackPtr callback_;
    adviskv::common::ThreadPool thread_pool_;
    bool initialized_{false};
};

}  // namespace adviskv::sdsdk
