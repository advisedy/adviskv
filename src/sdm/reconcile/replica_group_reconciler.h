#pragma once

#include <algorithm>
#include <semaphore>
#include <unordered_map>

#include "common/background_task.h"
#include "common/model/storage_replica_status.h"
#include "common/status.h"
#include "common/type.h"
#include "sdm/model/sdm_store.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {

class ReplicaReconclieTask : public BackgroundTask {
   protected:
    void run() override {
        std::vector<ReplicaGroup> groups;

        if (store_->list_replica_groups(groups).fail()) {
            //
        }

        for (ReplicaGroup& group : groups) {
            if (check_group(group).fail()) {
                //
            }
        }
    }

   private:
    Status check_group(ReplicaGroup& group) {
        if (group.phase == ReplicaGroupPhase::TABLE_RECONCILE) {
            //

            return Status::OK();
        }
        // 计算一下健康的数量是多少，补充到target_replica_count的数量
        // 把不健康的删掉
        // 如果剩下来的数量比target_replica_count大的话，就选择然后删掉
        int health_count = calc_healthy_replica(group);
        if (health_count < group.target_replica_count) {
            // TODO
            return Status::OK();
        }
        int unhealth_count = calc_unhealthy_replica(group);
        if (unhealth_count > 0) {
            // TODO
            return Status::OK();
        }

        if (group.desired_members.size() > group.target_replica_count) {
            // TODO
            return Status::OK();
        }

        return Status::OK();
    }

    int calc_healthy_replica(const ReplicaGroup& group) {
        std::vector<Replica> replicas;
        store_->list_replicas_by_shard(group.shard_id, replicas);
        return std::count_if(
            replicas.begin(), replicas.end(), [](const Replica& replica) {
                return (!replica.spec.assign_node_id.empty() and
                        replica.state.phase != ReplicaPhase::LOST and
                        replica.state.phase != ReplicaPhase::ERROR);
            });
    }
    int calc_unhealthy_replica(const ReplicaGroup& group) {
        std::vector<Replica> replicas;
        store_->list_replicas_by_shard(group.shard_id, replicas);
        return std::count_if(
            replicas.begin(), replicas.end(), [](const Replica& replica) {
                return (replica.state.phase == ReplicaPhase::LOST or
                        replica.state.phase == ReplicaPhase::ERROR);
            });
    }
    SdmStore* store_;
};

class ReplicaGroupReconciler {
   public:
    void start();

   private:
    ReplicaReconclieTask reconcile_task;
    SdmStore* store_;
};

}  // namespace adviskv::sdm