#pragma once

#include "common/model/type.h"
#include "common/status.h"
#include "meta/model/model.h"
#include "meta/service/replica_group_service.h"

namespace adviskv::meta {

// 这个是全权由ReplicaGroupService去负责
class ReplicaGroupPlanReconciler {
private:
    friend class ReplicaGroupService;

    explicit ReplicaGroupPlanReconciler(ReplicaGroupReconcileContext ctx);

    Status reconcile_all();
    Status reconcile_table(const Table& table);
    Status reconcile_present_table(const Table& table);
    Status ensure_group_for_shard(const Table& table, ShardIndex shard_index);
    Status reconcile_absent_table(const Table& table);

    ReplicaGroupReconcileContext ctx_;
};

}  // namespace adviskv::meta