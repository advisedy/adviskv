#pragma once

#include <vector>

#include "common/model/type.h"
#include "common/status.h"
#include "topo/model/model.h"
#include "topo/service/replica_group_service.h"

namespace adviskv::topo {

class ReplicaGroupMembershipReconciler {
private:
    friend class ReplicaGroupService;

    explicit ReplicaGroupMembershipReconciler(ReplicaGroupReconcileContext ctx);

    Status reconcile_all();
    Status reconcile_group(const ReplicaGroup& group);
    Status add_members(const ReplicaGroup& group, int32_t count_to_add);
    Status remove_members(const ReplicaGroup& group, int32_t count_to_remove);
    Status remove_specific_members(const ReplicaGroup& group, const std::vector<ReplicaID>& victims);
    Status cleanup_group(const ReplicaGroup& group);

    ReplicaGroupReconcileContext ctx_;
};

}  // namespace adviskv::topo