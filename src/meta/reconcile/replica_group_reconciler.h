#pragma once

#include "common/status.h"
#include "meta/reconcile/replica_group_membership_reconciler.h"
#include "meta/reconcile/replica_group_plan_reconciler.h"
#include "meta/reconcile/replica_phase_reconciler.h"

namespace adviskv::meta {

class MetaStore;
class NodeSelector;
class ReplicaGroupService;

class ReplicaGroupReconciler {
private:
    friend class ReplicaGroupService;

    ReplicaGroupReconciler(MetaStore* store, NodeSelector* selector);

    Status run();
    ReplicaGroupPlanReconciler plan_reconciler_;
    ReplicaPhaseReconciler phase_reconciler_;
    ReplicaGroupMembershipReconciler membership_reconciler_;
};

}  // namespace adviskv::meta
