#pragma once

#include <vector>

#include "common/model/type.h"
#include "common/status.h"
#include "meta/model/model.h"
#include "meta/reconcile/replica_group_reconcile_util.h"

namespace adviskv::meta {

class MetaStore;
class NodeSelector;
class ReplicaGroupReconciler;

class ReplicaGroupMembershipReconciler {
private:
    friend class ReplicaGroupReconciler;

    ReplicaGroupMembershipReconciler(MetaStore* store, NodeSelector* selector);

    Status run();
    Status reconcile_group(const ReplicaGroup& group);
    Status add_members(const ReplicaGroup& group, int32_t count_to_add);
    Status remove_members(const ReplicaGroup& group, int32_t count_to_remove);
    Status remove_specific_members(const ReplicaGroup& group, const std::vector<ReplicaID>& victims);
    Status cleanup_group(const ReplicaGroup& group);

    MetaStore* store_{nullptr};
    NodeSelector* selector_{nullptr};
};

}  // namespace adviskv::meta
