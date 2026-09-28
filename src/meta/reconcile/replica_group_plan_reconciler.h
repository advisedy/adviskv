#pragma once

#include "common/model/type.h"
#include "common/status.h"
#include "meta/model/model.h"
#include "meta/reconcile/replica_group_reconcile_util.h"

namespace adviskv::meta {

class MetaStore;
class NodeSelector;
class ReplicaGroupReconciler;

class ReplicaGroupPlanReconciler {
private:
    friend class ReplicaGroupReconciler;

    explicit ReplicaGroupPlanReconciler(MetaStore* store);

    Status run();
    Status reconcile_table(const Table& table);
    Status reconcile_present_table(const Table& table);
    Status ensure_group_for_shard(const Table& table, ShardIndex shard_index);
    Status reconcile_absent_table(const Table& table);

    MetaStore* store_{nullptr};
};

}  // namespace adviskv::meta
