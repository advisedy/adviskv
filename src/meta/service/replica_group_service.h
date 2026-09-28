#pragma once

#include "common/status.h"
#include "meta/model/model.h"
#include "meta/model/param.h"
#include "meta/reconcile/replica_group_reconciler.h"

namespace adviskv::meta {

class MetaStore;
class NodeSelector;

class ReplicaGroupService {
public:
    ReplicaGroupService(MetaStore* store, NodeSelector* selector);

    Status reconcile_all();
    Status build_heartbeat_result(const HeartBeatParam& param, HeartBeatResult& result) const;

private:
    MetaStore* store_{nullptr};
    ReplicaGroupReconciler reconciler_;
};

}  // namespace adviskv::meta
