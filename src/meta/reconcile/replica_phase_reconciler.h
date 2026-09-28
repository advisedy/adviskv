#pragma once

#include "common/status.h"
#include "meta/model/model.h"

namespace adviskv::meta {

class MetaStore;
class MetaStoreTxn;
class ReplicaGroupReconciler;

class ReplicaPhaseReconciler {
private:
    friend class ReplicaGroupReconciler;

    explicit ReplicaPhaseReconciler(MetaStore* store);

    Status run();
    Status reconcile_replica_phase(MetaStoreTxn& txn, Replica& replica);

    MetaStore* store_{nullptr};
};

}  // namespace adviskv::meta
