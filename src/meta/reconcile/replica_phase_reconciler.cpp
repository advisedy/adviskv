#include "meta/reconcile/replica_phase_reconciler.h"

#include "common/func.h"
#include "meta/store/meta_store.h"
#include "meta/store/meta_store_txn.h"

namespace adviskv::meta {

namespace {

ReplicaPhase project_phase_from_observed_facts(const MetaStoreTxn& txn, const Replica& replica,
                                               const ReplicaObservedOr& observed_or) {
    if (replica.state.phase == ReplicaPhase::DELETED) return ReplicaPhase::DELETED;

    if (replica.state.desired == ReplicaDesired::ABSENT) {
        // 没有删除确认，继续等。
        bool no_exist = observed_or.has_value() && observed_or->no_exist;
        if (replica.state.phase == ReplicaPhase::DELETING && no_exist) {
            return ReplicaPhase::DELETED;
        }
        return ReplicaPhase::DELETING;
    }

    if (replica.state.phase == ReplicaPhase::DELETING || replica.state.phase == ReplicaPhase::ERROR) {
        return replica.state.phase;
    }

    NodeOr node;
    if (txn.get_node(replica.spec.assign_node_id, node).fail() || node.is_empty()) {
        return replica.state.phase;
    }
    if (node->state.status == NodeStatus::OFFLINE) {
        return ReplicaPhase::LOST;
    } else if (replica.state.phase == ReplicaPhase::LOST) {
        return ReplicaPhase::CREATING;
    }

    // 没有心跳观测时维持原 phase。
    if (observed_or.is_empty()) {
        return replica.state.phase;
    }
    const ReplicaObserved& observed = observed_or.value();

    switch (observed.storage_status) {
        case StorageReplicaStatus::INITIALIZING:
        case StorageReplicaStatus::RECOVERING:
            return ReplicaPhase::CREATING;
        case StorageReplicaStatus::READY:
            return ReplicaPhase::READY;
        case StorageReplicaStatus::FAULTED:
        default:
            return ReplicaPhase::ERROR;
    }
}

}  // namespace

ReplicaPhaseReconciler::ReplicaPhaseReconciler(MetaStore* store) : store_(store) {}

Status ReplicaPhaseReconciler::run() {
    if (store_ == nullptr) {
        return Status{StatusCode::INVALID_ARGUMENT, "store is nullptr"};
    }

    return store_->write_with([&](MetaStoreTxn& txn) -> Status {
        std::vector<Replica> replicas;
        RETURN_IF_INVALID_STATUS(txn.list_replicas(replicas))
        for (Replica& replica : replicas) {
            RETURN_IF_INVALID_STATUS(reconcile_replica_phase(txn, replica))
        }
        return Status::OK();
    });
}

Status ReplicaPhaseReconciler::reconcile_replica_phase(MetaStoreTxn& txn, Replica& replica) {
    ReplicaObservedOr observed_or;
    RETURN_IF_INVALID_STATUS(txn.get_replica_observed(replica.replica_id, observed_or))
    ReplicaPhase next_phase = project_phase_from_observed_facts(txn, replica, observed_or);
    if (next_phase == replica.state.phase) {
        return Status::OK();
    }
    replica.state.phase = next_phase;
    replica.state.update_ts = func::get_current_ts_ms();
    return txn.put_replica(replica);
}

}  // namespace adviskv::meta
