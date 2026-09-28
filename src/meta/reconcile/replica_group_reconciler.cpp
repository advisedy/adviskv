#include "meta/reconcile/replica_group_reconciler.h"

#include "common/log.h"
#include "meta/store/meta_store.h"

namespace adviskv::meta {

ReplicaGroupReconciler::ReplicaGroupReconciler(MetaStore* store, NodeSelector* selector)
        : plan_reconciler_(store), phase_reconciler_(store), membership_reconciler_(store, selector) {}

Status ReplicaGroupReconciler::run() {
    Status status = plan_reconciler_.run();
    if (status.fail()) {
        LOG_WARN("[ReplicaGroupReconciler] plan reconcile failed, msg={}", status.msg());
        return status;
    }

    status = phase_reconciler_.run();
    if (status.fail()) {
        LOG_WARN("[ReplicaGroupReconciler] reconcile replica phases failed, msg={}", status.msg());
        return status;
    }

    status = membership_reconciler_.run();
    if (status.fail()) {
        LOG_WARN("[ReplicaGroupReconciler] membership reconcile failed, msg={}", status.msg());
        return status;
    }

    // 再跑一次 plan，处理本轮状态变化。
    status = plan_reconciler_.run();
    if (status.fail()) {
        LOG_WARN("[ReplicaGroupReconciler] final plan reconcile failed, msg={}", status.msg());
        return status;
    }
    return Status::OK();
}

}  // namespace adviskv::meta
