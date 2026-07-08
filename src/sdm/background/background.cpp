#include "sdm/background/background.h"

#include "common/define.h"
#include "common/log.h"
#include "common/status.h"
#include "sdm/service/service_manager.h"

namespace adviskv::sdm {

//////////////////////////////////////////
// TableReconcileTask
//////////////////////////////////////////

TableReconcileTask::TableReconcileTask(ServiceManager* service_manager) : service_manager_(service_manager) {}

void TableReconcileTask::run() {
    Status status = reconcile_once();
    if (status.fail()) {
        LOG_WARN("[TableReconcileTask] table reconcile task run failed, msg={}", status.msg());
    }
}

Status TableReconcileTask::reconcile_once() {
    RETURN_IF_NULLPTR(service_manager_, "service manager is nullptr")
    return service_manager_->reconcile_tables();
}

//////////////////////////////////////////
// ReplicaGroupReconcileTask
//////////////////////////////////////////

ReplicaGroupReconcileTask::ReplicaGroupReconcileTask(ServiceManager* service_manager)
        : service_manager_(service_manager) {}

void ReplicaGroupReconcileTask::run() {
    if (service_manager_ == nullptr) {
        LOG_ERROR("[ReplicaGroupReconcileTask] service manager is nullptr");
        return;
    }
    Status status = service_manager_->reconcile_replica_groups();
    if (status.fail()) {
        LOG_WARN("[ReplicaGroupReconcileTask] replica group reconcile run failed, msg={}", status.msg());
    }
}

//////////////////////////////////////////
// RouteUpdateCheckTask
//////////////////////////////////////////

RouteUpdateCheckTask::RouteUpdateCheckTask(ServiceManager* service_manager) : service_manager_(service_manager) {}

void RouteUpdateCheckTask::run() {
    if (service_manager_ == nullptr) {
        LOG_ERROR("[RouteUpdateCheckTask] service manager is nullptr");
        return;
    }
    Status status = service_manager_->reconcile_routes();
    if (status.fail()) {
        LOG_WARN("[RouteUpdateCheckTask] route update failed, msg={}", status.msg());
    }
}

//////////////////////////////////////////
// HeartBeatCheckTask
//////////////////////////////////////////

HeartBeatCheckTask::HeartBeatCheckTask(ServiceManager* service_manager) : service_manager_(service_manager) {}

void HeartBeatCheckTask::run() {
    if (service_manager_ == nullptr) {
        LOG_ERROR("[HeartBeatCheckTask] service manager is nullptr");
        return;
    }

    Status status = service_manager_->reconcile_nodes();
    if (status.fail()) {
        LOG_WARN("[HeartBeatCheckTask] heartbeat check run failed, msg={}", status.msg());
    }
}

}  // namespace adviskv::sdm