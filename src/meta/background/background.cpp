#include "meta/background/background.h"

#include "common/log.h"
#include "meta/service/service_manager.h"

namespace adviskv::meta {

MetaReconcileTask::MetaReconcileTask(ServiceManager* service_manager) : service_manager_(service_manager) {}

void MetaReconcileTask::run() { reconcile_once(); }

void MetaReconcileTask::reconcile_once() {
    if (service_manager_ == nullptr) {
        LOG_ERROR("[MetaReconcileTask] service manager is nullptr");
        return;
    }
    service_manager_->tick();
}

}  // namespace adviskv::meta
