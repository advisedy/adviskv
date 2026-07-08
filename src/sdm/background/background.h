#pragma once

#include "common/background_task.h"
#include "common/status.h"

namespace adviskv::sdm {

class ServiceManager;

class TableReconcileTask : public BackgroundTask {
public:
    explicit TableReconcileTask(ServiceManager* service_manager);

    Status reconcile_once();

protected:
    void run() override;

private:
    ServiceManager* service_manager_{nullptr};
};

class ReplicaGroupReconcileTask : public BackgroundTask {
public:
    explicit ReplicaGroupReconcileTask(ServiceManager* service_manager);

protected:
    void run() override;

private:
    ServiceManager* service_manager_{nullptr};
};

class RouteUpdateCheckTask : public BackgroundTask {
public:
    explicit RouteUpdateCheckTask(ServiceManager* service_manager);

protected:
    void run() override;

private:
    ServiceManager* service_manager_{nullptr};
};

class HeartBeatCheckTask : public BackgroundTask {
public:
    explicit HeartBeatCheckTask(ServiceManager* service_manager);

protected:
    void run() override;

private:
    ServiceManager* service_manager_{nullptr};
};

}  // namespace adviskv::sdm