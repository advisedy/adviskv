#pragma once

#include "common/background_task.h"
#include "catalog/store/catalog_store.h"
#include "catalog/service/ddl_service.h"

namespace adviskv::catalog {

class TableReconciler : public BackgroundTask {
public:
    TableReconciler(CatalogStore* catalog_store, ITopoClient* topo_client);

protected:
    void run() override;

private:
    CatalogStore* catalog_store_;
    ITopoClient* topo_client_;
};

}  // namespace adviskv::catalog
