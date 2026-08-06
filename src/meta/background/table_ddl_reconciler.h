#pragma once

#include "common/background_task.h"
#include "meta/catalog/catalog_manager.h"
#include "meta/service/ddl_service.h"

namespace adviskv::meta {

class TableDdlReconciler : public BackgroundTask {
public:
    TableDdlReconciler(CatalogManager* catalog_manager, ITopoClient* topo_client);

protected:
    void run() override;

private:
    CatalogManager* catalog_manager_;
    ITopoClient* topo_client_;
};

}  // namespace adviskv::meta
