#pragma once

#include "common/background_task.h"
#include "meta/catalog/catalog_manager.h"
#include "meta/service/ddl_service.h"

namespace adviskv::meta {


class TableDdlReconciler : public BackgroundTask {
   public:
    TableDdlReconciler(CatalogManager* catalog_manager,
                       ISdmClient* sdm_client);

   protected:
    void run() override;

   private:
    CatalogManager* catalog_manager_;
    ISdmClient* sdm_client_;
};

}  // namespace adviskv::meta
