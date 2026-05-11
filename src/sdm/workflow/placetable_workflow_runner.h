#pragma once

#include <vector>

#include "common/background_task.h"
#include "common/type.h"
#include "sdm/client/storage_client.h"
#include "sdm/model/sdm_store.h"
#include "sdm/selector/node_selector/node_selector.h"
#include "sdm/workflow/placetable_workflow.h"

namespace adviskv::sdm {

class PlaceTableWorkflowRunner : public BackgroundTask {
   public:
    PlaceTableWorkflowRunner(SdmStore* store, IStorageClient* client,
                             NodeSelector* selector);

   protected:
    void run() override;

   private:
    SdmStore* store_;
    std::unique_ptr<PlaceTableWorkflow> workflow_;
    std::vector<TableLifecycle> need_run_lifecycles_;
};

}  // namespace adviskv::sdm
