#include "sdm/workflow/placetable_workflow_runner.h"

#include "common/log.h"
#include "common/status.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {

PlaceTableWorkflowRunner::PlaceTableWorkflowRunner(SdmStore* store,
                                                   IStorageClient* client,
                                                   NodeSelector* selector)
    : store_(store),
      workflow_(std::make_unique<PlaceTableWorkflow>(store, client, selector)),
      need_run_lifecycles_(
          {TableLifecycle::CREATING, TableLifecycle::PLACING,
           TableLifecycle::CREATING_REPLICAS, TableLifecycle::WAITING_READY,
           TableLifecycle::WAITING_ROUTE_READY, TableLifecycle::ROLLING_BACK}) {
}

void PlaceTableWorkflowRunner::run() {
    for (auto lc : need_run_lifecycles_) {
        std::vector<TablePtr> tables;
        Status status = store_->list_tables_by_lifecycle(lc, tables);
        if (status.fail()) {
            LOG_WARN("runner list_tables_by_lifecycle failed, msg={}",
                     status.msg());
            continue;
        }

        for (auto& table : tables) {
            status = workflow_->step(*table);
            if (status.fail()) {
                LOG_WARN("runner step failed for table={}, msg={}",
                         table->table_id, status.msg());
            }
        }
    }
}

}  // namespace adviskv::sdm
