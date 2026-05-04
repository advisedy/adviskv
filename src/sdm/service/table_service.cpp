#include "sdm/service/table_service.h"

#include "common/define.h"
#include "common/status.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {

TableService::TableService(PlaceTableWorkflow* workflow) : workflow_(workflow) {}

Status TableService::place_table(const PlaceTableParam& param) {
    RETURN_IF_INVALID_PARAM(param)

    Table table{
        .table_id = param.table_id,
        .spec{
            .table_name = param.table_name,
            .db_name = param.db_name,
            .db_id = param.db_id,
            .replica_count = param.replica_count,
            .shard_count = param.shard_count,
            .resource_pool = param.resource_pool,
        },
        .state{
            .status = TableStatus::CREATEING,
            .lifecycle = TableLifecycle::CREATING,
        },
    };
    return workflow_->step(table);
}

}  // namespace adviskv::sdm