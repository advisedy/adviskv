#include "sdm/service/table_service.h"
#include "common/define.h"
#include "sdm/model/store.h"

namespace adviskv::sdm{

Status TableService::place_table(const PlaceTableParam& param){
    Table table{
        .table_id = param.table_id,
        .spec.table_name = param.table_name,
        .spec.db_name = param.db_name,
        .spec.db_id = param.db_id,
        .spec.replica_count = param.replica_count,
        .spec.shard_count = param.shard_count,
        .spec.resource_pool = param.resource_pool,
    };
    Status status = sdm_store->put_table(table);
    RETURN_IF_INVALID_STATUS(status)
    return status;
}


}