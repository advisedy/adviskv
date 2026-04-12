#include "sdm/service/placement_service.h"
#include "common/define.h"
#include "common/status.h"
#include "common/type.h"
#include "sdm/manager/meta_cache_manager.h"
#include "sdm/manager/node_manager.h"
#include "sdm/manager/route_manager.h"
#include "sdm/operation/placetable_operation.h"
#include "sdm/operation/operation_factory.h"
namespace adviskv {

Status PlacementService::place_table(const PlaceTableParam &param,
                                     TableMetaCache *table_meta_cache) {

    PlaceTableOperation op;
    Status status = factory_->create_place_table_operation(param, op);
    RETURN_IF_INVALID_STATUS(status)
    status = op.execute();
    return status;

}

} // namespace adviskv