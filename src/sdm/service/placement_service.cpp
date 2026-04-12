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


PlacementService::PlacementService(OperationFactory* factorys):factory_(factorys){


}


Status PlacementService::place_table(const PlaceTableParam &param,
                                     TableMetaCache *table_meta_cache) {
    PlaceTableOperation op = factory_->create_place_table_operation(param);
    return op.execute();

}

Status PlacementService::place_db(const PlaceDBParam& param, DBMetaCache* db_meta_cache){
    PlaceDBOperation op = factory_->create_place_db_operation(param);
    return op.execute();
}


} // namespace adviskv