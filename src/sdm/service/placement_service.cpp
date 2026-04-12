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

PlacementServiceDeps PlacementService::get_deps() const{
    return factory_->get_deps();
}


Status PlacementService::place_table(const PlaceTableParam &param,
                                     TableMetaCache *table_meta_cache) {
    PlaceTableOperation op = factory_->create_place_table_operation(param);
    return op.execute();

}

Status PlacementService::place_db(const PlaceDBParam& param, DBMetaCache* db_meta_cache){

    Status status = get_deps().meta_cache_manager->update_db_meta(DBMetaCache{.db_name = param.db_name, .db_id = param.db_id, .zone = param.zone});
     return status;

}


} // namespace adviskv