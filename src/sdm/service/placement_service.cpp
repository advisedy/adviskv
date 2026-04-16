// #include "sdm/service/placement_service.h"
// #include "common/define.h"
// #include "common/status.h"
// #include "common/type.h"
// #include "sdm/manager/meta_cache_manager.h"
// #include "sdm/manager/node_manager.h"
// #include "sdm/manager/route_manager.h"
// #include "sdm/operation/operation_factory.h"
// #include "sdm/operation/placetable_operation.h"
// namespace adviskv {

// PlacementService::PlacementService(OperationFactory *factorys,
//                                    MetaCacheManager *meta_cache_manager)
//     : factory_(factorys), meta_cache_manager_(meta_cache_manager) {}

// Status PlacementService::place_table(const PlaceTableParam &param) {
// //   PlaceTableOperation op = factory_->create_place_table_operation(param);
// //   return op.execute();

// }

// Status PlacementService::place_db(const PlaceDBParam &param) {
// //   if (!meta_cache_manager_) {
// //     return Status{StatusCode::ERROR, "meta_cache_manager is nullptr"};
// //   }
// //   Status status = meta_cache_manager_->update_db_meta(DBMetaCache{
// //       .db_name = param.db_name, .db_id = param.db_id, .zone =
// param.zone});
// //   return status;
//     return Status{StatusCode::NOT_SUPPORTED, ""};
// }

// } // namespace adviskv