// #pragma once

// // #include "sdm/manager/meta_cache_manager.h"
// // #include "sdm/manager/node_manager.h"
// // #include "sdm/manager/route_manager.h"
// // #include "sdm/selector/leader_selector/leader_selector.h"
// // #include "sdm/selector/node_selector/node_selector.h"
// #include "common/define.h"
// #include "common/status.h"
// #include "sdm/operation/placetable_operation.h"

// #include "sdm/operation/operation_deps.h"
// #include <optional>

// namespace adviskv{

// class OperationFactory{

// public:
//     explicit OperationFactory(OperationFactoryDeps deps):deps_(deps){}

//     PlaceTableOperation create_place_table_operation(const PlaceTableParam&
//     param){
//         return PlaceTableOperation{param,
//         build_place_table_operation_deps()};
//     }

// private:

//     PlaceTableOperationDeps build_place_table_operation_deps(){
//         PlaceTableOperationDeps deps;
//         deps.leader_selector = deps_.leader_selector;
//         deps.node_selector = deps_.node_selector;
//         deps.meta_cache_manager = deps_.meta_cache_manager;
//         deps.route_manager = deps_.route_manager;
//         deps.node_manager = deps_.node_manager;
//         return deps;
//     }

// private:
//     OperationFactoryDeps deps_;

// };

// }