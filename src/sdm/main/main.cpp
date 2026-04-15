#include <algorithm>
#include <iostream>
#include <memory>

#include "sdm/manager/meta_cache_manager.h"
#include "sdm/manager/node_manager.h"
#include "sdm/manager/route_manager.h"

#include "sdm/model/sdm_store.h"
#include "sdm/operation/operation_factory.h"
#include "sdm/selector/leader_selector/leader_selector.h"
#include "sdm/selector/node_selector/node_selector.h"
#include "sdm/service/node_service.h"
#include "sdm/service/placement_service.h"
#include "sdm/service/route_service.h"


int main() {
using namespace adviskv;

    auto meta_cache_manager = std::make_unique<MetaCacheManager>();
    auto node_manager = std::make_unique<NodeManager>();
    auto route_manager = std::make_unique<RouteManager>();
    
    auto node_selector = std::make_unique<DefaultNodeSelector>();
    auto leader_selector = std::make_unique<DefaultLeaderSelector>();
    
    auto sdm_store = std::make_unique<sdm::SdmStore>();

    OperationFactoryDeps operation_factory_deps{
        meta_cache_manager.get(),
        node_manager.get(),
        route_manager.get(),
        node_selector.get(),
        leader_selector.get(),
    };
    auto operation_factory = std::make_unique<OperationFactory>(operation_factory_deps);
    
    auto route_service = std::make_unique<sdm::RouteService>(sdm_store.get());
    auto node_service = std::make_unique<sdm::NodeService>(sdm_store.get());
    auto placement_service = std::make_unique<PlacementService>(operation_factory.get(), meta_cache_manager.get());
    

  std::cout << "Hello, AdvisKV!" << std::endl;
  return 0;
}