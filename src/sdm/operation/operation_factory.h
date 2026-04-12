#pragma once

// #include "sdm/manager/meta_cache_manager.h"
// #include "sdm/manager/node_manager.h"
// #include "sdm/manager/route_manager.h"
// #include "sdm/selector/leader_selector/leader_selector.h"
// #include "sdm/selector/node_selector/node_selector.h"
#include "common/define.h"
#include "common/status.h"
#include "sdm/operation/placetable_operation.h"
#include "sdm/service/placement_service.h"
#include "sdm/operation/operation_deps.h"

namespace adviskv{




#define DEFINE_OPERATION(OpType, name, ParamType) \
    public:     \
        OpType create_##name(const ParamType& param); \
    private:    \
        Optype##Deps build_##name##_deps();

class OperationFactory{

public:
    explicit OperationFactory(OperationFactoryDeps deps):deps_(deps){}

    Status create_place_table_operation(const PlaceTableParam& param, PlaceTableOperation& op){
        RETURN_IF_INVALID_PARAM(param)
        // op = PlaceTableOperation{param, build_place_table_operation_deps()};
        Status status = op.init(param, build_place_table_operation_deps());
        return status;
    }

private:

    PlaceTableOperationDeps build_place_table_operation_deps(){
        PlaceTableOperationDeps deps;
        deps.leader_selector = deps_.leader_selector;
        deps.node_selector = deps_.node_selector;
        deps.meta_cache_manager = deps_.meta_cache_manager;
        deps.route_manager = deps_.route_manager;
        deps.node_manager = deps_.node_manager;
        return deps;
    }



private:
    OperationFactoryDeps deps_;

};


}