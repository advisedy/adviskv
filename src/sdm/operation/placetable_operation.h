#pragma once

#include "sdm/plan/placement_plan.h"
#include "sdm/service/placement_service.h"
#include "sdm/operation/ioperation.h"
#include "sdm/operation/operation_deps.h"
#include "sdm/plan/placement_plan.h"

namespace adviskv{

class PlaceTableOperation : public IOperation{

public:
    PlaceTableOperation() = default;
    // PlaceTableOperation(const PlaceTableParam& param, const PlaceTableOperationDeps& deps):param_(param), deps_(deps){}
    Status init(const PlaceTableParam& param, const PlaceTableOperationDeps& deps){
        deps_ = deps;
        param_ = param;
        return Status::OK();
    }
    Status execute() override;
    std::string get_name() const override;

private:

    Status build_plan(PlaceTablePlan& plan);
    Status commit_plan(const PlaceTablePlan& plan);

    PlaceTableOperationDeps deps_;
    PlaceTableParam param_;

};


}