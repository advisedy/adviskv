#pragma once

#include "sdm/plan/placement_plan.h"
#include "sdm/model/service_param.h"

#include "sdm/operation/ioperation.h"
#include "sdm/operation/operation_deps.h"
#include "sdm/plan/placement_plan.h"

namespace adviskv{

class PlaceTableOperation : public IOperation{

public:
    PlaceTableOperation(const PlaceTableParam& param, const PlaceTableOperationDeps& deps):param_(param), deps_(deps){}
    Status execute() override;
    std::string get_name() const override;

private:

    Status build_plan(PlaceTablePlan& plan);
    Status commit_plan(const PlaceTablePlan& plan);

    PlaceTableOperationDeps deps_;
    PlaceTableParam param_;

};


}