#pragma once

#include "sdm/plan/placement_plan.h"
#include "sdm/service/placement_service.h"
#include "sdm/operation/ioperation.h"
#include "sdm/operation/operation_deps.h"
#include "sdm/plan/placement_plan.h"

namespace adviskv{

class PlaceDBOperation : public IOperation{

public:

    PlaceDBOperation(const PlaceDBParam& param, const PlaceDBOperationDeps& deps):param_(param), deps_(deps){}
    Status execute() override;
    std::string get_name() const override;

private:

    Status build_plan(PlaceDBPlan& plan);
    Status commit_plan(const PlaceDBPlan& plan);

    PlaceDBOperationDeps deps_;
    PlaceDBParam param_;

};


}