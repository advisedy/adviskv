#include "sdm/operation/placedb_operation.h"
#include "common/status.h"
#include "meta/catalog/catalog_manager.h"
#include "sdm/plan/placement_plan.h"

namespace adviskv {

/*
    Status execute() override;
    std::string get_name() const override;

private:

    Status build_plan(PlaceDBPlan& plan);
    Status commit_plan(const PlaceDBPlan& plan);
*/

std::string PlaceDBOperation::get_name() const { return "PlaceDBOperation"; }

Status PlaceDBOperation::execute() {
  PlaceDBPlan plan;
  Status status = build_plan(plan);
  RETURN_IF_INVALID_STATUS(status)
  RETURN_IF_INVALID_PLAN(plan)
  status = commit_plan(plan);
  return status;
}

Status PlaceDBOperation::build_plan(PlaceDBPlan &plan) {
  plan.db_id = param_.db_id;
  plan.db_name = param_.db_name;
  plan.zone = param_.zone;
  return Status::OK();
}

Status PlaceDBOperation::commit_plan(const PlaceDBPlan &plan) {
  DBMetaCache meta;
  meta.db_id = plan.db_id;
  meta.db_name = plan.db_name;
  meta.zone = plan.zone;
  Status status = deps_.meta_cache_manager->update_db_meta(meta);
  return status;
}

} // namespace adviskv