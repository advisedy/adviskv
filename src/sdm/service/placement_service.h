#pragma once

#include "common/status.h"
#include "sdm/model/service_param.h"
#include "sdm/operation/operation_deps.h"

namespace adviskv {

class OperationFactory;

using PlacementServiceDeps = OperationFactoryDeps;


class PlacementService {

public:
  explicit PlacementService(OperationFactory *factorys);

  Status place_table(const PlaceTableParam &param);
  Status place_db(const PlaceDBParam &param);

private:
  PlacementServiceDeps get_deps() const;

  OperationFactory *factory_;
};

} // namespace adviskv