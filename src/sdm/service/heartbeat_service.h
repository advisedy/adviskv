#pragma once

#include "common/status.h"
#include "sdm/model/service_param.h"

namespace adviskv::sdm {

class SdmStore;

class HeartBeatService {
   public:
    explicit HeartBeatService(SdmStore* store);

    Status heartbeat(const HeartBeatParam& param);

   private:
    Status update_node_state(const HeartBeatParam& param);
    Status apply_reported_replicas(const HeartBeatParam& param);

    SdmStore* store_{nullptr};
};

}  // namespace adviskv::sdm