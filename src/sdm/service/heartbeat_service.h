#pragma once

#include "common/status.h"
#include "sdm/model/service_param.h"
#include "sdm/model/sdm_store.h"

namespace adviskv::sdm {

class HeartBeatService {
public:
    explicit HeartBeatService(SdmStore* sdm_store);

    Status heartbeat(const HeartBeatParam& param);

private:
    Status update_node_state(const HeartBeatParam& param);
    Status apply_reported_replicas(const HeartBeatParam& param);
    // Status build_desired_replicas(const NodeID& node_id, HeartBeatResult* result);

private:
    SdmStore* sdm_store_{nullptr};
};

}  // namespace adviskv::sdm