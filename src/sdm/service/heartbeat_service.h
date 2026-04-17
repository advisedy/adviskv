#pragma once

#include "common/status.h"
#include "sdm/model/service_param.h"
#include "sdm/model/sdm_store.h"

namespace adviskv::sdm{

class HeartBeatService{

public:
    explicit HeartBeatService(SdmStore* sdm_store);

    Status heartbeat(const HeartBeatParam& param, HeartBeatRes &res);

private:
    SdmStore* sdm_store_;
};
}