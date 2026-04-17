#include "sdm/service/heartbeat_service.h"

#include "common/define.h"
#include "sdm/model/service_param.h"

namespace adviskv::sdm {

Status HeartBeatService::heartbeat(const HeartBeatParam& param, HeartBeatRes& res) {
    RETURN_IF_INVALID_PARAM(param)

    // 接收到了storage侧传来的自己目前的状态和携带的replca_info
    // 首先需要更新到sdm_store
    
    // 然后得构造

}

}  // namespace adviskv::sdm