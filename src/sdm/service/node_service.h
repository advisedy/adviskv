#pragma once

#include <cstdint>

#include "common/status.h"
#include "common/type.h"
#include "sdm/model/service_param.h"
#include "sdm/model/store.h"

namespace adviskv::sdm {

class SdmStore;
class SdmStoreTxn;

class NodeService {
   public:
    static constexpr int64_t HEARTBEAT_SUSPECT_TIMEOUT_MS = 10 * 1000;
    static constexpr int64_t HEARTBEAT_OFFLINE_TIMEOUT_MS = 30 * 1000;
    static constexpr int64_t HEARTBEAT_STARTUP_GRACE_MS = 30 * 1000;

    explicit NodeService(SdmStore* store);

    Status register_node(const RegisterNodeParam& param);
    Status heartbeat(const HeartBeatParam& param);

    Status reconcile_all();

    Status check_and_modify_node_for_test(Node& node);
    void set_start_ts_ms_for_test(int64_t ts) { start_ts_ms_ = ts; }

   private:
    Status update_node_heartbeat(SdmStoreTxn& txn, const HeartBeatParam& param);
    Status apply_reported_replicas(SdmStoreTxn& txn,
                                   const HeartBeatParam& param);
    Status mark_deleted_replicas(SdmStoreTxn& txn, const HeartBeatParam& param);
    Status check_and_modify_node(SdmStoreTxn& txn, Node& node);
    Status mark_node_offline(SdmStoreTxn& txn, Node& node);
    Status mark_node_suspect(SdmStoreTxn& txn, Node& node);
    Status mark_node_online(SdmStoreTxn& txn, Node& node);

    SdmStore* store_{nullptr};
    int64_t start_ts_ms_;
};

}  // namespace adviskv::sdm