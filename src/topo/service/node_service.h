#pragma once

#include <cstdint>

#include "common/model/type.h"
#include "common/status.h"
#include "topo/model/model.h"
#include "topo/model/param.h"

namespace adviskv::topo {

class TopoStore;
class TopoStoreTxn;

class NodeService {
public:
    static constexpr int64_t HEARTBEAT_SUSPECT_TIMEOUT_MS = 10 * 1000;
    static constexpr int64_t HEARTBEAT_OFFLINE_TIMEOUT_MS = 30 * 1000;
    static constexpr int64_t HEARTBEAT_STARTUP_GRACE_MS = 30 * 1000;

    explicit NodeService(TopoStore* store);

    Status register_node(const RegisterNodeParam& param);
    Status heartbeat(const HeartBeatParam& param);

    Status reconcile_all();

    Status check_and_modify_node_for_test(Node& node);
    void set_start_ts_ms_for_test(int64_t ts) { start_ts_ms_ = ts; }

private:
    Status update_node_heartbeat(TopoStoreTxn& txn, const HeartBeatParam& param);
    Status apply_reported_replicas(TopoStoreTxn& txn, const HeartBeatParam& param);
    Status mark_no_exist_replicas(TopoStoreTxn& txn, const HeartBeatParam& param);
    Status check_and_modify_node(TopoStoreTxn& txn, Node& node);
    Status mark_node_offline(TopoStoreTxn& txn, Node& node);
    Status mark_node_suspect(TopoStoreTxn& txn, Node& node);
    Status mark_node_online(TopoStoreTxn& txn, Node& node);

    TopoStore* store_{nullptr};
    int64_t start_ts_ms_;
};

}  // namespace adviskv::topo