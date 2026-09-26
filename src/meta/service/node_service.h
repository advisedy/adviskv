#pragma once

#include <cstdint>

#include "common/model/type.h"
#include "common/status.h"
#include "meta/model/model.h"
#include "meta/model/param.h"

namespace adviskv::meta {

class MetaStore;
class MetaStoreTxn;

class NodeService {
public:
    static constexpr int64_t HEARTBEAT_SUSPECT_TIMEOUT_MS = 10 * 1000;
    static constexpr int64_t HEARTBEAT_OFFLINE_TIMEOUT_MS = 30 * 1000;
    static constexpr int64_t HEARTBEAT_STARTUP_GRACE_MS = 30 * 1000;

    explicit NodeService(MetaStore* store);

    Status register_node(const RegisterNodeParam& param);
    Status heartbeat(const HeartBeatParam& param);

    Status reconcile_all();

    Status check_and_modify_node_for_test(Node& node);
    void set_start_ts_ms_for_test(int64_t ts) { start_ts_ms_ = ts; }

private:
    Status update_node_heartbeat(MetaStoreTxn& txn, const HeartBeatParam& param);
    Status apply_reported_replicas(MetaStoreTxn& txn, const HeartBeatParam& param);
    Status mark_no_exist_replicas(MetaStoreTxn& txn, const HeartBeatParam& param);
    Status check_and_modify_node(MetaStoreTxn& txn, Node& node);
    Status mark_node_offline(MetaStoreTxn& txn, Node& node);
    Status mark_node_suspect(MetaStoreTxn& txn, Node& node);
    Status mark_node_online(MetaStoreTxn& txn, Node& node);

    MetaStore* store_{nullptr};
    int64_t start_ts_ms_;
};

}  // namespace adviskv::meta