#pragma once

#include "common/model/model.h"
#include "common/status.h"
#include "meta/model/model.h"
#include "meta/model/param.h"
#include "meta/reconcile/node_reconciler.h"

namespace adviskv::meta {

class MetaStore;
class MetaStoreTxn;

class NodeService {
public:
    explicit NodeService(MetaStore* store);
    NodeService(MetaStore* store, int64_t start_ts_ms);

    Status register_node(const RegisterNodeParam& param);
    Status heartbeat(const HeartBeatParam& param);
    Status reconcile_all();

private:
    Status update_node_heartbeat(MetaStoreTxn& txn, const HeartBeatParam& param);
    Status apply_reported_replicas(MetaStoreTxn& txn, const HeartBeatParam& param);
    Status mark_no_exist_replicas(MetaStoreTxn& txn, const HeartBeatParam& param);

    MetaStore* store_{nullptr};
    NodeReconciler reconciler_;
};

}  // namespace adviskv::meta
