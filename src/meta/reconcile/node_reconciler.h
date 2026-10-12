#pragma once

#include <cstdint>

#include "common/status.h"
#include "meta/model/model.h"

namespace adviskv::meta {

class MetaStore;
class MetaStoreTxn;
class NodeService;

class NodeReconciler {
private:
    friend class NodeService;

    explicit NodeReconciler(MetaStore* store);
    NodeReconciler(MetaStore* store, int64_t start_ts_ms);

    Status run();
    Status check_and_modify_node(MetaStoreTxn& txn, Node& node);
    Status mark_node_offline(MetaStoreTxn& txn, Node& node);
    Status mark_node_suspect(MetaStoreTxn& txn, Node& node);
    Status mark_node_online(MetaStoreTxn& txn, Node& node);

    MetaStore* store_{nullptr};
    int64_t start_ts_ms_;
};

}  // namespace adviskv::meta
