#pragma once

#include <optional>

#include "common/model/type.h"
#include "common/status.h"
#include "topo/model/model.h"
#include "topo/model/param.h"

namespace adviskv::topo {

class TopoStore;
class TopoStoreTxn;
class NodeSelector;

struct ReplicaGroupReconcileContext {
    TopoStore* store{nullptr};
    NodeSelector* selector{nullptr};
};

inline bool is_healthy(const Replica& replica) {
    return replica.state.desired == ReplicaDesired::PRESENT && replica.state.phase != ReplicaPhase::LOST &&
           replica.state.phase != ReplicaPhase::ERROR && replica.state.phase != ReplicaPhase::DELETING &&
           replica.state.phase != ReplicaPhase::DELETED;
}

Status select_remove_member_victim(const TopoStoreTxn& txn, const ReplicaGroup& group, Optional<ReplicaID>& victim);

class ReplicaGroupService {
public:
    explicit ReplicaGroupService(TopoStore* store, NodeSelector* selector = nullptr);

    Status reconcile_all();
    Status build_heartbeat_result(const HeartBeatParam& param, HeartBeatResult& result) const;

private:
    TopoStore* store_{nullptr};
    NodeSelector* selector_{nullptr};
};

}  // namespace adviskv::topo