#pragma once

#include <optional>

#include "common/status.h"
#include "common/model/type.h"
#include "sdm/model/param.h"
#include "sdm/model/model.h"

namespace adviskv::sdm {

class SdmStore;
class SdmStoreTxn;
class NodeSelector;

struct ReplicaGroupReconcileContext {
    SdmStore* store{nullptr};
    NodeSelector* selector{nullptr};
};

inline bool is_healthy(const Replica& replica) {
    return replica.state.desired == ReplicaDesired::PRESENT &&
           replica.state.phase != ReplicaPhase::LOST &&
           replica.state.phase != ReplicaPhase::ERROR &&
           replica.state.phase != ReplicaPhase::DELETING &&
           replica.state.phase != ReplicaPhase::DELETED;
}

Status select_remove_member_victim(const SdmStoreTxn& txn,
                                   const ReplicaGroup& group,
                                   Optional<ReplicaID>& victim);

class ReplicaGroupService {
   public:
    explicit ReplicaGroupService(SdmStore* store,
                                 NodeSelector* selector = nullptr);

    Status reconcile_all();
    Status build_heartbeat_result(const HeartBeatParam& param,
                                  HeartBeatResult& result) const;

   private:
    SdmStore* store_{nullptr};
    NodeSelector* selector_{nullptr};
};

}  // namespace adviskv::sdm