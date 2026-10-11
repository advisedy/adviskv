#pragma once

#include <functional>
#include <utility>

#include "common/model/model.h"
#include "common/status.h"
#include "storage/model/model.h"
#include "storage/raft/raft_sender.h"
#include "storage/replica/fault_reporter.h"

namespace adviskv::storage {

class PersistEngine;
class ReplicaMessageDispatcher;

class RaftEffectExecutor {
public:
    RaftEffectExecutor(PersistEngine& persist, ReplicaMessageDispatcher& dispatcher, FaultReporter fault_reporter)
            : persist_(persist), dispatcher_(dispatcher), fault_reporter_(std::move(fault_reporter)) {}

    Status execute(const std::function<Status(RaftEffects&)>& step) const;

private:
    Status persist_effects(const RaftEffects& effects) const;

    PersistEngine& persist_;
    ReplicaMessageDispatcher& dispatcher_;
    FaultReporter fault_reporter_;
};

}
