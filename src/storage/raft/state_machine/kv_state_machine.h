#pragma once

#include <memory>

#include "common/type.h"
#include "storage/engine/kv_engine.h"
#include "storage/model/param.h"
#include "storage/raft/state_machine/state_machine.h"
namespace adviskv::storage {
/*

    virtual ~StateMachine() = default;

    virtual Status apply(const LogEntry& entry) = 0;
    virtual SnapshotPtr snapshot() const = 0;
    virtual Status restore(const SnapshotPtr& snap) = 0;
    virtual LogIndex apply_index() const = 0;
    virtual LogIndex apply_term() const = 0;
    virtual Status get(const Key& key, Value& value) const = 0;
*/

class KvStateMachine : public StateMachine {
   public:
    KvStateMachine(EngineType type);
    Status apply(const LogEntry& entry) override;
    SnapshotPtr snapshot() const override;
    Status restore(const SnapshotPtr& snap) override;
    LogIndex apply_index() const override;
    LogIndex apply_term() const override;
    Status get(const Key& key, Value& value) const override;

   private:
    std::unique_ptr<KVEngine> engine_;
    LogIndex apply_index_;
    Term apply_term_;
};

}  // namespace adviskv::storage