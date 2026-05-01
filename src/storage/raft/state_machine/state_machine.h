#pragma once
#include <memory>
#include <utility>
#include <vector>

#include "common/status.h"
#include "common/type.h"
#include "storage/model/param.h"

namespace adviskv::storage {

struct Snapshot {
    LogIndex apply_index;
    Term apply_term;
    std::vector<std::pair<Key, Value>> kvs;
};
using SnapshotPtr = std::shared_ptr<Snapshot>;

class StateMachine {
   public:
    virtual ~StateMachine() = default;

    virtual Status apply(const LogEntry& entry) = 0;
    virtual SnapshotPtr snapshot() const = 0;
    virtual Status restore(const SnapshotPtr& snap) = 0;
    virtual LogIndex apply_index() const = 0;
    virtual LogIndex apply_term() const = 0;
    virtual Status get(const Key& key, Value& value) const = 0;
};

}  // namespace adviskv::storage