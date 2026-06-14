#pragma once
#include <functional>
#include <memory>

#include "common/status.h"
#include "common/type.h"
#include "storage/model/param.h"

namespace adviskv::storage {

struct Snapshot {
    LogIndex apply_index{0};
    Term apply_term{0};
    // std::string path;
};
using SnapshotPtr = std::shared_ptr<Snapshot>;

using KvVisitor = std::function<Status(const Key&, const Value&)>;
using KvForEach = std::function<Status(const KvVisitor&)>;

class StateMachine {
   public:
    virtual ~StateMachine() = default;

    virtual Status apply(const LogEntry& entry) = 0;
    virtual Status restore(const SnapshotPtr& snap,
                           const KvForEach& for_each_kv) = 0;
    virtual LogIndex apply_index() const = 0;
    virtual LogIndex apply_term() const = 0;
    virtual Status get(const Key& key, Value& value) const = 0;

    // 状态机给外部提供的一个接口，传递一个函数，可以操作状态机内部持有的KV
    virtual Status for_each_kv(
        const std::function<Status(const Key&, const Value&)>& fn) const = 0;
};

}  // namespace adviskv::storage