#pragma once

#include <functional>
#include <string_view>

#include "common/common.h"
#include "common/status.h"
#include "common/type.h"

namespace adviskv::storage {

class KVEngine {
   public:
    KVEngine() = default;
    virtual ~KVEngine() = default;

    virtual Status put(const Key& key, const Value& value) = 0;
    virtual Status get(const Key& key, Value& value) = 0;
    virtual Status del(const Key& key) = 0;
    virtual Status clear() = 0;
    virtual std::vector<KV> dump_all() const = 0;

    // Iterate all KVs in the engine without materializing a full dump.
    virtual Status for_each_kv(
        const std::function<Status(const Key&, const Value&)>& fn) const = 0;

   private:
};
}  // namespace adviskv::storage
