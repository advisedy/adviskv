#pragma once

#include <functional>
#include <map>
#include <shared_mutex>

#include "common/type.h"
#include "storage/engine/kv_engine.h"

namespace adviskv::storage {

class MapEngine : public KVEngine {
   public:
    MapEngine() = default;
    ~MapEngine() = default;

    Status put(const Key& key, const Value& value) override;
    Status get(const Key& key, Value& value) override;
    Status del(const Key& key) override;
    std::vector<KV> dump_all() const override;
    Status clear() override;

    Status for_each_kv(const std::function<Status(const Key&, const Value&)>&
                           fn) const override;

   private:
    mutable std::shared_mutex map_mutex_;
    std::map<Key, Value> map_;
};

}  // namespace adviskv::storage