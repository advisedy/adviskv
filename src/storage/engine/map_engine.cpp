#include "storage/engine/map_engine.h"

#include <fmt/format.h>

#include <mutex>
#include <shared_mutex>
#include <utility>
#include <vector>

#include "common/log.h"
#include "common/status.h"
#include "common/model/type.h"

namespace adviskv::storage {

Status MapEngine::put(const Key& key, const Value& value) {
    std::unique_lock lock(map_mutex_);

    map_[key] = value;

    LOG_DEBUG("map put ok, key = {}, value = {}", key, value);

    return Status::OK();
}

Status MapEngine::get(const Key& key, Value& value) {
    std::shared_lock lock(map_mutex_);
    if (!map_.count(key)) {
        LOG_DEBUG("key = {}, not found", key);
        return Status{StatusCode::KEY_NOT_FOUND,
                      fmt::format("key = {} not found", key)};
    }
    value = map_[Key(key)];
    return Status::OK();
}

Status MapEngine::del(const Key& key) {
    std::unique_lock lock(map_mutex_);
    if (!map_.count(key)) return Status::OK();
    map_.erase(key);
    return Status::OK();
}

std::vector<KV> MapEngine::dump_all() const {
    std::shared_lock lock(map_mutex_);
    std::vector<KV> kvs;
    kvs.reserve(map_.size());
    for (const auto& [k, v] : map_) {
        KV kv{k, v};
        kvs.emplace_back(std::move(kv));
    }
    return kvs;
}

Status MapEngine::clear() {
    std::unique_lock lock(map_mutex_);
    map_.clear();
    return Status::OK();
}

Status MapEngine::for_each_kv(
    const std::function<Status(const Key&, const Value&)>& fn) const {
    std::shared_lock lock(map_mutex_);
    for (const auto& [k, v] : map_) {
        Status st = fn(k, v);
        if (st.fail()) {
            return st;
        }
    }
    return Status::OK();
}

}  // namespace adviskv::storage