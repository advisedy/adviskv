#include "storage/engine/map_engine.h"

#include <fmt/format.h>

#include <mutex>
#include <shared_mutex>

#include "common/log.h"
#include "common/status.h"
#include "common/type.h"

namespace adviskv::storage {

Status MapEngine::put(const Key& key, const Value& value) {
  std::unique_lock lock(map_mutex_);

  map_[key] = value;

  DEBUG("map put ok, key = {}, value = {}", key, value);

  return Status::OK();
}

Status MapEngine::get(const Key& key, Value& value) {
  std::shared_lock lock(map_mutex_);
  if (!map_.count(key)) {
    DEBUG("key = {}, not found", key);
    return Status{StatusCode::KEY_NOT_FOUND,
                  fmt::format("key = {} not found", key)};
  }
  value = map_[Key(key)];
  return Status::OK();
}

Status MapEngine::del(const Key& key) {
  return Status{StatusCode::NOT_SUPPORTED,
                "del operation is not supported in MapEngine"};
}

}  // namespace adviskv