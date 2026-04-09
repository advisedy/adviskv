#include "storage/engine/map_engine.h"
#include "common/log.h"
#include "common/status.h"
#include "common/type.h"
#include <fmt/format.h>
#include <mutex>
#include <shared_mutex>

namespace adviskv{

Status MapEngine::put(KeyView key, ValueView value){
    
    std::scoped_lock lock(map_mutex_);

    map_[Key(key)] = Value(value);

    DEBUG("map put ok, key = {}, value = {}", key, value);

    return Status::OK();
}

Status MapEngine::get(KeyView key, Value& value){
    std::shared_lock lock(map_mutex_);
    if(!map_.count(Key(key))){
        DEBUG("key = {}, not found", key);
        return Status{StatusCode::KEY_NOT_FOUND, fmt::format( "key = {} not found", key)};
    }
    value = map_[Key(key)];
    return Status::OK();
}

Status MapEngine::del(KeyView key) {

}


}