#pragma once

#include "common/type.h"
#include "storage/engine/kv_engine.h"
#include <map>
#include <shared_mutex>
namespace adviskv{

class MapEngine: public KVEngine{

public:
    MapEngine()=default;
    ~MapEngine()=default;

    virtual Status put(KeyView key, ValueView value) override;
    virtual Status get(KeyView key, Value& value) override;
    virtual Status del(KeyView key) override;

private:
    std::shared_mutex map_mutex_;
    std::map<Key, Value> map_;
};

}