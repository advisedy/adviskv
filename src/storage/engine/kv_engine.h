#pragma once

#include "common/common.h"
#include "common/status.h"
#include "common/type.h"
#include <string_view>

namespace adviskv{

class KVEngine{
public:
    KVEngine() = default;
    virtual ~KVEngine() = default;

    virtual Status put(const Key& key, const Value& value) = 0;
    virtual Status get(const Key& key, Value& value) = 0;
    virtual Status del(const Key& key) = 0;
private:
    
};
}