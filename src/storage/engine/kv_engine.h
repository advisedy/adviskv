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

    virtual Status put(KeyView key, ValueView value) = 0;
    virtual Status get(KeyView key, Value& value) = 0;
    virtual Status del(KeyView key) = 0;
private:
    
};
}