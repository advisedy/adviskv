#pragma once

#include "common/status.h"

namespace adviskv{

class IPlan{
    public:
    virtual ~IPlan() = default;

    virtual Status validate() const = 0;
};

}