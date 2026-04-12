#pragma once

#include "common/status.h"

namespace adviskv{

class IOperation{
public:
    virtual ~IOperation() = default;
    virtual Status execute() = 0;
    virtual std::string get_name() const = 0;
};

}