#pragma once

#include "common/types.h"

namespace adviskv {

enum class EngineType : int8 {
    MAP = 0,
    ROCKSDB = 1,
};

}  // namespace adviskv