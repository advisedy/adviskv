#pragma once

#include <cstdint>

namespace adviskv::storage {

enum class ReplicaStatus : int8_t {
    INITIALIZING = 0,
    RECOVERING = 1,
    READY = 2,
    FAULTED = 3,
};

}  // namespace adviskv::storage