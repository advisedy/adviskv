#pragma once

#include <cstdint>

namespace adviskv {

enum class SdmTableDesired : int32_t {
    PRESENT = 1,
    ABSENT = 2,
};

enum class SdmTablePhase : int32_t {
    CREATING = 1,
    READY = 2,
    DELETING = 3,
    DELETED = 4,
    FAILED = 5,
    RESIZING = 6,
};

}  // namespace adviskv