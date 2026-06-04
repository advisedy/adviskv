#pragma once

#include <unistd.h>

#include <cstdlib>
#include <cstring>

namespace adviskv::testhook {

inline bool crash_point_enabled(const char* name) {
#ifdef ADVISKV_ENABLE_CRASH_TEST
    if (!name) return false;
    const char* target = std::getenv("ADVISKV_ENABLE_CRASH_POINT");
    return target != nullptr && std::strcmp(name, target) == 0;
#else
    (void)name;
    return false;
#endif
}

inline void crash_point(const char* name) {
#ifdef ADVISKV_ENABLE_CRASH_TEST
    if (!name) {
        return;
    }
    if (crash_point_enabled(name)) {
        ::_exit(137);
    }
#else
    (void)name;
#endif
}

}  // namespace adviskv::testhook