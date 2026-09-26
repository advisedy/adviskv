#pragma once

#include "e2e_case_util.h"
#include "e2e_options.h"

namespace adviskv::e2e {

// Meta 崩溃恢复场景：建表写入后重启 Meta，再验证旧数据和新写入。
inline bool run_meta_crash_seed_case(const Options& options) {
    if (options.key_count <= 0) {
        return false;
    }
    return run_seed_case(options, "meta-crash");
}

inline bool run_meta_crash_verify_case(const Options& options) {
    if (options.key_count <= 0) {
        return false;
    }
    return run_verify_case(options, "meta-crash", "after-recovery");
}

}  // namespace adviskv::e2e
