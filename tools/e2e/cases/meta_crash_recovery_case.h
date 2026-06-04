#pragma once

#include "e2e_case_util.h"
#include "e2e_options.h"

namespace adviskv::e2e {

// Meta 崩溃恢复场景：
// 1. seed: 创建 DB + table，写入一批 KV
// 2. Python 侧 kill Meta 进程，再重新启动
// 3. verify: 等待 table 恢复正常，验证之前写入的数据仍然可读，
//    再写入一条新数据并验证可读

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

    if (!run_verify_case(options, "meta-crash", "after-recovery")) {
        return false;
    }

    return true;
}

}  // namespace adviskv::e2e