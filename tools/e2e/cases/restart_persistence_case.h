#pragma once

#include "e2e_case_util.h"
#include "e2e_options.h"
namespace adviskv::e2e {

// 这个场景验证整集群重启后的持久化恢复。
// 正常的创建db，table，写入key，然后
// 全部都崩溃之后，检测是否可以继续操作
inline bool run_restart_persistence_seed_case(const Options& options) {
    if (options.key_count <= 0) {
        return false;
    }
    return run_seed_case(options, "restart_persistence");
}
inline bool run_restart_persistence_verify_case(const Options& options) {
    if (options.key_count <= 0) {
        return false;
    }

    if (!run_verify_case(options, "restart_persistence", "after")) {
        return false;
    }

    return true;
}

}  // namespace adviskv::e2e