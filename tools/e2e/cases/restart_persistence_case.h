#pragma once

#include "e2e_assert.h"
#include "e2e_kv_util.h"
#include "e2e_options.h"
namespace adviskv::e2e {

// 这个测试场景就是先写入key之后，然后崩溃，然后
// 检验之前的持久化是否没有问题，并且接着写入key，检测一下是否没有问题。
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