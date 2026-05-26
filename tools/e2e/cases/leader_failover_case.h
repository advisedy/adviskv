#pragma once

#include "e2e_assert.h"
#include "e2e_kv_util.h"
#include "e2e_options.h"

namespace adviskv::e2e {

// 当前 storage leader 进程退出后，route 应切到新
// leader，旧数据仍可读且新写可用。
/*
输出出来leader的ip和port之后，被python那边捕获，然后通过prot去停掉leader
*/
inline bool run_leader_failover_seed_case(const Options& options) {
    // TODO 要在这里输出一个title
    const bool ok = run_seed_case(options, "leader-failover", true);
    if (ok) {
        print_pass_bold("e2e leader failover seed passed");
    }
    return ok;
}

inline bool run_leader_failover_verify_case(const Options& options) {
    // TODO 要在这里输出一个title

    const bool ok =
        run_verify_case(options, "leader-failover", "after-failover");
    if (ok) {
        print_pass_bold("e2e leader failover verify passed");
    }
    return ok;
}
}  // namespace adviskv::e2e