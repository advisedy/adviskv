#pragma once

#include "e2e_assert.h"
#include "e2e_case_util.h"
#include "e2e_options.h"

namespace adviskv::e2e {

// leader崩溃了后切主，然后再回来之后要成为follower追上，并且后续可以继续操作

inline bool run_leader_failover_seed_case(const Options& options) {
    /*

输出出来leader的ip和port之后，被python那边捕获，然后通过prot去停掉leader
*/
    return run_seed_case(options, "leader-failover", true);
}

inline bool run_leader_failover_verify_case(const Options& options) {
    return run_verify_case(options, "leader-failover", "after-failover",
                           true);
}

}  // namespace adviskv::e2e