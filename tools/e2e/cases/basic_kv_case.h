#pragma once

#include "e2e_assert.h"
#include "e2e_kv_util.h"
#include "e2e_options.h"

namespace adviskv::e2e {
// 这个场景就是做一些最基本的这个正常的链路，写入，然后你查找、删除，有没有问题
inline bool run_basic_kv_case(const Options& options) {
    {
        bool ok = run_seed_case(options, "AdvisKV E2E Leader Failover Seed",
                                "leader-failover");
        if (!ok) {
            print_fail("basic kv case", "run seed case failed");
        }
    }
    {
        bool ok = run_verify_case(options, "AdvisKV E2E Leader Failover Verify",
                                  "leader-failover");
        if (!ok) {
            print_fail("basic kv case", "run verify case failed");
        }
    }
    return true;
}

}  // namespace adviskv::e2e