#pragma once

#include <fmt/core.h>

#include <cstdint>
#include <string>
#include <vector>

#include "e2e_assert.h"
#include "e2e_kv_util.h"
#include "e2e_options.h"
#include "e2e_table_util.h"

namespace adviskv::e2e {

// 覆盖 table replica_count 扩缩容闭环：
// 先以 3 副本建表并写入数据，再依次扩容到 4、扩容到 5、缩容回 3。
// 每次 alter 后都等待 Meta/SDM/Storage 收敛到 NORMAL，并验证历史数据仍可读。
inline bool run_table_replica_count_resize_case(const Options& options) {
    if (!validate_key_count(options)) {
        return false;
    }

    E2EContext context(options);
    if (!prepare_table(&context)) {
        return false;
    }
    if (!wait_table_replica_count(&context, options.replica_count)) {
        return false;
    }

    sdk::KVClient client = make_kv_client(options);
    std::vector<std::string> dataset_prefixes;
    dataset_prefixes.push_back("resize-base");
    if (!write_dataset(&client, options, dataset_prefixes.back())) {
        return false;
    }
    if (!verify_dataset(&client, options, dataset_prefixes.back())) {
        return false;
    }

    auto verify_all_datasets = [&]() -> bool {
        for (const std::string& prefix : dataset_prefixes) {
            if (!verify_dataset(&client, options, prefix)) {
                return false;
            }
        }
        return true;
    };

    auto alter_and_check = [&](int32_t replica_count,
                               const std::string& new_prefix) -> bool {
        std::string error;
        if (!alter_table_replica_count(&context, replica_count, &error)) {
            print_fail("alter table replica_count", error);
            return false;
        }
        print_pass("alter table replica_count",
                   fmt::format("target={}", replica_count));

        if (!wait_table_replica_count(&context, replica_count)) {
            return false;
        }
        if (!verify_all_datasets()) {
            return false;
        }

        dataset_prefixes.push_back(new_prefix);
        if (!write_dataset(&client, options, new_prefix)) {
            return false;
        }
        if (!verify_all_datasets()) {
            return false;
        }
        return true;
    };

    if (!alter_and_check(4, "resize-to-4")) {
        return false;
    }
    if (!alter_and_check(5, "resize-to-5")) {
        return false;
    }
    if (!alter_and_check(3, "resize-back-3")) {
        return false;
    }

    return true;
}

}  // namespace adviskv::e2e
