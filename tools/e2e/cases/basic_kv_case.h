#pragma once

#include "e2e_assert.h"
#include "e2e_kv_util.h"
#include "e2e_options.h"
#include "e2e_table_util.h"

namespace adviskv::e2e {

// 这个场景覆盖最基础的端到端 KV 链路：
// 先通过 meta 创建 DB 和 table，等待 SDM 完成 route，然后用 SDK 写入一批 key。
// 随后验证这些 key 都能读到，再覆盖第一个 key，最后删除最后一个 key
// 并确认读不到。 这个 case 不注入故障，主要用来证明 meta -> sdm -> storage ->
// sdk 的正常链路可用。
// 并且然后再删除这个table，然后再次创建，重新再写入KV，检查是否可行。
inline bool run_basic_kv_case(const Options& options) {
    if (!validate_key_count(options)) {
        return false;
    }

    E2EContext context(options);
    if (!prepare_table(&context)) {
        return false;
    }

    sdk::KVClient client = make_kv_client(options);
    if (!write_dataset(&client, options, "basic-kv")) {
        return false;
    }
    if (!verify_dataset(&client, options, "basic-kv")) {
        return false;
    }

    print_pass("write and verify dataset", "ok");

    if (!wait_status("sdk overwrite first key", options, [&]() {
            return client.put("basic-kv-key-000", "basic-kv-value-overwrite");
        })) {
        return false;
    }

    if (!wait_get_value(&client, options, "basic-kv-key-000",
                        "basic-kv-value-overwrite")) {
        return false;
    }

    print_pass("overwrite and get the value", "ok");

    const std::vector<Key> keys = make_case_keys("basic-kv", options.key_count);
    if (!wait_status("sdk delete last key", options,
                     [&]() { return client.del(keys.back()); })) {
        return false;
    }
    if (!wait_key_not_found(&client, options, keys.back())) {
        return false;
    }

    print_pass("delete key and get", "ok");

    if (!remove_table(&context)) {
        return false;
    }

    print_pass("remove table", "ok");

    if (!prepare_table(&context)) {
        return false;
    }

    print_pass("recreate table", "ok");

    if (!wait_key_not_found(&client, options, "basic-kv-key-000")) {
        return false;
    }

    print_pass("find key expect not found", "ok");

    if (!write_dataset(&client, options, "basic2-kv")) {
        return false;
    }

    print_pass("recreate table write kvs", "ok");

    if (!verify_dataset(&client, options, "basic2-kv")) {
        return false;
    }

    print_pass("recreate table read kvs", "ok");

    return true;
}

}  // namespace adviskv::e2e