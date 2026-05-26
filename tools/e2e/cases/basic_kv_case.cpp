// #include "cases/basic_kv_case.h"

// #include <fmt/core.h>

// #include <string>
// #include <vector>

// #include "e2e_assert.h"
// #include "e2e_context.h"
// #include "e2e_kv_util.h"
// #include "sdk/client.h"

// namespace adviskv::e2e {
// namespace {

// bool run_basic_kv_checks(sdk::KVClient* client, const Options& options) {
//     std::vector<std::string> keys;
//     keys.reserve(options.key_count);
//     for (int32_t i = 0; i < options.key_count; ++i) {
//         keys.push_back(fmt::format("e2e-key-{:03d}", i));
//     }

//     for (int32_t i = 0; i < options.key_count; ++i) {
//         const std::string value = fmt::format("e2e-value-{:03d}", i);
//         if (!wait_status(fmt::format("sdk put {}", keys[i]), options,
//                          [&]() { return client->put(keys[i], value); })) {
//             return false;
//         }
//     }

//     for (int32_t i = 0; i < options.key_count; ++i) {
//         const std::string value = fmt::format("e2e-value-{:03d}", i);
//         if (!wait_get_value(client, options, keys[i], value)) {
//             return false;
//         }
//     }

//     const std::string overwritten = "e2e-value-overwritten";
//     if (!wait_status("sdk overwrite first key", options, [&]() {
//             return client->put(keys.front(), overwritten);
//         })) {
//         return false;
//     }
//     if (!wait_get_value(client, options, keys.front(), overwritten)) {
//         return false;
//     }

//     if (!wait_status("sdk delete last key", options,
//                      [&]() { return client->del(keys.back()); })) {
//         return false;
//     }
//     return wait_key_not_found(client, options, keys.back());
// }

// }  // namespace

// bool run_basic_kv_case(const Options& options) {
//     fmt::print("{} {}\n", colorize(COLOR::BOLD, "AdvisKV E2E Basic KV Test"),
//                colorize(COLOR::BLUE, "(single-shard replicated cluster)"));

//     E2EContext context(options);

//     if(!prepare_table(&context)){
//         print_fail("prepare table", const std::string &message)
//     }

//     std::string error;
//     if (!create_db(&context, &error)) {
//         print_fail("create database", error);
//         return false;
//     }
//     print_pass("create database", options.db);

//     if (!create_table(&context, &error)) {
//         print_fail("create table", error);
//         return false;
//     }
//     print_pass("create table", fmt::format("{}.{}", options.db, options.table));

//     if (options.key_count <= 0) {
//         print_fail("validate options", "--key_count must be positive");
//         return false;
//     }

//     if (!wait_table_normal(&context)) {
//         return false;
//     }

//     sdk::KVClient client = make_kv_client(options);
//     if (!run_basic_kv_checks(&client, options)) {
//         return false;
//     }
//     print_pass_bold("e2e basic kv case test passed");
//     return true;
// }

// }  // namespace adviskv::e2e