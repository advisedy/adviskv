#include "e2e_case_util.h"

#include <fmt/core.h>

#include "e2e_assert.h"
#include "e2e_context.h"
#include "e2e_kv_util.h"
#include "e2e_route_util.h"
#include "e2e_table_util.h"

namespace adviskv::e2e {

bool run_seed_case(const Options& options, const std::string& prefix,
                   bool print_leader) {
    if (!validate_key_count(options)) {
        return false;
    }

    E2EContext context(options);
    if (!prepare_table(&context)) {
        return false;
    }

    sdk::KVClient client = make_kv_client(options);
    if (!write_dataset(&client, options, prefix)) {
        return false;
    }

    if (print_leader && !print_current_leader(&context, prefix + "-key-000")) {
        return false;
    }
    return true;
}

bool run_verify_case(const Options& options, const std::string& prefix,
                     const std::string& after_key_suffix) {
    if (!validate_key_count(options)) {
        return false;
    }

    E2EContext context(options);
    if (!wait_table_normal(&context)) {
        return false;
    }
    if (!wait_route_has_leader(&context, prefix + "-key-000", nullptr)) {
        return false;
    }

    sdk::KVClient client = make_kv_client(options);
    if (!verify_dataset(&client, options, prefix)) {
        return false;
    }

    if (after_key_suffix.empty()) return true;

    const std::string after_key = prefix + "-" + after_key_suffix;
    const std::string after_value = prefix + "-value-" + after_key_suffix;
    if (!wait_status(fmt::format("sdk put {}", after_key), options,
                     [&]() { return client.put(after_key, after_value); })) {
        return false;
    }
    return wait_get_value(&client, options, after_key, after_value);
}

}  // namespace adviskv::e2e