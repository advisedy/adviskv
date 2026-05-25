#pragma once

#include <string>

#include "e2e_options.h"
#include "sdk/client.h"

namespace adviskv::e2e {

sdk::KVClient make_kv_client(const Options& options);

bool wait_get_value(sdk::KVClient* client, const Options& options,
                    const std::string& key, const std::string& expected);

bool wait_key_not_found(sdk::KVClient* client, const Options& options,
                        const std::string& key);

}  // namespace adviskv::e2e