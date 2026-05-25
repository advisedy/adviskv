#pragma once

#include <cstdint>
#include <string>

namespace adviskv::e2e {

struct Options {
    std::string case_name{"basic_kv"};
    std::string meta_host{"127.0.0.1"};
    int32_t meta_port{50048};
    std::string sdm_host{"127.0.0.1"};
    int32_t sdm_port{50049};
    std::string db{"e2e_db"};
    std::string table{"e2e_table"};
    std::string zone{"dc1"};
    std::string resource_pool{"default"};
    int32_t shard_count{1};
    int32_t replica_count{3};
    int32_t key_count{8};
    int32_t timeout_ms{60000};
    int32_t poll_interval_ms{500};
};

}  // namespace adviskv::e2e