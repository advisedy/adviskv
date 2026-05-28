#pragma once

#include <string>

#include "e2e_options.h"

namespace adviskv::e2e {

// prepare table + write_dataset
bool run_seed_case(const Options& options, const std::string& prefix,
                   bool print_leader = false);

// wait table is normal + verify_dataset
// 并且再次尝试写入key和读取key，看看是否正常 (after_key_suffix非空的时候)
bool run_verify_case(const Options& options, const std::string& prefix,
                     const std::string& after_key_suffix = "");

}  // namespace adviskv::e2e