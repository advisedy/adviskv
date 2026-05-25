#pragma once

#include "e2e_options.h"

namespace adviskv::e2e {

bool run_restart_persistence_seed_case(const Options& options);
bool run_restart_persistence_verify_case(const Options& options);

}  // namespace adviskv::e2e