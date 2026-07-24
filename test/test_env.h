#pragma once

#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <string>

#include "common/path_util.h"
#include "common/status.h"

namespace adviskv::test {

inline std::filesystem::path test_data_dir() {
    return adviskv::path_from_config("test_data_root");
}

inline std::filesystem::path make_unique_test_dir(const std::string& prefix,
                                                  int unique_id) {
    return test_data_dir() /
           (prefix + "_" +
            std::to_string(::testing::UnitTest::GetInstance()->random_seed()) +
            "_" + std::to_string(::getpid()) +
            "_" + std::to_string(unique_id));
}

inline std::string status_debug_string(const Status& status) {
    return "code=" + std::to_string(static_cast<int>(status.code())) +
           ", msg=" + status.msg();
}

}  // namespace adviskv::test
