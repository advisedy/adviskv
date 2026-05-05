#pragma once

#include <gtest/gtest.h>

#include <filesystem>
#include <string>

#include "common/path_util.h"

namespace adviskv::test {

inline std::filesystem::path test_data_dir() {
    return adviskv::common::path_from_config("test_data_root");
}

inline std::filesystem::path make_unique_test_dir(const std::string& prefix,
                                                  int unique_id) {
    return test_data_dir() /
           (prefix + "_" +
            std::to_string(::testing::UnitTest::GetInstance()->random_seed()) +
            "_" + std::to_string(unique_id));
}

}  // namespace adviskv::test
