#pragma once

#include <gtest/gtest.h>

#include <filesystem>
#include <string>

#include "common/confmgr.h"

namespace adviskv::test {

inline std::filesystem::path repo_root() {
    return std::filesystem::path(__FILE__).parent_path().parent_path();
}

inline std::filesystem::path resolve_config_path(const std::string& key) {
    std::filesystem::path path =
        adviskv::common::ConfMgr::get_instance().Get<std::string>(key);
    if (path.is_relative()) {
        path = repo_root() / path;
    }
    return path.lexically_normal();
}

inline std::filesystem::path test_data_root() {
    return resolve_config_path("test_data_root");
}

inline std::filesystem::path make_unique_test_dir(const std::string& prefix,
                                                  int unique_id) {
    return test_data_root() /
           (prefix + "_" +
            std::to_string(::testing::UnitTest::GetInstance()->random_seed()) +
            "_" + std::to_string(unique_id));
}

}  // namespace adviskv::test
