#include <gtest/gtest.h>

#include <filesystem>
#include <string>

#include "common/confmgr.h"
#include "common/log.h"
#include "test/test_env.h"

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);

    adviskv::common::ConfMgr::get_instance().LoadFromFile(
        (adviskv::test::repo_root() / "conf" / "test.yaml").string());

    adviskv::common::LogConfig config;
    config.logger_name = CONF_GET_STR("logger_name");
    config.log_dir = adviskv::test::resolve_config_path("log_dir").string();
    config.log_filename = CONF_GET_STR("log_filename");
    config.log_level = CONF_GET_STR("log_level");
    config.log_to_console = CONF_GET_BOOL("log_to_console");
    config.log_to_file = CONF_GET_BOOL("log_to_file");
    adviskv::common::Logger::get_instance().init(config);

    return RUN_ALL_TESTS();
}
