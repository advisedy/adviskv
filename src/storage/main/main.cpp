#include "common/log.h"
#include "common/confmgr.h"
#include <cstdint>
#include <iostream>
#include <cstring>
#include "common/type.h"
#include "storage/engine/map_engine.h"
namespace {

void test(){

    std::string s1{"1"},s2{"2"};
    adviskv::MapEngine engine;
    engine.put(s1, s2);
    adviskv::Value value;
    engine.get(s1, value);
    engine.get(s2, value);
    INFO("value = {}", value);

}


void init_logger() {
    adviskv::common::LogConfig config;
    config.logger_name = CONF_GET_STR("logger_name");
    config.log_dir = CONF_GET_STR("log_dir");
    config.log_filename = CONF_GET_STR("log_filename");
    config.log_level = CONF_GET_STR("log_level");
    config.log_to_console = CONF_GET_BOOL("log_to_console");
    config.log_to_file = CONF_GET_BOOL("log_to_file");
    adviskv::common::Logger::get_instance().init(config);
    DEBUG("logger config: logger_name={}, log_dir={}, log_filename={}, log_level={}, log_to_console={}, log_to_file={}",
        config.logger_name, config.log_dir, config.log_filename, config.log_level, config.log_to_console, config.log_to_file);
}

void init_conf() {
    auto& conf_mgr = adviskv::common::ConfMgr::get_instance();
    conf_mgr.LoadFromFile("./conf/test.yaml");
}

}

int main() {
    try{
        init_conf();
        init_logger();
        INFO("init phase finish");

        test();
    }
    catch (const std::exception& e) {
        fmt::print(stderr, "Exception caught in main: {}\n", e.what());
    }


    return 0;
}

