#include <grpcpp/server.h>

#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>

#include "common/confmgr.h"
#include "common/log.h"
#include "common/type.h"
#include "storage/engine/map_engine.h"
#include "storage/handler/storage_service.h"
#include "storage/replica/replica_manager.h"

namespace {

void init_logger() {
    adviskv::common::LogConfig config;
    config.logger_name = CONF_GET_STR("logger_name");
    config.log_dir = CONF_GET_STR("log_dir");
    config.log_filename = CONF_GET_STR("log_filename");
    config.log_level = CONF_GET_STR("log_level");
    config.log_to_console = CONF_GET_BOOL("log_to_console");
    config.log_to_file = CONF_GET_BOOL("log_to_file");
    adviskv::common::Logger::get_instance().init(config);
    LOG_DEBUG(
        "logger config: logger_name={}, log_dir={}, log_filename={}, "
        "log_level={}, log_to_console={}, log_to_file={}",
        config.logger_name, config.log_dir, config.log_filename,
        config.log_level, config.log_to_console, config.log_to_file);
}

void init_conf() {
    auto& conf_mgr = adviskv::common::ConfMgr::get_instance();
    conf_mgr.LoadFromFile("./conf/test.yaml");
}

}  // namespace

namespace adviskv::storage {

auto replica_manager = std::make_unique<ReplicaManager>();

}

int main() {
    try {
        init_conf();
        init_logger();
        LOG_INFO("init phase finish");
    } catch (const std::exception& e) {
        fmt::print(stderr, "Exception caught in main: {}\n", e.what());
    }

    {
        using namespace adviskv::storage;
        replica_manager->recover();
        replica_manager->start_tick();


        auto service =
            std::make_unique<StorageServiceImpl>(std::move(replica_manager));


        grpc::ServerBuilder builder;
        builder.AddListeningPort("0.0.0.0:50051",
                                 grpc::InsecureServerCredentials());


        builder.RegisterService(service.get());

        // 4. 启动
        std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
        LOG_INFO("Server listening on 0.0.0.0:50051");

        // 5. 阻塞等待（不然 main 就退出了）
        server->Wait();
    }

    return 0;
}
