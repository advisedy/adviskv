#include <grpcpp/create_channel.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>

#include "common/confmgr.h"
#include "common/log.h"
#include "common/path_util.h"
#include "common/type.h"
#include "meta/catalog/catalog_manager.h"
#include "meta/handler/meta_service_impl.h"
#include "meta/service/ddl_service.h"

namespace {

void init_conf() {
    auto& conf_mgr = adviskv::common::ConfMgr::get_instance();
    conf_mgr.LoadFromFile(
        adviskv::common::path_from_project_root("conf/meta.yaml").string());
}

void init_logger() {
    adviskv::common::LogConfig config;
    config.logger_name = CONF_GET_STR("logger_name");
    config.log_dir = adviskv::common::path_from_config("log_dir").string();
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

}  // namespace

int main() {
    try {
        init_conf();
        init_logger();
        LOG_INFO("init phase finish");
    } catch (const std::exception& e) {
        fmt::print(stderr, "Exception caught in main: {}\n", e.what());
    }

    {
        using namespace adviskv::meta;

        int32_t listen_port = CONF_GET_INT("port");
        std::string sdm_host = CONF_GET_STR("sdm_host");
        int32_t sdm_port = CONF_GET_INT("sdm_port");

        auto catalog_manager = std::make_unique<CatalogManager>();

        auto sdm_channel =
            grpc::CreateChannel(fmt::format("{}:{}", sdm_host, sdm_port),
                                grpc::InsecureChannelCredentials());
        auto sdm_client = std::make_unique<SdmClient>(sdm_channel);

        auto ddl_service = std::make_unique<DdlService>(catalog_manager.get(),
                                                        sdm_client.get());

        auto meta_service = std::make_unique<MetaServiceImpl>(
            ddl_service.get(), catalog_manager.get());

        grpc::ServerBuilder builder;
        builder.AddListeningPort(fmt::format("0.0.0.0:{}", listen_port),
                                 grpc::InsecureServerCredentials());
        builder.RegisterService(meta_service.get());

        std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
        LOG_INFO("Meta server listening on 0.0.0.0:{}", listen_port);

        server->Wait();
    }

    return 0;
}
