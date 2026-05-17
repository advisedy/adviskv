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
#include "common/status.h"
#include "common/type.h"
#include "meta/background/table_ddl_reconciler.h"
#include "meta/catalog/catalog_manager.h"
#include "meta/handler/meta_service_impl.h"
#include "meta/persist/meta_persist_engine.h"
#include "meta/service/ddl_service.h"

namespace {

void init_conf() {
    auto& conf_mgr = adviskv::ConfMgr::get_instance();
    conf_mgr.LoadFromFile(
        adviskv::path_from_project_root("conf/meta.yaml").string());
}

void init_logger() {
    adviskv::LogConfig config;
    config.logger_name = CONF_GET_STR("logger_name");
    config.log_dir = adviskv::path_from_config("log_dir").string();
    config.log_filename = CONF_GET_STR("log_filename");
    config.log_level = CONF_GET_STR("log_level");
    config.log_to_console = CONF_GET_BOOL("log_to_console");
    config.log_to_file = CONF_GET_BOOL("log_to_file");
    adviskv::Logger::get_instance().init(config);
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
        std::string listen_host = adviskv::ConfMgr::get_instance()
                                      .Get<std::string>("listen_host",
                                                        "127.0.0.1");
        std::string sdm_host = CONF_GET_STR("sdm_host");
        int32_t sdm_port = CONF_GET_INT("sdm_port");
        std::string data_dir =
            adviskv::path_from_project_root(
                CONF_GET_STR("data_dir"))
                .string();

        auto persist_engine = std::make_unique<MetaPersistEngine>(data_dir);
        if (adviskv::Status status = persist_engine->init(); status.fail()) {
            LOG_ERROR("failed to init meta persist engine: {}", status.msg());
            return 1;
        }

        auto catalog_manager =
            std::make_unique<CatalogManager>(persist_engine.get());

        if (adviskv::Status status = catalog_manager->init(); status.fail()) {
            LOG_ERROR("failed to init catalog manager: {}", status.msg());
            return 1;
        }

        auto sdm_channel =
            grpc::CreateChannel(fmt::format("{}:{}", sdm_host, sdm_port),
                                grpc::InsecureChannelCredentials());
        auto sdm_client = std::make_unique<SdmClient>(sdm_channel);

        auto ddl_service = std::make_unique<DdlService>(catalog_manager.get(),
                                                        sdm_client.get());
        auto table_ddl_reconciler = std::make_unique<TableDdlReconciler>(
            catalog_manager.get(), sdm_client.get());
        table_ddl_reconciler->start(Milliseconds(3000));

        auto meta_service = std::make_unique<MetaServiceImpl>(
            ddl_service.get(), catalog_manager.get());

        grpc::ServerBuilder builder;
        builder.AddListeningPort(fmt::format("{}:{}", listen_host, listen_port),
                                 grpc::InsecureServerCredentials());
        builder.RegisterService(meta_service.get());

        std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
        LOG_INFO("Meta server listening on {}:{}", listen_host, listen_port);

        server->Wait();
        table_ddl_reconciler->stop();
    }

    return 0;
}