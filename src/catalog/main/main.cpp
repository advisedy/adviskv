#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/create_channel.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include "common/arg_parser.h"
#include "common/confmgr.h"
#include "common/log.h"
#include "common/model/type.h"
#include "common/path_util.h"
#include "common/status.h"
#include "catalog/background/table_reconciler.h"
#include "catalog/store/catalog_store.h"
#include "catalog/handler/catalog_service_impl.h"
#include "catalog/persist/catalog_persist_engine.h"
#include "catalog/service/ddl_service.h"
#include "catalog/service/topo_client.h"

namespace {

void print_usage() { fmt::print(stderr, "usage: catalog --conf=<conf.yaml>\n"); }

bool parse_args(int argc, char* argv[], std::string* conf_file) {
    adviskv::ArgParser parser;
    parser.add_string("conf", *conf_file);
    if (!parser.parse(argc, argv)) return false;
    if (conf_file->empty()) {
        fmt::print(stderr, "--conf must be provided\n");
        return false;
    }
    return true;
}

void init_conf(const char* conf_file) {
    auto& conf_mgr = adviskv::ConfMgr::get_instance();
    conf_mgr.LoadFromFile(adviskv::path_from_project_root(conf_file).string());
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
            config.logger_name, config.log_dir, config.log_filename, config.log_level, config.log_to_console,
            config.log_to_file);
}

}  // namespace

int main(int argc, char* argv[]) {
    std::string conf_file;
    if (!parse_args(argc, argv, &conf_file)) {
        print_usage();
        return 1;
    }
    try {
        init_conf(conf_file.c_str());
        init_logger();
        LOG_INFO("init phase finish");
    } catch (const std::exception& e) {
        fmt::print(stderr, "Exception caught in main: {}\n", e.what());
        return 1;
    }

    {
        using namespace adviskv::catalog;

        int32_t listen_port = CONF_GET_INT("port");
        std::string listen_host = adviskv::ConfMgr::get_instance().Get<std::string>("listen_host", "127.0.0.1");
        std::string topo_host = CONF_GET_STR("topo_host");
        int32_t topo_port = CONF_GET_INT("topo_port");
        int32_t topo_rpc_timeout_ms = CONF_GET_INT("topo_rpc_timeout_ms", 3000);
        std::string data_dir = adviskv::path_from_project_root(CONF_GET_STR("data_dir")).string();

        auto persist_engine = std::make_unique<CatalogPersistEngine>(data_dir);
        if (adviskv::Status status = persist_engine->init(); status.fail()) {
            LOG_ERROR("failed to init catalog persist engine: {}", status.msg());
            return 1;
        }

        auto catalog_store = std::make_unique<CatalogStore>(persist_engine.get());

        if (adviskv::Status status = catalog_store->init(); status.fail()) {
            LOG_ERROR("failed to init catalog store: {}", status.msg());
            return 1;
        }

        auto topo_channel =
                grpc::CreateChannel(fmt::format("{}:{}", topo_host, topo_port), grpc::InsecureChannelCredentials());
        auto topo_client = std::make_unique<TopoClient>(topo_channel, topo_rpc_timeout_ms);

        auto ddl_service = std::make_unique<DdlService>(catalog_store.get(), topo_client.get());
        auto table_reconciler = std::make_unique<TableReconciler>(catalog_store.get(), topo_client.get());
        table_reconciler->start(adviskv::Milliseconds(3000));

        auto catalog_service = std::make_unique<CatalogServiceImpl>(ddl_service.get());

        grpc::ServerBuilder builder;
        builder.AddListeningPort(fmt::format("{}:{}", listen_host, listen_port), grpc::InsecureServerCredentials());
        builder.RegisterService(catalog_service.get());

        std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
        LOG_INFO("Catalog server listening on {}:{}", listen_host, listen_port);

        server->Wait();
        table_reconciler->stop();
    }

    return 0;
}
