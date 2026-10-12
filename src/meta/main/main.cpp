#include <cstdint>
#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include "common/arg_parser.h"
#include "common/confmgr.h"
#include "common/constants.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "common/path.h"
#include "meta/background/background.h"
#include "meta/handler/meta_service_impl.h"
#include "meta/placement/node_selector.h"
#include "meta/service/service_manager.h"
#include "meta/store/meta_store.h"

namespace {

void print_usage() { fmt::print(stderr, "usage: meta --conf=<conf.yaml>\n"); }

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
    config.log_dir = adviskv::path_from_project_root(CONF_GET_STR("log_dir")).string();
    config.log_filename = CONF_GET_STR("log_filename");
    config.log_level = CONF_GET_STR("log_level");
    config.log_to_console = CONF_GET_BOOL("log_to_console");
    config.log_to_file = CONF_GET_BOOL("log_to_file");
    adviskv::Logger::get_instance().init(config);
}

void init_metrics() {
    adviskv::MetricsOptions options;
    options.http_enable = CONF_GET_BOOL("metrics_http_enable", options.http_enable);
    if (options.http_enable) {
        options.http_host = CONF_GET_STR("metrics_http_host", options.http_host);
        options.http_port = CONF_GET_INT("metrics_http_port", options.http_port);
        options.http_path = CONF_GET_STR("metrics_http_path", options.http_path);
    }
    adviskv::Status status = adviskv::AdvisMetrics::get_instance().init(options);
    if (status.fail()) {
        LOG_WARN("metrics init failed: {}", status.msg());
    }
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
        init_metrics();
        LOG_INFO("init phase finish");
    } catch (const std::exception& e) {
        fmt::print(stderr, "Exception caught in main: {}\n", e.what());
        return 1;
    }

    {
        using namespace adviskv::meta;
        int32_t listen_port = CONF_GET_INT("port");
        std::string listen_host = CONF_GET_STR("listen_host", std::string(adviskv::K_DEFAULT_META_LISTEN_HOST));
        std::string data_dir = adviskv::path_from_project_root(
                CONF_GET_STR("data_dir", std::string(adviskv::K_DEFAULT_META_DATA_DIR))).string();

        auto store = std::make_unique<MetaStore>(PersistentMetaStoreParam{data_dir});
        if (adviskv::Status status = store->init(); status.fail()) {
            LOG_ERROR("failed to init meta store: {}", status.to_string());
            return 1;
        }
        auto node_selector = std::make_unique<DefaultNodeSelector>(store.get());
        auto service_manager = std::make_unique<ServiceManager>(store.get(), node_selector.get());
        auto meta_service = std::make_unique<MetaServiceImpl>(service_manager.get());

        grpc::ServerBuilder builder;
        builder.AddListeningPort(fmt::format("{}:{}", listen_host, listen_port), grpc::InsecureServerCredentials());
        builder.RegisterService(meta_service.get());
        std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
        if (server == nullptr) {
            LOG_ERROR("failed to start meta server on {}:{}", listen_host, listen_port);
            return 1;
        }

        auto reconcile_task = std::make_unique<MetaReconcileTask>(service_manager.get());
        reconcile_task->start(adviskv::Milliseconds(adviskv::K_META_RECONCILE_INTERVAL_MS));

        LOG_INFO("Meta server listening on {}:{}", listen_host, listen_port);
        server->Wait();
        reconcile_task->stop();
    }
    return 0;
}
