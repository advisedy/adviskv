#include <grpcpp/create_channel.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>

#include "common/arg_parser.h"
#include "common/confmgr.h"
#include "common/log.h"
#include "common/path_util.h"
#include "common/type.h"
#include "sdm/background/background.h"
#include "sdm/client/storage_client.h"
#include "sdm/handler/sdm_service_impl.h"
#include "sdm/model/sdm_store.h"
#include "sdm/selector/node_selector/node_selector.h"
#include "sdm/service/service_manager.h"

namespace {
void print_usage() { fmt::print(stderr, "usage: sdm --conf=<conf.yaml>\n"); }

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
        config.logger_name, config.log_dir, config.log_filename,
        config.log_level, config.log_to_console, config.log_to_file);
}

adviskv::sdm::SdmMetaStoreType get_metastore_type() {
    return adviskv::sdm::SdmMetaStoreType::PERSISTENT;
}

std::string get_metastore_data_dir() {
    return adviskv::path_from_project_root(
               CONF_GET_STR("data_dir", std::string("build/runtime/data/sdm")))
        .string();
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
        using namespace adviskv::sdm;

        int32_t listen_port = CONF_GET_INT("port");
        std::string listen_host =
            CONF_GET_STR("listen_host", std::string("127.0.0.1"));

        const SdmMetaStoreType metastore_type = get_metastore_type();
        const std::string metastore_data_dir = get_metastore_data_dir();
        auto sdm_store =
            std::make_unique<SdmStore>(metastore_type, metastore_data_dir);
        LOG_INFO("SDM metastore initialized: type={}, data_dir={}",
                 metastore_type == SdmMetaStoreType::PERSISTENT ? "persistent"
                                                                : "memory",
                 metastore_data_dir);
        auto storage_client = std::make_unique<StorageClient>();
        auto node_selector =
            std::make_unique<DefaultNodeSelector>(sdm_store.get());

        auto service_manager = std::make_unique<ServiceManager>(
            sdm_store.get(), node_selector.get());

        auto sdm_service =
            std::make_unique<SdmServiceImpl>(service_manager.get());

        auto table_reconcile_task =
            std::make_unique<TableReconcileTask>(service_manager.get());
        auto replica_group_reconcile_task =
            std::make_unique<ReplicaGroupReconcileTask>(service_manager.get());
        auto route_task =
            std::make_unique<RouteUpdateCheckTask>(service_manager.get());
        auto heartbeat_check_task =
            std::make_unique<HeartBeatCheckTask>(service_manager.get());
        heartbeat_check_task->start(adviskv::Milliseconds(3000));
        table_reconcile_task->start(adviskv::Milliseconds(3000));
        replica_group_reconcile_task->start(adviskv::Milliseconds(3000));
        route_task->start(adviskv::Milliseconds(3000));

        grpc::ServerBuilder builder;
        builder.AddListeningPort(fmt::format("{}:{}", listen_host, listen_port),
                                 grpc::InsecureServerCredentials());
        builder.RegisterService(sdm_service.get());

        std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
        LOG_INFO("SDM server listening on {}:{}", listen_host, listen_port);

        route_task->stop();
        replica_group_reconcile_task->stop();
        table_reconcile_task->stop();
        heartbeat_check_task->stop();
        server->Wait();
    }

    return 0;
}
