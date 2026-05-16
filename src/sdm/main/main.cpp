#include <grpcpp/create_channel.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>

#include "common/confmgr.h"
#include "common/log.h"
#include "common/path_util.h"
#include "common/type.h"
#include "sdm/background/heartbeat_check_task.h"
#include "sdm/background/routeupdate_check_task.h"
#include "sdm/client/storage_client.h"
#include "sdm/handler/sdm_service_impl.h"
#include "sdm/model/sdm_store.h"
#include "sdm/selector/node_selector/node_selector.h"
#include "sdm/service/heartbeat_service.h"
#include "sdm/service/node_service.h"
#include "sdm/service/route_service.h"
#include "sdm/service/table_service.h"
#include "sdm/workflow/placetable_workflow.h"
#include "sdm/workflow/placetable_workflow_runner.h"

namespace {
void init_conf() {
    auto& conf_mgr = adviskv::ConfMgr::get_instance();
    conf_mgr.LoadFromFile(
        adviskv::path_from_project_root("conf/sdm.yaml").string());
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
    return adviskv::path_from_project_root(CONF_GET_STR(
                                                       "data_dir",
                                                       std::string("./build/"
                                                                   "sdm_metastore")))
        .string();
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

        auto workflow = std::make_unique<PlaceTableWorkflow>(
            sdm_store.get(), storage_client.get(), node_selector.get());

        auto table_service =
            std::make_unique<TableService>(sdm_store.get(), workflow.get());
        auto node_service = std::make_unique<NodeService>(sdm_store.get());
        auto heartbeat_service =
            std::make_unique<HeartBeatService>(sdm_store.get());
        auto route_service = std::make_unique<RouteService>(sdm_store.get());

        auto sdm_service = std::make_unique<SdmServiceImpl>(
            table_service.get(), node_service.get(), heartbeat_service.get(),
            route_service.get());

        auto runner = std::make_unique<PlaceTableWorkflowRunner>(
            sdm_store.get(), storage_client.get(), node_selector.get());
        auto route_task =
            std::make_unique<RouteUpdateCheckTask>(sdm_store.get());
        auto heartbeat_check_task =
            std::make_unique<HeartBeatCheckTask>(sdm_store.get());
        runner->start(Milliseconds(3000));
        route_task->start(Milliseconds(3000));
        heartbeat_check_task->start(Milliseconds(3000));

        grpc::ServerBuilder builder;
        builder.AddListeningPort(fmt::format("{}:{}", listen_host, listen_port),
                                 grpc::InsecureServerCredentials());
        builder.RegisterService(sdm_service.get());

        std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
        LOG_INFO("SDM server listening on {}:{}", listen_host, listen_port);

        server->Wait();
        heartbeat_check_task->stop();
        runner->stop();
        route_task->stop();
    }

    return 0;
}
