#include <grpcpp/create_channel.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <cstdint>
#include <iostream>
#include <memory>

#include "common/confmgr.h"
#include "common/log.h"
#include "common/type.h"
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
    conf_mgr.LoadFromFile("./conf/sdm.yaml");
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

        auto sdm_store = std::make_unique<SdmStore>(SdmMetaStoreType::MEMORY);
        auto storage_client = std::make_unique<StorageClient>();
        auto node_selector = std::make_unique<DefaultNodeSelector>();

        auto workflow = std::make_unique<PlaceTableWorkflow>(
            sdm_store.get(), storage_client.get(), node_selector.get());

        auto table_service = std::make_unique<TableService>(workflow.get());
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
        runner->start(Milliseconds(3000));
        route_task->start(Milliseconds(3000));

        grpc::ServerBuilder builder;
        builder.AddListeningPort(fmt::format("0.0.0.0:{}", listen_port),
                                 grpc::InsecureServerCredentials());
        builder.RegisterService(sdm_service.get());

        std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
        LOG_INFO("SDM server listening on 0.0.0.0:{}", listen_port);

        server->Wait();
        runner->stop();
        route_task->stop();
    }

    return 0;
}
