#include <grpcpp/server.h>

#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>

#include "common/confmgr.h"
#include "common/log.h"
#include "common/metrics/metrics.h"
#include "common/path_util.h"
#include "common/type.h"
#include "storage/engine/map_engine.h"
#include "storage/handler/storage_service.h"
#include "storage/node_agent/node_agent.h"
#include "storage/replica/replica_manager.h"

namespace {

void init_conf(char* conf_file) {
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

void init_metrics() {
    adviskv::MetricsOptions options;
    options.http_enable = CONF_GET_BOOL("metrics_http_enable", false);
    if (options.http_enable) {
        options.http_host =
            CONF_GET_STR("metrics_http_host", options.http_host);
        options.http_port =
            CONF_GET_INT("metrics_http_port", options.http_port);
        options.http_path =
            CONF_GET_STR("metrics_http_path", options.http_path);
    }

    adviskv::Status status =
        adviskv::AdvisMetrics::get_instance().init(options);
    if (status.ok()) {
        if (options.http_enable) {
            LOG_INFO("metrics http server listening on {}:{}{}",
                     options.http_host, options.http_port, options.http_path);
        }
        return;
    }
    LOG_WARN("metrics init failed: {}", status.msg());
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 2) {
        fmt::print(stderr, "need: <conf_file>\n");
        return 1;
    }
    try {
        init_conf(argv[1]);
        init_logger();
        init_metrics();

        LOG_INFO("init phase finish");
    } catch (const std::exception& e) {
        fmt::print(stderr, "Exception caught in main: {}\n", e.what());
    }

    {
        using namespace adviskv::storage;

        std::string data_dir = adviskv::path_from_config("data_dir").string();
        int32_t listen_port = CONF_GET_INT("port");

        auto replica_manager =
            std::make_unique<ReplicaManager>(std::move(data_dir));
        ReplicaManager* replica_manager_ptr = replica_manager.get();

        replica_manager->recover();
        replica_manager->start_tick();

        NodeAgentConf agent_conf;
        agent_conf.node_id = CONF_GET_STR("node_id");
        agent_conf.ip = CONF_GET_STR("ip");
        agent_conf.port = CONF_GET_INT("port");
        agent_conf.resource_pool = CONF_GET_STR("resource_pool");
        agent_conf.dc = CONF_GET_STR("dc");
        agent_conf.manager_host = CONF_GET_STR("manager_host");
        agent_conf.manager_port = CONF_GET_INT("manager_port");
        agent_conf.heartbeat_interval_ms =
            CONF_GET_INT("heartbeat_interval_ms");

        auto service =
            std::make_unique<StorageServiceImpl>(std::move(replica_manager));

        NodeAgent node_agent;
        adviskv::Status agent_status =
            node_agent.init(agent_conf, replica_manager_ptr);
        if (agent_status.ok()) {
            agent_status = node_agent.start();
        }
        if (agent_status.ok()) {
            LOG_INFO("node agent started, node_id={}", agent_conf.node_id);
        } else {
            LOG_WARN("node agent setup failed: {}", agent_status.msg());
        }

        std::string listen_host =
            adviskv::ConfMgr::get_instance().Get<std::string>("listen_host",
                                                              agent_conf.ip);

        grpc::ServerBuilder builder;
        builder.AddListeningPort(fmt::format("{}:{}", listen_host, listen_port),
                                 grpc::InsecureServerCredentials());

        builder.RegisterService(service.get());

        std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
        LOG_INFO("Server listening on {}:{}", listen_host, listen_port);

        server->Wait();
        node_agent.stop();
    }

    return 0;
}
