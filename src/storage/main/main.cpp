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
#include "storage/node_agent/node_agent.h"
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

        std::string data_dir = CONF_GET_STR("data_dir");
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

        grpc::ServerBuilder builder;
        builder.AddListeningPort(
            fmt::format("0.0.0.0:{}", listen_port),
            grpc::InsecureServerCredentials());

        builder.RegisterService(service.get());

        std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
        LOG_INFO("Server listening on 0.0.0.0:{}", listen_port);

        server->Wait();
        node_agent.stop();
    }

    return 0;
}
