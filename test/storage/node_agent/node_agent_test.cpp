#include "storage/node_agent/node_agent.h"

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <gtest/gtest.h>

#include <condition_variable>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>

#include "sdm.grpc.pb.h"
#include "storage/model/param.h"
#include "storage/replica/replica_manager.h"
#include "test/test_env.h"

namespace fs = std::filesystem;

namespace adviskv::storage {
namespace {

class FakeSdmService final : public rpc::ShardingManagerService::Service {
   public:
    grpc::Status RegisterNode(grpc::ServerContext*,
                              const rpc::RegisterNodeRequest* request,
                              rpc::RegisterNodeResponse* response) override {
        {
            std::lock_guard lock(mutex_);
            ++register_count_;
            last_register_node_id_ = request->node_id();
            last_register_ip_ = request->ip();
            last_register_port_ = request->port();
            last_register_resource_pool_ = request->resource_pool();
            last_register_dc_ = request->dc();
        }
        response->mutable_base_rsp()->set_code(0);
        cv_.notify_all();
        return grpc::Status::OK;
    }

    grpc::Status HeartBeat(grpc::ServerContext*,
                           const rpc::HeartBeatRequest* request,
                           rpc::HeartBeatResponse* response) override {
        {
            std::lock_guard lock(mutex_);
            ++heartbeat_count_;
            last_heartbeat_ = *request;
        }
        response->mutable_base_rsp()->set_code(0);
        cv_.notify_all();
        return grpc::Status::OK;
    }

    bool wait_for_counts(int expected_registers, int expected_heartbeats,
                         Milliseconds timeout) {
        std::unique_lock lock(mutex_);
        return cv_.wait_for(lock, timeout, [&] {
            return register_count_ >= expected_registers &&
                   heartbeat_count_ >= expected_heartbeats;
        });
    }

    int register_count() const {
        std::lock_guard lock(mutex_);
        return register_count_;
    }

    int heartbeat_count() const {
        std::lock_guard lock(mutex_);
        return heartbeat_count_;
    }

    std::string last_register_node_id() const {
        std::lock_guard lock(mutex_);
        return last_register_node_id_;
    }

    std::string last_register_ip() const {
        std::lock_guard lock(mutex_);
        return last_register_ip_;
    }

    int last_register_port() const {
        std::lock_guard lock(mutex_);
        return last_register_port_;
    }

    std::string last_register_resource_pool() const {
        std::lock_guard lock(mutex_);
        return last_register_resource_pool_;
    }

    std::string last_register_dc() const {
        std::lock_guard lock(mutex_);
        return last_register_dc_;
    }

    rpc::HeartBeatRequest last_heartbeat() const {
        std::lock_guard lock(mutex_);
        return last_heartbeat_;
    }

   private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    int register_count_{0};
    int heartbeat_count_{0};
    std::string last_register_node_id_;
    std::string last_register_ip_;
    int last_register_port_{0};
    std::string last_register_resource_pool_;
    std::string last_register_dc_;
    rpc::HeartBeatRequest last_heartbeat_;
};

class FakeSdmServer {
   public:
    void start() {
        grpc::ServerBuilder builder;
        builder.AddListeningPort("127.0.0.1:0",
                                 grpc::InsecureServerCredentials(), &port_);
        builder.RegisterService(&service_);
        server_ = builder.BuildAndStart();
        ASSERT_NE(server_, nullptr);
        ASSERT_GT(port_, 0);
    }

    void stop() {
        if (server_) {
            server_->Shutdown();
            server_.reset();
        }
    }

    int port() const { return port_; }
    FakeSdmService& service() { return service_; }

   private:
    int port_{0};
    FakeSdmService service_;
    std::unique_ptr<grpc::Server> server_;
};

class NodeAgentTest : public ::testing::Test {
   protected:
    void SetUp() override {
        base_dir_ =
            adviskv::test::make_unique_test_dir("node_agent", sequence_++);
        ASSERT_TRUE(fs::create_directories(base_dir_)) << base_dir_.string();
        fake_sdm_.start();
    }

    void TearDown() override {
        fake_sdm_.stop();
        std::error_code ec;
        fs::remove_all(base_dir_, ec);
    }

    NodeAgentConf valid_conf() const {
        return NodeAgentConf{
            "node-1",
            "127.0.0.1",
            50050,
            "default",
            "dc1",
            "127.0.0.1",
            fake_sdm_.port(),
            20,
            10,
        };
    }

    ReplicaInitParam make_replica_param() const {
        ReplicaID replica_id{101, 7, 0};
        Endpoint endpoint{"127.0.0.1", 50050};
        return ReplicaInitParam{
            replica_id,
            EngineType::MAP,
            endpoint,
            {PeerMember{"node-1", replica_id, endpoint}},
            base_dir_.string(),
        };
    }

    static inline int sequence_{0};
    fs::path base_dir_;
    FakeSdmServer fake_sdm_;
};

TEST_F(NodeAgentTest, RejectsInvalidConfig) {
    NodeAgentConf conf = valid_conf();

    EXPECT_TRUE(conf.validate().ok());

    conf.node_id.clear();
    EXPECT_TRUE(conf.validate().fail());

    conf = valid_conf();
    conf.manager_port = 0;
    EXPECT_TRUE(conf.validate().fail());
}

TEST_F(NodeAgentTest, StartRegistersNodeAndSendsPeriodicHeartbeat) {
    ReplicaManager replica_manager(base_dir_.string());
    ASSERT_TRUE(replica_manager.add_replica(make_replica_param()).ok());

    NodeAgent agent;
    Status status = agent.init(valid_conf(), &replica_manager);
    ASSERT_TRUE(status.ok()) << status.msg();

    status = agent.start();
    ASSERT_TRUE(status.ok()) << status.msg();

    ASSERT_TRUE(fake_sdm_.service().wait_for_counts(1, 20, Milliseconds(1000)));
    agent.stop();

    EXPECT_EQ(fake_sdm_.service().register_count(), 1);
    EXPECT_GE(fake_sdm_.service().heartbeat_count(), 20);
    EXPECT_EQ(fake_sdm_.service().last_register_node_id(), "node-1");
    EXPECT_EQ(fake_sdm_.service().last_register_ip(), "127.0.0.1");
    EXPECT_EQ(fake_sdm_.service().last_register_port(), 50050);
    EXPECT_EQ(fake_sdm_.service().last_register_resource_pool(), "default");
    EXPECT_EQ(fake_sdm_.service().last_register_dc(), "dc1");

    rpc::HeartBeatRequest heartbeat = fake_sdm_.service().last_heartbeat();
    EXPECT_EQ(heartbeat.node_id(), "node-1");
    EXPECT_EQ(heartbeat.ip(), "127.0.0.1");
    EXPECT_EQ(heartbeat.port(), 50050);
    EXPECT_EQ(heartbeat.resource_pool(), "default");
    EXPECT_EQ(heartbeat.dc(), "dc1");
    ASSERT_EQ(heartbeat.replica_info_list_size(), 1);
    EXPECT_EQ(heartbeat.replica_info_list(0).table_id(), 101);
    EXPECT_EQ(heartbeat.replica_info_list(0).shard_id(), 7);
    EXPECT_EQ(heartbeat.replica_info_list(0).replica_index(), 0);
    EXPECT_EQ(heartbeat.replica_info_list(0).role(), pb::ReplicaRole::FOLLOWER);
    EXPECT_EQ(heartbeat.replica_info_list(0).status(),
              pb::ReplicaStatus::READY);
}

}  // namespace
}  // namespace adviskv::storage