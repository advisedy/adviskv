#include "storage/node_agent/node_agent.h"

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <gtest/gtest.h>

#include <condition_variable>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/proto/replica_id_proto.h"
#include "sdm.grpc.pb.h"
#include "storage/model/param.h"
#include "storage/replica/replica_manager.h"
#include "test/test_env.h"

namespace fs = std::filesystem;

namespace adviskv::storage {
namespace {

class FakeSdmService final : public sdm_rpc::SdmService::Service {
   public:
    grpc::Status RegisterNode(grpc::ServerContext*,
                              const sdm_rpc::RegisterNodeRequest* request,
                              sdm_rpc::RegisterNodeResponse* response) override {
        {
            std::lock_guard lock(mutex_);
            ++register_count_;
            last_register_node_id_ = request->node_id();
            last_register_ip_ = request->ip();
            last_register_port_ = request->port();
            last_register_resource_pool_ = request->resource_pool();
            last_register_dc_ = request->dc();
            response->mutable_base_rsp()->set_code(
                to_rpc_code(next_register_status_.code()));
            response->mutable_base_rsp()->set_msg(next_register_status_.msg());
            next_register_status_ = Status::OK();
        }
        cv_.notify_all();
        return grpc::Status::OK;
    }

    grpc::Status Heartbeat(grpc::ServerContext*,
                           const sdm_rpc::HeartbeatRequest* request,
                           sdm_rpc::HeartbeatResponse* response) override {
        {
            std::lock_guard lock(mutex_);
            ++heartbeat_count_;
            last_heartbeat_ = *request;
            for (const sdm_rpc::ExpectedReplica& expected :
                 next_expects_) {
                *response->add_expects() = expected;
            }
            next_expects_.clear();
            response->mutable_base_rsp()->set_code(
                to_rpc_code(next_heartbeat_status_.code()));
            response->mutable_base_rsp()->set_msg(
                next_heartbeat_status_.msg());
            next_heartbeat_status_ = Status::OK();
        }
        cv_.notify_all();
        return grpc::Status::OK;
    }

    void set_next_expects(
        std::vector<sdm_rpc::ExpectedReplica> expects) {
        std::lock_guard lock(mutex_);
        next_expects_ = std::move(expects);
    }

    void set_next_heartbeat_status(Status status) {
        std::lock_guard lock(mutex_);
        next_heartbeat_status_ = std::move(status);
    }

    void set_next_register_status(Status status) {
        std::lock_guard lock(mutex_);
        next_register_status_ = std::move(status);
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

    sdm_rpc::HeartbeatRequest last_heartbeat() const {
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
    sdm_rpc::HeartbeatRequest last_heartbeat_;
    std::vector<sdm_rpc::ExpectedReplica> next_expects_;
    Status next_register_status_{Status::OK()};
    Status next_heartbeat_status_{Status::OK()};
};

class FakeSdmServer {
   public:
    bool start() {
        grpc::ServerBuilder builder;
        builder.AddListeningPort("127.0.0.1:0",
                                 grpc::InsecureServerCredentials(), &port_);
        builder.RegisterService(&service_);
        server_ = builder.BuildAndStart();
        return server_ != nullptr && port_ > 0;
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
        if (!fake_sdm_.start()) {
            GTEST_SKIP() << "local gRPC listener is unavailable in this "
                            "environment";
        }
    }

    void TearDown() override {
        fake_sdm_.stop();
        std::error_code ec;
        fs::remove_all(base_dir_, ec);
    }

    NodeAgentConf valid_conf() const {
        NodeAgentConf conf;
        conf.node_id = "node-1";
        conf.ip = "127.0.0.1";
        conf.port = 50050;
        conf.resource_pool = "default";
        conf.dc = "dc1";
        conf.manager_host = "127.0.0.1";
        conf.manager_port = fake_sdm_.port();
        conf.heartbeat_interval_ms = 20;
        conf.register_interval_ms = 30 * 1000;
        conf.first_sync_retry_ms = 10;
        conf.replica_ops = noop_replica_ops();
        return conf;
    }

    NodeAgentReplicaOps noop_replica_ops() const {
        NodeAgentReplicaOps ops;
        ops.list_replicas = [] { return std::vector<ReplicaPtr>{}; };
        ops.create_replica = [](const ReplicaInitParam&) {
            return Status::OK();
        };
        ops.delete_replica = [](const ReplicaID&) { return Status::OK(); };
        return ops;
    }

    NodeAgentReplicaOps replica_manager_ops(
        ReplicaManager* replica_manager) const {
        NodeAgentReplicaOps ops;
        ops.list_replicas = [replica_manager] {
            return replica_manager->get_replicas();
        };
        ops.create_replica = [replica_manager](const ReplicaInitParam& param) {
            return replica_manager->add_replica(param);
        };
        ops.delete_replica = [replica_manager](const ReplicaID& replica_id) {
            return replica_manager->delete_replica(replica_id);
        };
        return ops;
    }

    ReplicaInitParam make_replica_param() const {
        ReplicaID replica_id{101, 7, 0};
        Endpoint endpoint{"127.0.0.1", 50050};
        return ReplicaInitParam{
            replica_id,
            EngineType::MAP,
            endpoint,
            {PeerMember{"node-1", replica_id, endpoint}},
            ReplicaRuntimeOptions{base_dir_.string(), 1000},
        };
    }

    sdm_rpc::ExpectedReplica make_expected_replica(
        sdm_rpc::ExpectedReplicaType type,
        const ReplicaID& replica_id, int32_t port = 50050) const {
        Endpoint endpoint{"127.0.0.1", port};
        sdm_rpc::ExpectedReplica expected;
        expected.set_type(type);
        encode_pb_replica_id(replica_id, *expected.mutable_replica_id());
        expected.set_engine_type(static_cast<int32>(EngineType::MAP));
        encode_pb_peer_member(
            PeerMember{"node-1", replica_id, endpoint},
            *expected.add_initial_members());
        return expected;
    }

    static inline int sequence_{0};
    fs::path base_dir_;
    FakeSdmServer fake_sdm_;
};

// 检测 NodeAgentConf 会拒绝缺少 node_id 或非法 manager_port 的配置。
TEST_F(NodeAgentTest, RejectsInvalidConfig) {
    NodeAgentConf conf = valid_conf();

    EXPECT_TRUE(conf.validate().ok());

    conf.node_id.clear();
    EXPECT_TRUE(conf.validate().fail());

    conf = valid_conf();
    conf.manager_port = 0;
    EXPECT_TRUE(conf.validate().fail());
}

// 检测 NodeAgent 启动后会注册节点，并周期性上报本地 replica 心跳。
TEST_F(NodeAgentTest, StartRegistersNodeAndSendsPeriodicHeartbeat) {
    ReplicaManager replica_manager(base_dir_.string());
    ASSERT_TRUE(replica_manager.add_replica(make_replica_param()).ok());

    NodeAgentConf conf = valid_conf();
    conf.replica_ops = replica_manager_ops(&replica_manager);

    NodeAgent agent;
    Status status = agent.init(std::move(conf));
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

    sdm_rpc::HeartbeatRequest heartbeat =
        fake_sdm_.service().last_heartbeat();
    EXPECT_EQ(heartbeat.node_id(), "node-1");
    EXPECT_EQ(heartbeat.ip(), "127.0.0.1");
    EXPECT_EQ(heartbeat.port(), 50050);
    EXPECT_EQ(heartbeat.resource_pool(), "default");
    EXPECT_EQ(heartbeat.dc(), "dc1");
    ASSERT_EQ(heartbeat.replica_info_list_size(), 1);
    EXPECT_EQ(heartbeat.replica_info_list(0).replica_id().table_id(), 101);
    EXPECT_EQ(heartbeat.replica_info_list(0).replica_id().shard_index(), 7);
    EXPECT_EQ(heartbeat.replica_info_list(0).replica_id().replica_seq(), 0);
    EXPECT_EQ(heartbeat.replica_info_list(0).role(),
              pb::RaftRole::RAFT_ROLE_FOLLOWER);
    EXPECT_EQ(
        heartbeat.replica_info_list(0).status(),
        pb::StorageReplicaStatus::STORAGE_REPLICA_STATUS_READY);
    EXPECT_EQ(heartbeat.replica_info_list(0).member_type(),
              pb::RaftMemberType::RAFT_MEMBER_TYPE_VOTER);
}

// 检测 heartbeat 响应里的 PRESENT/ABSENT 指令会触发本地 replica 创建和删除。
TEST_F(NodeAgentTest, HeartbeatAppliesExpectedReplicaCreateAndDelete) {
    ReplicaID create_id{101, 7, 0};
    ReplicaID delete_id{101, 8, 0};
    fake_sdm_.service().set_next_expects({
        make_expected_replica(sdm_rpc::PRESENT, create_id),
        make_expected_replica(sdm_rpc::ABSENT, delete_id),
    });

    std::mutex mutex;
    std::vector<ReplicaInitParam> created;
    std::vector<ReplicaID> deleted;

    NodeAgentReplicaOps ops;
    ops.list_replicas = [] { return std::vector<ReplicaPtr>{}; };
    ops.create_replica = [&](const ReplicaInitParam& param) {
        std::lock_guard lock(mutex);
        created.push_back(param);
        return Status::ERROR("injected create failure");
    };
    ops.delete_replica = [&](const ReplicaID& replica_id) {
        std::lock_guard lock(mutex);
        deleted.push_back(replica_id);
        return Status::OK();
    };

    NodeAgentConf conf = valid_conf();
    conf.replica_ops = std::move(ops);

    NodeAgent agent;
    Status status = agent.init(std::move(conf));
    ASSERT_TRUE(status.ok()) << status.msg();

    status = agent.start();
    ASSERT_TRUE(status.ok()) << status.msg();
    ASSERT_TRUE(fake_sdm_.service().wait_for_counts(1, 2, Milliseconds(1000)));
    agent.stop();

    std::lock_guard lock(mutex);
    ASSERT_EQ(created.size(), 1U);
    EXPECT_EQ(created[0].replica_id, create_id);
    EXPECT_EQ(created[0].engine_type, EngineType::MAP);
    EXPECT_EQ(created[0].local_endpoint, (Endpoint{"127.0.0.1", 50050}));
    ASSERT_EQ(created[0].members.size(), 1U);
    EXPECT_EQ(created[0].members[0].replica_id, create_id);

    ASSERT_EQ(deleted.size(), 1U);
    EXPECT_EQ(deleted[0], delete_id);
}

// 检测 heartbeat 失败时不会立即重新注册节点，只继续按心跳流程重试。
TEST_F(NodeAgentTest, HeartbeatFailureDoesNotReRegisterImmediately) {
    fake_sdm_.service().set_next_heartbeat_status(
        Status::INVALID_ARGUMENT("node not found"));

    NodeAgentReplicaOps ops;
    ops.list_replicas = [] { return std::vector<ReplicaPtr>{}; };
    ops.create_replica = [](const ReplicaInitParam&) { return Status::OK(); };
    ops.delete_replica = [](const ReplicaID&) { return Status::OK(); };

    NodeAgentConf conf = valid_conf();
    conf.replica_ops = std::move(ops);

    NodeAgent agent;
    Status status = agent.init(std::move(conf));
    ASSERT_TRUE(status.ok()) << status.msg();

    status = agent.start();
    ASSERT_TRUE(status.ok()) << status.msg();
    ASSERT_TRUE(fake_sdm_.service().wait_for_counts(1, 2, Milliseconds(1000)));
    agent.stop();

    EXPECT_EQ(fake_sdm_.service().register_count(), 1);
    EXPECT_GE(fake_sdm_.service().heartbeat_count(), 2);
}

// 检测首次注册失败后，注册任务会按重试间隔再次发起注册。
TEST_F(NodeAgentTest, RegisterTaskRetriesAfterFailedRegister) {
    fake_sdm_.service().set_next_register_status(
        Status::INVALID_ARGUMENT("node register rejected once"));

    NodeAgentConf conf = valid_conf();
    conf.register_interval_ms = 30 * 1000;
    conf.first_sync_retry_ms = 10;

    NodeAgent agent;
    Status status = agent.init(std::move(conf));
    ASSERT_TRUE(status.ok()) << status.msg();

    status = agent.start();
    ASSERT_TRUE(status.ok()) << status.msg();
    ASSERT_TRUE(fake_sdm_.service().wait_for_counts(2, 0, Milliseconds(1000)));
    agent.stop();

    EXPECT_EQ(fake_sdm_.service().register_count(), 2);
}

}  // namespace
}  // namespace adviskv::storage
