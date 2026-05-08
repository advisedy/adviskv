#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <string>
#include <thread>

#include "test/test_env.h"
#include "storage/model/param.h"
#include "storage/persist/persist_engine.h"
#include "storage/raft/state_machine/kv_state_machine.h"
#include "storage/replica/replica_manager.h"

namespace fs = std::filesystem;

namespace adviskv::storage {
namespace {

std::string status_debug_string(const Status& status) {
    return "code=" + std::to_string(static_cast<int>(status.code())) +
           ", msg=" + status.msg();
}

class ReplicaTest : public ::testing::Test {
   protected:
    void SetUp() override {
        base_dir_ = adviskv::test::make_unique_test_dir("replica", sequence_++);
        ASSERT_TRUE(fs::create_directories(base_dir_)) << base_dir_.string();
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(base_dir_, ec);
    }

    ReplicaInitParam make_param() const {
        return ReplicaInitParam{
            .replica_id = replica_id_,
            .engine_type = EngineType::MAP,
            .local_endpoint = Endpoint{"127.0.0.1", 19090},
            .members = {
                PeerMember{
                    .node_id = "node-1",
                    .replica_id = replica_id_,
                    .endpoint = Endpoint{"127.0.0.1", 19090},
                },
            },
            .data_dir = base_dir_.string(),
        };
    }

    Replica* add_single_replica(ReplicaManager& manager) const {
        Status status = manager.add_replica(make_param());
        EXPECT_TRUE(status.ok()) << status_debug_string(status);
        if (!status.ok()) {
            return nullptr;
        }
        return manager.get_replica_by_id(replica_id_);
    }

    Replica* wait_until_leader(ReplicaManager& manager,
                               std::chrono::milliseconds timeout =
                                   std::chrono::milliseconds(1500)) const {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            Replica* replica = manager.get_replica_by_id(replica_id_);
            if (replica && replica->get_role() == ReplicaRole::LEADER) {
                return replica;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return manager.get_replica_by_id(replica_id_);
    }

    static inline int sequence_{0};

    fs::path base_dir_;
    ReplicaID replica_id_{.table_id = 201, .shard_index = 3, .replica_index = 0};
};

TEST_F(ReplicaTest, HandleInstallSnapshotUpdatesReadableState) {
    ReplicaManager manager(base_dir_.string());
    Replica* replica = add_single_replica(manager);
    ASSERT_NE(replica, nullptr);

    auto source_dir = base_dir_ / "source";
    ASSERT_TRUE(fs::create_directories(source_dir)) << source_dir.string();
    ReplicaID source_id{
        .table_id = 201,
        .shard_index = 3,
        .replica_index = 1,
    };
    PersistEngine source_persist(source_dir.string(), source_id);
    ASSERT_TRUE(source_persist.init().ok());

    KvStateMachine source_state(EngineType::MAP);
    ASSERT_TRUE(
        source_state
            .apply(LogEntry{.term = 7,
                            .index = 5,
                            .op_type = WriteOpType::PUT,
                            .key = "hello",
                            .value = "world"})
            .ok());
    ASSERT_TRUE(
        source_state
            .apply(LogEntry{.term = 7,
                            .index = 6,
                            .op_type = WriteOpType::PUT,
                            .key = "foo",
                            .value = "bar"})
            .ok());
    ASSERT_TRUE(source_persist.do_snapshot(source_state).ok());

    uint64 offset = 0;
    Status status = Status::OK();
    while (true) {
        std::string data;
        bool eof = false;
        status = source_persist.read_snapshot_chunk(offset, 8, data, eof);
        ASSERT_TRUE(status.ok()) << status_debug_string(status);

        InstallSnapshotParam param{
            .from_replica_id = source_id,
            .to_replica_id = replica_id_,
            .term = 8,
            .snapshot_index = source_state.apply_index(),
            .snapshot_term = source_state.apply_term(),
            .offset = offset,
            .data = data,
            .done = eof,
        };

        status = replica->handle_install_snapshot(param);
        ASSERT_TRUE(status.ok()) << status_debug_string(status);
        if (eof) break;
        offset += data.size();
    }

    manager.start_tick();
    replica = wait_until_leader(manager);
    ASSERT_NE(replica, nullptr);
    ASSERT_EQ(replica->get_role(), ReplicaRole::LEADER);

    Value value;
    status = replica->get(GetParam{.key = "hello"}, value);
    ASSERT_TRUE(status.ok()) << status_debug_string(status);
    EXPECT_EQ(value, "world");
}

TEST_F(ReplicaTest, SingleReplicaPutAndGetAfterElection) {
    ReplicaManager manager(base_dir_.string());
    Replica* replica = add_single_replica(manager);
    ASSERT_NE(replica, nullptr);

    manager.start_tick();
    replica = wait_until_leader(manager);
    ASSERT_NE(replica, nullptr);
    ASSERT_EQ(replica->get_role(), ReplicaRole::LEADER);

    Status status = replica->put(PutParam{.key = "k1", .value = "v1"});
    ASSERT_TRUE(status.ok()) << status_debug_string(status);

    Value value;
    status = replica->get(GetParam{.key = "k1"}, value);
    ASSERT_TRUE(status.ok()) << status_debug_string(status);
    EXPECT_EQ(value, "v1");
}

TEST_F(ReplicaTest, RecoverRestoresDataFromPersistedState) {
    {
        ReplicaManager manager(base_dir_.string());
        Replica* replica = add_single_replica(manager);
        ASSERT_NE(replica, nullptr);

        manager.start_tick();
        replica = wait_until_leader(manager);
        ASSERT_NE(replica, nullptr);
        ASSERT_EQ(replica->get_role(), ReplicaRole::LEADER);

        Status status = replica->put(PutParam{.key = "persisted", .value = "v"});
        ASSERT_TRUE(status.ok()) << status_debug_string(status);
    }

    ReplicaManager recovered_manager(base_dir_.string());
    Replica* recovered_replica = add_single_replica(recovered_manager);
    ASSERT_NE(recovered_replica, nullptr);

    recovered_manager.recover();
    recovered_manager.start_tick();
    recovered_replica = wait_until_leader(recovered_manager);
    ASSERT_NE(recovered_replica, nullptr);
    ASSERT_EQ(recovered_replica->get_role(), ReplicaRole::LEADER);

    Value value;
    Status status =
        recovered_replica->get(GetParam{.key = "persisted"}, value);
    ASSERT_TRUE(status.ok()) << status_debug_string(status);
    EXPECT_EQ(value, "v");
}

}  // namespace
}  // namespace adviskv::storage
