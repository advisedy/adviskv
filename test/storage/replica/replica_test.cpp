#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <future>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include "common/binary/codec.h"
#include "common/crc.h"
#include "common/model/model.h"
#include "common/pause_point.h"
#include "common/proto/codec.h"
#include "common/proto/codec.h"
#include "common/status.h"
#include "storage.grpc.pb.h"
#include "storage/model/param.h"
#include "storage/persist/persist_codecs.h"
#include "storage/persist/persist_engine.h"
#include "common/proto/codec.h"
#include "storage/raft/state_machine/kv_state_machine.h"
#include "storage/replica/replica_manager.h"
#include "storage/persist/snapshot_store.h"
#include "test/storage/persist/snapshot_test_util.h"
#include "test/test_env.h"

namespace fs = std::filesystem;

namespace adviskv::storage {
namespace {

class FakePeerService final : public rpc::StorageService::Service {
public:
    struct AppendBehavior {
        Term term{0};
        bool success{false};
    };

    explicit FakePeerService(ReplicaID self_id) : self_id_(self_id) {}

    void set_own_term(Term term) {
        std::lock_guard lock(mutex_);
        own_term_ = term;
    }

    void set_append_behavior(std::function<AppendBehavior(const AppendEntriesParam&)> fn) {
        std::lock_guard lock(mutex_);
        append_fn_ = std::move(fn);
    }

    int append_call_count() {
        std::lock_guard lock(mutex_);
        return append_call_count_;
    }

    grpc::Status AppendEntries(grpc::ServerContext*, const rpc::AppendEntriesRequest* request,
                               rpc::AppendEntriesResponse* response) override {
        AppendEntriesParam param;
        param.from_replica_id = decode_replica_id(request->from());
        param.to_replica_id = decode_replica_id(request->to());
        param.term = request->term();
        param.prev_log_index = request->prev_log_index();
        param.prev_log_term = request->prev_log_term();
        param.leader_commit = request->leader_commit();
        for (const auto& encoded : request->entries()) {
            LogEntry entry;
            if (!codec::decode(encoded, entry)) {
                return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid test log entry");
            }
            param.entries.push_back(std::move(entry));
        }

        AppendBehavior behavior;
        {
            std::lock_guard lock(mutex_);
            ++append_call_count_;
            behavior = append_fn_ ? append_fn_(param) : AppendBehavior{param.term, true};
        }
        response->set_term(behavior.term);
        response->set_success(behavior.success);
        codec::status_to_base_rsp(response, Status::OK());
        return grpc::Status::OK;
    }

    grpc::Status RequestVote(grpc::ServerContext*, const rpc::RequestVoteRequest* request,
                             rpc::RequestVoteResponse* response) override {
        response->set_term(request->term());
        response->set_vote_granted(true);
        codec::status_to_base_rsp(response, Status::OK());
        return grpc::Status::OK;
    }

    grpc::Status PreVote(grpc::ServerContext*, const rpc::PreVoteRequest* request,
                         rpc::PreVoteResponse* response) override {
        UNUSED(request);
        response->set_term(own_term_);
        response->set_vote_granted(true);
        codec::status_to_base_rsp(response, Status::OK());
        return grpc::Status::OK;
    }

    grpc::Status InstallSnapshot(grpc::ServerContext*, const rpc::InstallSnapshotRequest* request,
                                 rpc::InstallSnapshotResponse* response) override {
        response->set_term(request->term());
        response->set_snapshot_watermark(request->apply_index());
        codec::status_to_base_rsp(response, Status::OK());
        return grpc::Status::OK;
    }

    grpc::Status Put(grpc::ServerContext*, const rpc::PutRequest*, rpc::PutResponse*) override {
        return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "fake peer");
    }
    grpc::Status Get(grpc::ServerContext*, const rpc::GetRequest*, rpc::GetResponse*) override {
        return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "fake peer");
    }
    grpc::Status Delete(grpc::ServerContext*, const rpc::DeleteRequest*, rpc::DeleteResponse*) override {
        return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "fake peer");
    }
    grpc::Status Cas(grpc::ServerContext*, const rpc::CasRequest*, rpc::CasResponse*) override {
        return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "fake peer");
    }
    grpc::Status CreateReplica(grpc::ServerContext*, const rpc::CreateReplicaRequest*,
                               rpc::CreateReplicaResponse*) override {
        return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "fake peer");
    }
    grpc::Status DeleteReplica(grpc::ServerContext*, const rpc::DeleteReplicaRequest*,
                               rpc::DeleteReplicaResponse*) override {
        return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "fake peer");
    }
    grpc::Status GetReplicaInfo(grpc::ServerContext*, const rpc::GetReplicaInfoRequest*,
                                rpc::GetReplicaInfoResponse*) override {
        return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "fake peer");
    }
    grpc::Status TestGetReplicaState(grpc::ServerContext*, const rpc::TestGetReplicaStateRequest*,
                                     rpc::TestGetReplicaStateResponse*) override {
        return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "fake peer");
    }

private:
    static ReplicaID decode_replica_id(const pb::ReplicaID& in) {
        return ReplicaID{in.table_id(), in.shard_index(), in.replica_seq()};
    }

    ReplicaID self_id_;
    Term own_term_{0};
    std::mutex mutex_;
    std::function<AppendBehavior(const AppendEntriesParam&)> append_fn_;
    int append_call_count_{0};
};

class FakePeer {
public:
    FakePeer(const std::string& node_id, ReplicaID replica_id, int port) {
        member_.node_id = node_id;
        member_.replica_id = replica_id;
        member_.endpoint = Endpoint{"127.0.0.1", port};
        service_ = std::make_unique<FakePeerService>(replica_id);
        grpc::ServerBuilder builder;
        builder.AddListeningPort("127.0.0.1:" + std::to_string(port),
                                 grpc::InsecureServerCredentials());
        builder.RegisterService(service_.get());
        server_ = builder.BuildAndStart();
        EXPECT_NE(server_, nullptr);
    }

    ~FakePeer() {
        if (server_) {
            server_->Shutdown();
            server_->Wait();
        }
    }

    FakePeerService* service() { return service_.get(); }
    const PeerMember& member() const { return member_; }
    const ReplicaID& replica_id() const { return member_.replica_id; }

    void stop() {
        if (server_) {
            server_->Shutdown();
            server_->Wait();
            server_.reset();
        }
    }

private:
    PeerMember member_;
    std::unique_ptr<FakePeerService> service_;
    std::unique_ptr<grpc::Server> server_;
};


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
            replica_id_,
            EngineType::MAP,
            Endpoint{"127.0.0.1", 19090},
            {PeerMember{"node-1", replica_id_, Endpoint{"127.0.0.1", 19090}}},
            ReplicaRuntimeOptions{base_dir_.string(), 1000},
        };
    }

    ReplicaPtr add_single_replica(ReplicaManager& manager) const {
        Status status = manager.add_replica(make_param());
        EXPECT_TRUE(status.ok()) << test::status_debug_string(status);
        if (!status.ok()) {
            return nullptr;
        }
        return manager.get_replica_by_id(replica_id_);
    }

    ReplicaPtr wait_until_leader(ReplicaManager& manager,
                                 std::chrono::milliseconds timeout =
                                     std::chrono::milliseconds(1500)) const {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            ReplicaPtr replica = manager.get_replica_by_id(replica_id_);
            if (replica && replica->get_role() == ReplicaRole::LEADER) {
                return replica;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return manager.get_replica_by_id(replica_id_);
    }

    ReplicaPtr start_with_peer(ReplicaManager& manager, const FakePeer& peer) const {
        auto param = make_param();
        param.members.push_back(peer.member());
        Status status = manager.add_replica(param);
        EXPECT_TRUE(status.ok()) << test::status_debug_string(status);
        if (status.fail()) return nullptr;
        manager.start_tick();
        auto replica = wait_until_leader(manager);
        if (replica) {
            EXPECT_EQ(replica->get_role(), ReplicaRole::LEADER);
            status = replica->put(PutParam{"batch-a-baseline", "value", "batch-a-baseline"});
            EXPECT_TRUE(status.ok()) << test::status_debug_string(status);
        }
        return replica;
    }

    fs::path replica_directory() const {
        return base_dir_ / (std::to_string(replica_id_.table_id) + "-" +
                            std::to_string(replica_id_.shard_index));
    }

    static LogEntry make_entry(Term term, LogIndex index, WriteOpType op_type,
                               std::string key, std::string value) {
        return LogEntry{term, index, op_type, std::move(key), std::move(value)};
    }

    static Status install_snapshot_from_source(Replica& replica, PersistEngine& source_persist,
                                               const SnapshotHeaderPtr& snap, size_t chunk_bytes) {
        std::unique_ptr<SnapshotFileReader> reader;
        bool exists = false;
        Status status = source_persist.snapshot().open_reader(reader, exists);
        if (status.fail()) return status;
        if (!exists || !reader) return Status::ERROR("source snapshot missing");
        status = source_persist.snapshot().verify_header(*reader, snap->apply_index, snap->apply_term);
        if (status.fail()) return status;

        uint64 offset = 0;
        while (true) {
            std::string data;
            bool eof = false;
            status = reader->read_chunk(offset, chunk_bytes, data, eof);
            if (status.fail()) return status;

            status = replica.handle_install_snapshot(InstallSnapshotParam{
                replica.get_replica_id(), replica.get_replica_id(), 8,
                snap->apply_index, snap->apply_term, offset, data, eof});
            if (status.fail()) return status;
            if (eof) break;
            offset += data.size();
        }
        return Status::OK();
    }

    static inline int sequence_{0};

    fs::path base_dir_;
    ReplicaID replica_id_{201, 3, 0};
};

TEST_F(ReplicaTest, HandleInstallSnapshotUpdatesReadableState) {
    ReplicaManager manager(base_dir_.string());
    ReplicaPtr replica = add_single_replica(manager);
    ASSERT_NE(replica, nullptr);

    auto source_dir = base_dir_ / "source";
    ASSERT_TRUE(fs::create_directories(source_dir)) << source_dir.string();
    ReplicaID source_id{201, 3, 1};
    PersistEngine source_persist(source_dir.string(), source_id);
    ASSERT_TRUE(source_persist.init().ok());

    KvStateMachine source_state(EngineType::MAP);
    ASSERT_TRUE(
        source_state.apply(LogEntry{7, 5, WriteOpType::PUT, "hello", "world"})
            .ok());
    ASSERT_TRUE(
        source_state.apply(LogEntry{7, 6, WriteOpType::PUT, "foo", "bar"})
            .ok());
    ASSERT_TRUE(test::write_state_machine_snapshot(source_persist.snapshot(), source_state).ok());

    SnapshotHeaderPtr source_snap = std::make_shared<SnapshotHeader>();
    ASSERT_TRUE(source_persist.snapshot().load_header(source_snap).ok());
    ASSERT_NE(source_snap, nullptr);

    Status status = install_snapshot_from_source(*replica, source_persist, source_snap, 8);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    manager.start_tick();
    replica = wait_until_leader(manager);
    ASSERT_NE(replica, nullptr);
    ASSERT_EQ(replica->get_role(), ReplicaRole::LEADER);

    Value value;
    status = replica->get(GetParam{"hello"}, value);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(value, "world");
}

TEST_F(ReplicaTest, HandleInstallSnapshotClearsOldWalBeforeAppendingNewLog) {
    {
        PersistEngine target_persist(base_dir_.string(), replica_id_);
        Status status = target_persist.init();
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = target_persist.wal().append_batch({
            make_entry(1, 1, WriteOpType::PUT, "old-1", "v1"),
            make_entry(1, 2, WriteOpType::PUT, "old-2", "v2"),
            make_entry(1, 3, WriteOpType::PUT, "old-3", "v3"),
            make_entry(1, 4, WriteOpType::PUT, "old-4", "v4"),
            make_entry(1, 5, WriteOpType::PUT, "old-5", "v5"),
            make_entry(1, 6, WriteOpType::PUT, "old-6", "v6"),
            make_entry(1, 7, WriteOpType::PUT, "old-7", "v7"),
            make_entry(1, 8, WriteOpType::PUT, "old-8", "v8"),
        });
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        ASSERT_TRUE(target_persist.close().ok());
    }

    ReplicaManager manager(base_dir_.string());
    ReplicaPtr replica = add_single_replica(manager);
    ASSERT_NE(replica, nullptr);
    manager.recover();

    auto source_dir = base_dir_ / "snapshot_source_clear_wal";
    ASSERT_TRUE(fs::create_directories(source_dir)) << source_dir.string();
    ReplicaID source_id{201, 3, 1};
    PersistEngine source_persist(source_dir.string(), source_id);
    ASSERT_TRUE(source_persist.init().ok());

    KvStateMachine source_state(EngineType::MAP);
    for (LogIndex index = 1; index <= 6; ++index) {
        Status status = source_state.apply(make_entry(
            2, index, WriteOpType::PUT, "snap-" + std::to_string(index),
            "value-" + std::to_string(index)));
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    }
    ASSERT_TRUE(test::write_state_machine_snapshot(source_persist.snapshot(), source_state).ok());

    SnapshotHeaderPtr source_snap = std::make_shared<SnapshotHeader>();
    ASSERT_TRUE(source_persist.snapshot().load_header(source_snap).ok());

    Status status = install_snapshot_from_source(*replica, source_persist, source_snap, 9);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    {
        PersistEngine wal_reader(base_dir_.string(), replica_id_);
        status = wal_reader.init();
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        std::vector<LogEntry> entries;
        status = wal_reader.wal().read_batch(entries);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        EXPECT_TRUE(entries.empty());
    }

    AppendEntriesResult result;
    status = replica->handle_append_entries(
        AppendEntriesParam{replica_id_,
                           replica_id_,
                           8,
                           {make_entry(8, 7, WriteOpType::PUT, "new", "value")},
                           source_state.apply_index(),
                           source_state.apply_term(),
                           7},
        result);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    ASSERT_TRUE(result.success);

    PersistEngine wal_reader(base_dir_.string(), replica_id_);
    status = wal_reader.init();
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    std::vector<LogEntry> entries;
    status = wal_reader.wal().read_batch(entries);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    ASSERT_EQ(entries.size(), 1U);
    EXPECT_EQ(entries[0].index, 7);
    EXPECT_EQ(entries[0].key, "new");
    EXPECT_EQ(entries[0].value, "value");
}

TEST_F(ReplicaTest,
       HandleInstallSnapshotRejectsCoveredBoundaryWithoutClearingWal) {
    std::vector<LogEntry> initial_entries{
        make_entry(1, 1, WriteOpType::PUT, "k1", "v1"),
        make_entry(1, 2, WriteOpType::PUT, "k2", "v2"),
        make_entry(1, 3, WriteOpType::PUT, "k3", "v3"),
        make_entry(1, 4, WriteOpType::PUT, "k4", "v4"),
        make_entry(1, 5, WriteOpType::PUT, "k5", "v5"),
    };
    {
        PersistEngine target_persist(base_dir_.string(), replica_id_);
        Status status = target_persist.init();
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = target_persist.wal().append_batch(initial_entries);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        ASSERT_TRUE(target_persist.close().ok());
    }

    ReplicaManager manager(base_dir_.string());
    ReplicaPtr replica = add_single_replica(manager);
    ASSERT_NE(replica, nullptr);
    manager.recover();

    Status status = replica->handle_install_snapshot(InstallSnapshotParam{
        replica_id_,
        replica_id_,
        2,
        4,
        1,
        0,
        "this chunk should not be accepted",
        true,
    });

    ASSERT_EQ(status.code(), StatusCode::OK) << test::status_debug_string(status);

    PersistEngine wal_reader(base_dir_.string(), replica_id_);
    status = wal_reader.init();
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    std::vector<LogEntry> entries;
    status = wal_reader.wal().read_batch(entries);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(entries, initial_entries);
}

TEST_F(ReplicaTest, HandleInstallSnapshotRejectsStaleChunkAfterTmpReplaced) {
    ReplicaManager manager(base_dir_.string());
    ReplicaPtr replica = add_single_replica(manager);
    ASSERT_NE(replica, nullptr);

    Status status = replica->handle_install_snapshot(InstallSnapshotParam{
        replica_id_, replica_id_, 8, 10, 8, 0, "aaaa", false});
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    status = replica->handle_install_snapshot(InstallSnapshotParam{
        replica_id_, replica_id_, 8, 11, 8, 0, "bbbb", false});
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    status = replica->handle_install_snapshot(InstallSnapshotParam{
        replica_id_, replica_id_, 8, 10, 8, 4, "cccc", false});
    ASSERT_EQ(status.code(), StatusCode::ERROR)
        << test::status_debug_string(status);

    status = replica->handle_install_snapshot(InstallSnapshotParam{
        replica_id_, replica_id_, 8, 11, 8, 4, "dddd", false});
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
}

TEST_F(ReplicaTest, SingleReplicaPutAndGetAfterElection) {
    ReplicaManager manager(base_dir_.string());
    ReplicaPtr replica = add_single_replica(manager);
    ASSERT_NE(replica, nullptr);

    manager.start_tick();
    replica = wait_until_leader(manager);
    ASSERT_NE(replica, nullptr);
    ASSERT_EQ(replica->get_role(), ReplicaRole::LEADER);

    Status status = replica->put(PutParam{"k1", "v1", "single-replica-put"});
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    Value value;
    status = replica->get(GetParam{"k1"}, value);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(value, "v1");
}

TEST_F(ReplicaTest, RequestIdRetryIsIdempotentAndConflictsAreRejected) {
    ReplicaManager manager(base_dir_.string());
    ReplicaPtr replica = add_single_replica(manager);
    ASSERT_NE(replica, nullptr);

    manager.start_tick();
    replica = wait_until_leader(manager);
    ASSERT_NE(replica, nullptr);
    ASSERT_EQ(replica->get_role(), ReplicaRole::LEADER);

    constexpr const char* request_id = "replica-idempotent-request";
    Status status = replica->put(PutParam{"idempotent-key", "v1", request_id});
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    status = replica->put(PutParam{"idempotent-key", "v1", request_id});
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    status = replica->put(PutParam{"idempotent-key", "v2", request_id});
    ASSERT_EQ(status.code(), StatusCode::INVALID_ARGUMENT)
            << test::status_debug_string(status);

    Value value;
    status = replica->get(GetParam{"idempotent-key"}, value);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(value, "v1");

    PersistEngine wal_reader(base_dir_.string(), replica_id_);
    ASSERT_TRUE(wal_reader.init().ok());
    std::vector<LogEntry> entries;
    ASSERT_TRUE(wal_reader.wal().read_batch(entries).ok());

    int request_entry_count = 0;
    for (const LogEntry& entry : entries) {
        if (entry.request_id == request_id) {
            ++request_entry_count;
            EXPECT_EQ(entry.op_type, WriteOpType::PUT);
            EXPECT_EQ(entry.key, "idempotent-key");
            EXPECT_EQ(entry.value, "v1");
        }
    }
    EXPECT_EQ(request_entry_count, 1);
}

TEST_F(ReplicaTest, CasSucceedsAndRejectsMismatchAndMissingKey) {
    ReplicaManager manager(base_dir_.string());
    ReplicaPtr replica = add_single_replica(manager);
    ASSERT_NE(replica, nullptr);

    manager.start_tick();
    replica = wait_until_leader(manager);
    ASSERT_NE(replica, nullptr);
    ASSERT_EQ(replica->get_role(), ReplicaRole::LEADER);

    ASSERT_TRUE(replica->put(PutParam{"cas-key", "v1", "cas-put"}).ok());

    Status status = replica->cas(CasParam{"cas-key", "v1", "v2", "cas-ok"});
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    Value value;
    ASSERT_TRUE(replica->get(GetParam{"cas-key"}, value).ok());
    EXPECT_EQ(value, "v2");

    status = replica->cas(CasParam{"cas-key", "v1", "v3", "cas-mismatch"});
    ASSERT_EQ(status.code(), StatusCode::CONDITION_NOT_MET) << test::status_debug_string(status);
    ASSERT_TRUE(replica->get(GetParam{"cas-key"}, value).ok());
    EXPECT_EQ(value, "v2");

    status = replica->cas(CasParam{"missing-key", "v1", "v2", "cas-missing"});
    ASSERT_EQ(status.code(), StatusCode::KEY_NOT_FOUND) << test::status_debug_string(status);

    status = replica->cas(CasParam{"cas-key", "v1", "v3", "cas-mismatch"});
    ASSERT_EQ(status.code(), StatusCode::CONDITION_NOT_MET) << test::status_debug_string(status);
}

TEST_F(ReplicaTest, RecoveredUnappliedRequestIdRetryDoesNotAppend) {
    constexpr const char* request_id = "recovered-unapplied-request";
    LogEntry recovered = make_entry(1, 1, WriteOpType::PUT, "recovered-key", "v1");
    recovered.request_id = request_id;
    {
        PersistEngine writer(base_dir_.string(), replica_id_);
        ASSERT_TRUE(writer.init().ok());
        ASSERT_TRUE(writer.wal().append_batch({recovered}).ok());
        ASSERT_TRUE(writer.close().ok());
    }

    ReplicaManager manager(base_dir_.string());
    ReplicaPtr replica = add_single_replica(manager);
    ASSERT_NE(replica, nullptr);
    manager.recover();
    manager.start_tick();
    replica = wait_until_leader(manager);
    ASSERT_NE(replica, nullptr);
    ASSERT_EQ(replica->get_role(), ReplicaRole::LEADER);

    Status status = replica->put(PutParam{"recovered-key", "v1", request_id});
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    status = replica->put(PutParam{"recovered-key", "v2", request_id});
    EXPECT_EQ(status.code(), StatusCode::INVALID_ARGUMENT)
            << test::status_debug_string(status);

    Value value;
    status = replica->get(GetParam{"recovered-key"}, value);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(value, "v1");

    PersistEngine wal_reader(base_dir_.string(), replica_id_);
    ASSERT_TRUE(wal_reader.init().ok());
    std::vector<LogEntry> entries;
    ASSERT_TRUE(wal_reader.wal().read_batch(entries).ok());

    int request_entry_count = 0;
    for (const LogEntry& entry : entries) {
        if (entry.request_id == request_id) {
            ++request_entry_count;
            EXPECT_EQ(entry.op_type, WriteOpType::PUT);
            EXPECT_EQ(entry.key, "recovered-key");
            EXPECT_EQ(entry.value, "v1");
        }
    }
    EXPECT_EQ(request_entry_count, 1);
}

TEST_F(ReplicaTest, DuplicateRequestIdInLogKeepsFirstApply) {
    constexpr const char* request_id = "duplicate-log-request";
    LogEntry first = make_entry(1, 1, WriteOpType::PUT, "dup-key", "v1");
    first.request_id = request_id;
    LogEntry second = make_entry(1, 2, WriteOpType::PUT, "dup-key", "v2");
    second.request_id = request_id;
    {
        PersistEngine writer(base_dir_.string(), replica_id_);
        ASSERT_TRUE(writer.init().ok());
        ASSERT_TRUE(writer.wal().append_batch({first, second}).ok());
        ASSERT_TRUE(writer.close().ok());
    }

    ReplicaManager manager(base_dir_.string());
    ReplicaPtr replica = add_single_replica(manager);
    ASSERT_NE(replica, nullptr);
    manager.recover();
    manager.start_tick();
    replica = wait_until_leader(manager);
    ASSERT_NE(replica, nullptr);
    ASSERT_EQ(replica->get_role(), ReplicaRole::LEADER);

    Value value;
    Status status = replica->get(GetParam{"dup-key"}, value);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(value, "v1");
}

TEST_F(ReplicaTest, TakesSnapshotNaturallyAfterEnoughAppliedLogs) {
    ReplicaManager manager(base_dir_.string());
    ReplicaPtr replica = add_single_replica(manager);
    ASSERT_NE(replica, nullptr);

    manager.start_tick();
    replica = wait_until_leader(manager);
    ASSERT_NE(replica, nullptr);
    ASSERT_EQ(replica->get_role(), ReplicaRole::LEADER);

    for (int i = 0; i < 1000; ++i) {
        Status status = replica->put(PutParam{
            "snapshot-auto-" + std::to_string(i),
            "value-" + std::to_string(i),
            "snapshot-auto-request-" + std::to_string(i),
        });
        ASSERT_TRUE(status.ok())
            << test::status_debug_string(status) << ", index=" << i;
    }

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(3);
    Replica::ReplicaStateForTest state{};
    while (std::chrono::steady_clock::now() < deadline) {
        Status status = replica->get_replica_state_for_test(state);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        if (state.snapshot_index > 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    EXPECT_GT(state.snapshot_index, 0);
    EXPECT_GE(state.last_applied, state.snapshot_index);

    Value value;
    Status status = replica->get(GetParam{"snapshot-auto-999"}, value);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(value, "value-999");
}

TEST_F(ReplicaTest, RecoverRestoresDataFromPersistedState) {
    {
        ReplicaManager manager(base_dir_.string());
        ReplicaPtr replica = add_single_replica(manager);
        ASSERT_NE(replica, nullptr);

        manager.start_tick();
        replica = wait_until_leader(manager);
        ASSERT_NE(replica, nullptr);
        ASSERT_EQ(replica->get_role(), ReplicaRole::LEADER);

        Status status = replica->put(PutParam{"persisted", "v", "persisted-request"});
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    }

    ReplicaManager recovered_manager(base_dir_.string());
    ReplicaPtr recovered_replica = add_single_replica(recovered_manager);
    ASSERT_NE(recovered_replica, nullptr);

    recovered_manager.recover();
    recovered_manager.start_tick();
    recovered_replica = wait_until_leader(recovered_manager);
    ASSERT_NE(recovered_replica, nullptr);
    ASSERT_EQ(recovered_replica->get_role(), ReplicaRole::LEADER);

    Value value;
    Status status = recovered_replica->get(GetParam{"persisted"}, value);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(value, "v");
}

TEST_F(ReplicaTest, WalCatchupRecoveryRejectsRequestsUntilEntriesApplied) {
    {
        PersistEngine persist(base_dir_.string(), replica_id_);
        Status status = persist.init();
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

        status = persist.wal().append_batch({
            make_entry(1, 1, WriteOpType::PUT, "k1", "v1"),
            make_entry(1, 2, WriteOpType::PUT, "k2", "v2"),
        });
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = persist.raft_meta().save(RaftMeta{1, std::nullopt});
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        ASSERT_TRUE(persist.close().ok());
    }

    const fs::path wal_path = base_dir_ /
                              (std::to_string(replica_id_.table_id) + "-" +
                               std::to_string(replica_id_.shard_index)) /
                              "wal.log";
    std::ofstream wal(wal_path, std::ios::binary | std::ios::app);
    ASSERT_TRUE(wal.is_open()) << wal_path.string();
    wal.put('\x01');
    wal.close();

    ReplicaManager manager(base_dir_.string());
    ReplicaPtr replica = add_single_replica(manager);
    ASSERT_NE(replica, nullptr);

    manager.recover();
    EXPECT_TRUE(replica->is_recovering());

    Value value;
    Status status = replica->get(GetParam{"k1"}, value);
    EXPECT_EQ(status.code(), StatusCode::IS_RECOVERING);
    status = replica->put(PutParam{"blocked", "value", "blocked-request"});
    EXPECT_EQ(status.code(), StatusCode::IS_RECOVERING);

    AppendEntriesResult result;
    status = replica->handle_append_entries(
        AppendEntriesParam{replica_id_,
                           replica_id_,
                           2,
                           {make_entry(2, 3, WriteOpType::PUT, "k3", "v3")},
                           2,
                           1,
                           3},
        result);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_TRUE(result.success);
    EXPECT_FALSE(replica->is_recovering());

    manager.start_tick();
    replica = wait_until_leader(manager);
    ASSERT_NE(replica, nullptr);
    ASSERT_EQ(replica->get_role(), ReplicaRole::LEADER);

    status = replica->get(GetParam{"k3"}, value);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(value, "v3");
}

TEST_F(ReplicaTest, SnapshotCatchupRecoveryFinishesWhenSnapshotCoversTarget) {
    {
        PersistEngine persist(base_dir_.string(), replica_id_);
        Status status = persist.init();
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

        status = persist.wal().append_batch({
            make_entry(1, 1, WriteOpType::PUT, "k1", "v1"),
            make_entry(1, 2, WriteOpType::PUT, "k2", "v2"),
        });
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        status = persist.raft_meta().save(RaftMeta{1, std::nullopt});
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        ASSERT_TRUE(persist.close().ok());
    }

    const fs::path wal_path = base_dir_ /
                              (std::to_string(replica_id_.table_id) + "-" +
                               std::to_string(replica_id_.shard_index)) /
                              "wal.log";
    std::ofstream wal(wal_path, std::ios::binary | std::ios::app);
    ASSERT_TRUE(wal.is_open()) << wal_path.string();
    wal.put('\x01');
    wal.close();

    ReplicaManager manager(base_dir_.string());
    ReplicaPtr replica = add_single_replica(manager);
    ASSERT_NE(replica, nullptr);

    manager.recover();
    ASSERT_TRUE(replica->is_recovering());
    EXPECT_EQ(replica->get_status(), storage::ReplicaStatus::RECOVERING);

    auto source_dir = base_dir_ / "snapshot_source";
    ASSERT_TRUE(fs::create_directories(source_dir)) << source_dir.string();
    ReplicaID source_id{201, 3, 1};
    PersistEngine source_persist(source_dir.string(), source_id);
    ASSERT_TRUE(source_persist.init().ok());

    KvStateMachine source_state(EngineType::MAP);
    Status status = Status::OK();
    for (LogIndex index = 1; index <= 6; ++index) {
        status = source_state.apply(make_entry(
            2, index, WriteOpType::PUT, "snap-" + std::to_string(index),
            "value-" + std::to_string(index)));
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    }
    ASSERT_TRUE(test::write_state_machine_snapshot(source_persist.snapshot(), source_state).ok());

    SnapshotHeaderPtr source_snap = std::make_shared<SnapshotHeader>();
    ASSERT_TRUE(source_persist.snapshot().load_header(source_snap).ok());

    status = install_snapshot_from_source(*replica, source_persist, source_snap, 7);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    EXPECT_FALSE(replica->is_recovering());
    EXPECT_EQ(replica->get_status(), storage::ReplicaStatus::READY);

    manager.start_tick();
    replica = wait_until_leader(manager);
    ASSERT_NE(replica, nullptr);
    ASSERT_EQ(replica->get_role(), ReplicaRole::LEADER);

    Value value;
    status = replica->get(GetParam{"snap-6"}, value);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(value, "value-6");
}


static int alloc_local_port() {
    int sock = ::socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_GE(sock, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (::bind(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(sock);
        return 0;
    }
    socklen_t len = sizeof(addr);
    if (::getsockname(sock, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
        ::close(sock);
        return 0;
    }
    int port = ntohs(addr.sin_port);
    ::close(sock);
    return port;
}

template <typename Predicate>
static bool wait_for(Predicate pred, std::chrono::milliseconds timeout = std::chrono::milliseconds(2000)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

TEST_F(ReplicaTest, ReadIndexDoesNotCountLearnerAsQuorum) {
    const int fake_voter_port = alloc_local_port();
    const int learner_port = alloc_local_port();
    ASSERT_NE(fake_voter_port, 0);
    ASSERT_NE(learner_port, 0);

    ReplicaID fake_voter_id{replica_id_.table_id, replica_id_.shard_index, 1};
    ReplicaID learner_id{replica_id_.table_id, replica_id_.shard_index, 2};

    FakePeer fake_voter("fake-voter", fake_voter_id, fake_voter_port);
    FakePeer learner("learner", learner_id, learner_port);

    ReplicaManager manager(base_dir_.string());
    ReplicaInitParam param = make_param();
    param.members = {
        PeerMember{"self", replica_id_, Endpoint{"127.0.0.1", 19090}},
        fake_voter.member(),
    };
    ASSERT_TRUE(manager.add_replica(param).ok());
    ReplicaPtr replica = manager.get_replica_by_id(replica_id_);
    ASSERT_NE(replica, nullptr);

    manager.start_tick();
    replica = wait_until_leader(manager);
    ASSERT_NE(replica, nullptr);
    ASSERT_EQ(replica->get_role(), ReplicaRole::LEADER);

    Status status = replica->put(PutParam{"quorum-key", "v1", "quorum-put"});
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    learner.service()->set_append_behavior([](const AppendEntriesParam& p) {
        return FakePeerService::AppendBehavior{p.term, false};
    });
    status = replica->add_member(learner.member());
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    ASSERT_TRUE(wait_for([&]() { return learner.service()->append_call_count() > 0; }))
        << "learner was never contacted; membership did not take effect";

    fake_voter.stop();

    Value value;
    Status get_status;
    bool settled = wait_for([&]() {
        get_status = replica->get(GetParam{"quorum-key"}, value);
        return get_status.code() != StatusCode::NOT_YET_COMMIT &&
               get_status.code() != StatusCode::IS_RECOVERING;
    });
    ASSERT_TRUE(settled) << "get did not settle: " << test::status_debug_string(get_status);

    EXPECT_EQ(get_status.code(), StatusCode::NOT_LEADER)
        << "learner responses must not count toward read quorum, status: "
        << test::status_debug_string(get_status);
}

TEST_F(ReplicaTest, LocalSnapshotKeepsMembershipGrowing) {
    const int learner_port = alloc_local_port();
    ASSERT_NE(learner_port, 0);
    ReplicaID learner_id{replica_id_.table_id, replica_id_.shard_index, 1};
    FakePeer learner("learner", learner_id, learner_port);

    ReplicaManager manager(base_dir_.string());
    ReplicaPtr replica = add_single_replica(manager);
    ASSERT_NE(replica, nullptr);

    manager.start_tick();
    replica = wait_until_leader(manager);
    ASSERT_NE(replica, nullptr);

    for (int i = 0; i < 1100; ++i) {
        Status status = replica->put(PutParam{"member-grow-" + std::to_string(i),
                                              "v", "member-grow-request-" + std::to_string(i)});
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status) << ", i=" << i;
    }
    Replica::ReplicaStateForTest state{};
    bool has_snapshot = wait_for([&]() {
        Status s = replica->get_replica_state_for_test(state);
        return s.ok() && state.snapshot_index > 0;
    });
    ASSERT_TRUE(has_snapshot) << "snapshot was not taken";

    Status status = replica->add_member(learner.member());
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    status = replica->put(PutParam{"after-learner", "v", "after-learner-request"});
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
}

TEST_F(ReplicaTest, FaultedReplicaFailsFastAndRejectsNewRequests) {
    ReplicaManager manager(base_dir_.string());
    ReplicaPtr replica = add_single_replica(manager);
    ASSERT_NE(replica, nullptr);

    manager.start_tick();
    replica = wait_until_leader(manager);
    ASSERT_NE(replica, nullptr);
    ASSERT_EQ(replica->get_role(), ReplicaRole::LEADER);

    Status status = replica->put(PutParam{"fault-key", "v1", "fault-put-1"});
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    const Term cur_term = replica->current_term();
    InstallSnapshotParam first_param{replica_id_,
                                     replica_id_,
                                     cur_term,
                                     100,
                                     cur_term,
                                     0,
                                     "aaaa",
                                     false};
    status = replica->handle_install_snapshot(first_param);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    const fs::path replica_dir =
        base_dir_ / (std::to_string(replica_id_.table_id) + "-" +
                     std::to_string(replica_id_.shard_index));
    const std::string session_prefix = "snapshot.receiving.100.";
    std::string session_file;
    for (const auto& item : fs::directory_iterator(replica_dir)) {
        const std::string name = item.path().filename().string();
        if (name.rfind(session_prefix, 0) == 0 && name.size() > 4 && name.substr(name.size() - 4) == ".tmp") {
            session_file = item.path().string();
            break;
        }
    }
    ASSERT_FALSE(session_file.empty()) << "receive session tmp file not found in " << replica_dir;
    {
        std::ofstream tmp(session_file, std::ios::binary | std::ios::app);
        ASSERT_TRUE(tmp.is_open()) << session_file;
        tmp << "garbage";
    }

    InstallSnapshotParam bad_param{replica_id_,
                                   replica_id_,
                                   cur_term,
                                   100,
                                   cur_term,
                                   4,
                                   "aaaa",
                                   false};
    status = replica->handle_install_snapshot(bad_param);
    ASSERT_TRUE(status.fail()) << "expected snapshot chunk to fail";

    bool faulted = wait_for([&]() {
        Value value;
        Status s = replica->get(GetParam{"fault-key"}, value);
        return s.code() == StatusCode::IS_FAULTED;
    });
    ASSERT_TRUE(faulted) << "replica did not enter faulted state";

    Status put_status = replica->put(PutParam{"after-fault", "v", "after-fault-request"});
    EXPECT_EQ(put_status.code(), StatusCode::IS_FAULTED)
        << test::status_debug_string(put_status);
}

TEST_F(ReplicaTest, ApplyFailureEntersFaulted) {
    ReplicaManager manager(base_dir_.string());
    ReplicaPtr replica = add_single_replica(manager);
    ASSERT_NE(replica, nullptr);

    manager.start_tick();
    replica = wait_until_leader(manager);
    ASSERT_NE(replica, nullptr);
    ASSERT_EQ(replica->get_role(), ReplicaRole::LEADER);

    Status status = replica->put(PutParam{"poison-key", "v1", "poison-put-1"});
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    Value value;
    status = replica->get(GetParam{"poison-key"}, value);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    ASSERT_EQ(value, "v1");

    const Term cur_term = replica->current_term();
    LogEntry poison;
    poison.term = cur_term;
    poison.index = 3;
    poison.op_type = WriteOpType::PROMOTE_VOTER;
    poison.config_replica_id = ReplicaID{replica_id_.table_id, replica_id_.shard_index, 99};

    AppendEntriesParam ae{replica_id_, replica_id_, cur_term, {poison}, /*prev_log_index=*/2,
                          /*prev_log_term=*/cur_term, /*leader_commit=*/3};
    AppendEntriesResult ae_result;
    status = replica->handle_append_entries(ae, ae_result);
    EXPECT_TRUE(status.fail()) << "poisoned config entry must fail its inline apply: "
                               << test::status_debug_string(status);

    bool faulted = wait_for([&]() { return replica->get_status() == ReplicaStatus::FAULTED; });
    ASSERT_TRUE(faulted) << "apply failure must terminate the replica (fault drain), "
                            "not leave it RUNNING with a stuck entry";

    Status put_status = replica->put(PutParam{"after-poison", "v", "after-poison-request"});
    EXPECT_EQ(put_status.code(), StatusCode::IS_FAULTED) << test::status_debug_string(put_status);
    Status get_status = replica->get(GetParam{"poison-key"}, value);
    EXPECT_EQ(get_status.code(), StatusCode::IS_FAULTED) << test::status_debug_string(get_status);
}


TEST_F(ReplicaTest, DispatcherSubmitRejectionDoesNotBreakNormalSend) {
    ReplicaManager manager(base_dir_.string());
    ReplicaPtr replica = add_single_replica(manager);
    ASSERT_NE(replica, nullptr);

    manager.start_tick();
    replica = wait_until_leader(manager);
    ASSERT_NE(replica, nullptr);
    ASSERT_EQ(replica->get_role(), ReplicaRole::LEADER);

    for (int i = 0; i < 10; ++i) {
        Status status = replica->put(PutParam{"normal-" + std::to_string(i), "v",
                                              "normal-request-" + std::to_string(i)});
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    }

    Value value;
    Status status = replica->get(GetParam{"normal-9"}, value);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(value, "v");
}

TEST_F(ReplicaTest, GetFailsBeforeRecoverSucceedsAfter) {
    {
        PersistEngine writer(base_dir_.string(), replica_id_);
        ASSERT_TRUE(writer.init().ok());
        ASSERT_TRUE(writer
                        .wal()
                        .append_batch({make_entry(1, 1, WriteOpType::PUT,
                                                  "recover-order-key", "v1")})
                        .ok());
        ASSERT_TRUE(writer.close().ok());
    }

    ReplicaManager manager(base_dir_.string());
    ReplicaPtr replica = add_single_replica(manager);
    ASSERT_NE(replica, nullptr);

    ReplicaStatus status_before = replica->get_status();
    EXPECT_EQ(status_before, ReplicaStatus::INITIALIZING)
        << "replica must not expose READY before recover";

    manager.recover();
    EXPECT_EQ(replica->get_status(), ReplicaStatus::READY);

    manager.start_tick();
    replica = wait_until_leader(manager);
    ASSERT_NE(replica, nullptr);

    Value value;
    Status status = replica->get(GetParam{"recover-order-key"}, value);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(value, "v1");
}

TEST_F(ReplicaTest, RequestsAreRejectedUntilRecoverCompletes) {
    {
        PersistEngine writer(base_dir_.string(), replica_id_);
        ASSERT_TRUE(writer.init().ok());
        ASSERT_TRUE(writer.wal()
                            .append_batch({make_entry(1, 1, WriteOpType::PUT, "gate-key", "v1")})
                            .ok());
        ASSERT_TRUE(writer.close().ok());
    }

    ReplicaManager manager(base_dir_.string());
    ReplicaPtr replica = add_single_replica(manager);
    ASSERT_NE(replica, nullptr);
    EXPECT_EQ(replica->get_status(), ReplicaStatus::INITIALIZING);

    Status put_status = replica->put(PutParam{"gate-key", "v2", "gate-put"});
    EXPECT_FALSE(put_status.ok()) << "put must be rejected before recover";
    EXPECT_EQ(put_status.code(), StatusCode::NOT_INIT) << test::status_debug_string(put_status);

    Value value;
    Status get_status = replica->get(GetParam{"gate-key"}, value);
    EXPECT_FALSE(get_status.ok()) << "get must be rejected before recover";
    EXPECT_EQ(get_status.code(), StatusCode::NOT_INIT) << test::status_debug_string(get_status);

    EXPECT_TRUE(manager.add_replica(make_param()).ok());
    EXPECT_EQ(replica->get_status(), ReplicaStatus::INITIALIZING)
        << "re-add must not bypass recover";

    manager.recover();
    EXPECT_EQ(replica->get_status(), ReplicaStatus::READY);

    manager.start_tick();
    replica = wait_until_leader(manager);
    ASSERT_NE(replica, nullptr);

    Status status = replica->put(PutParam{"gate-key", "v2", "gate-put"});
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);

    value.clear();
    ASSERT_TRUE(replica->get(GetParam{"gate-key"}, value).ok());
    EXPECT_EQ(value, "v2");
}


class BatchAGate {
public:
    void arrive_and_wait() {
        std::unique_lock lock(mutex_);
        arrived_ = true;
        cv_.notify_all();
        if (!cv_.wait_for(lock, std::chrono::seconds(5), [this] { return released_; })) {
            timed_out_ = true;
        }
    }

    bool wait_until_arrived() {
        std::unique_lock lock(mutex_);
        return cv_.wait_for(lock, std::chrono::seconds(2), [this] { return arrived_; });
    }

    void release() {
        std::lock_guard lock(mutex_);
        released_ = true;
        cv_.notify_all();
    }

    bool timed_out() const {
        std::lock_guard lock(mutex_);
        return timed_out_;
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    bool arrived_{false};
    bool released_{false};
    bool timed_out_{false};
};

class BatchAScopedPause {
public:
    explicit BatchAScopedPause(std::string name) : name_(std::move(name)) {
        testhook::PausePointRegistry::instance().register_point(name_, [gate = gate_]() {
            gate->arrive_and_wait();
            return false;
        });
    }

    ~BatchAScopedPause() {
        release();
        testhook::PausePointRegistry::instance().unregister_point(name_);
        EXPECT_FALSE(gate_->timed_out()) << "pause timed out: " << name_;
    }

    bool wait_until_arrived() { return gate_->wait_until_arrived(); }
    void release() { gate_->release(); }

private:
    std::string name_;
    std::shared_ptr<BatchAGate> gate_{std::make_shared<BatchAGate>()};
};

class BatchAPendingPut {
public:
    BatchAPendingPut(ReplicaPtr replica, FakePeer& peer) : replica_(std::move(replica)) {
        peer.service()->set_append_behavior([gate = gate_](const AppendEntriesParam& param) {
            for (const auto& entry : param.entries) {
                if (entry.request_id == "batch-a-pending") {
                    gate->arrive_and_wait();
                    return FakePeerService::AppendBehavior{param.term, false};
                }
            }
            return FakePeerService::AppendBehavior{param.term, true};
        });
        result_ = std::async(std::launch::async, [replica = replica_]() {
            return replica->put(PutParam{"batch-a-pending", "value", "batch-a-pending"});
        });
    }

    ~BatchAPendingPut() {
        gate_->release();
        if (result_.valid()) result_.wait();
        EXPECT_FALSE(gate_->timed_out()) << "pending follower response timed out";
    }

    bool wait_until_pending() {
        if (!gate_->wait_until_arrived()) return false;
        PreVoteResult result;
        Status status = replica_->handle_pre_vote(
            PreVoteParam{replica_->get_replica_id(), replica_->get_replica_id(),
                         replica_->current_term(), 0, 0}, result);
        return status.ok() && result_.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout;
    }

    void expect_fault_error() {
        ASSERT_EQ(result_.wait_for(std::chrono::milliseconds(500)), std::future_status::ready)
            << "pending proposal must be completed by fault drain, not its 1s timeout";
        Status status = result_.get();
        EXPECT_EQ(status.code(), StatusCode::IS_FAULTED) << test::status_debug_string(status);
        EXPECT_FALSE(gate_->timed_out());
    }

private:
    ReplicaPtr replica_;
    std::shared_ptr<BatchAGate> gate_{std::make_shared<BatchAGate>()};
    std::future<Status> result_;
};

static void expect_batch_a_faulted(const ReplicaPtr& replica) {
    ASSERT_TRUE(wait_for([&] { return replica->get_status() == ReplicaStatus::FAULTED; }));
    Value value;
    EXPECT_EQ(replica->get(GetParam{"batch-a-baseline"}, value).code(), StatusCode::IS_FAULTED);
    EXPECT_EQ(replica->put(PutParam{"new", "value", "after-batch-a-fault"}).code(), StatusCode::IS_FAULTED);
    EXPECT_EQ(replica->del(DelParam{"new", "after-batch-a-fault-del"}).code(), StatusCode::IS_FAULTED);
    EXPECT_EQ(replica->cas(CasParam{"new", "old", "value", "after-batch-a-fault-cas"}).code(),
              StatusCode::IS_FAULTED);
}

TEST_F(ReplicaTest, WalRewriteFailureFailsPendingProposalAndRejectsNewRequests) {
    const int port = alloc_local_port();
    ASSERT_NE(port, 0);
    FakePeer peer("batch-a-peer", ReplicaID{201, 3, 1}, port);
    auto manager = std::make_unique<ReplicaManager>(base_dir_.string());
    auto replica = start_with_peer(*manager, peer);
    ASSERT_NE(replica, nullptr);
    manager.reset();

    BatchAPendingPut pending(replica, peer);
    ASSERT_TRUE(pending.wait_until_pending());
    ASSERT_TRUE(fs::create_directory(replica_directory() / "wal.log.tmp"));

    const Term term = replica->current_term();
    AppendEntriesResult result;
    Status status = replica->handle_append_entries(AppendEntriesParam{
        replica_id_, replica_id_, term + 1,
        {make_entry(term + 1, 3, WriteOpType::PUT, "replacement", "value")},
        2, term, 2}, result);
    EXPECT_EQ(status.code(), StatusCode::ERROR) << test::status_debug_string(status);
    pending.expect_fault_error();
    expect_batch_a_faulted(replica);
}

TEST_F(ReplicaTest, RaftMetaSaveFailureFailsPendingProposalAndRejectsNewRequests) {
    const int port = alloc_local_port();
    ASSERT_NE(port, 0);
    FakePeer peer("batch-a-peer", ReplicaID{201, 3, 1}, port);
    auto manager = std::make_unique<ReplicaManager>(base_dir_.string());
    auto replica = start_with_peer(*manager, peer);
    ASSERT_NE(replica, nullptr);
    manager.reset();

    BatchAPendingPut pending(replica, peer);
    ASSERT_TRUE(pending.wait_until_pending());
    ASSERT_TRUE(fs::create_directory(replica_directory() / "raft_meta.tmp"));

    RequestVoteResult result;
    Status status = replica->handle_request_vote(RequestVoteParam{
        peer.member().replica_id, replica_id_, replica->current_term() + 1,
        3, replica->current_term()}, result);
    EXPECT_EQ(status.code(), StatusCode::ERROR) << test::status_debug_string(status);
    pending.expect_fault_error();
    expect_batch_a_faulted(replica);
}

static std::string encode_single_kv_snapshot(LogIndex index, Term term, const Key& key, const Value& value) {
    constexpr int32 kMagic = SnapshotStore::K_SNAPSHOT_MAGIC;
    constexpr size_t kTrailerBytes = 4 + 4 + 4 + 8 + 4;

    BufferWriter body;
    binary::encode(kMagic, body);
    binary::encode(index, body);
    binary::encode(term, body);
    binary::encode(static_cast<int32>(0), body);
    binary::encode(key, body);
    binary::encode(value, body);

    std::vector<uint8_t> body_bytes = body.take();
    const uint32_t body_crc = compute_crc32(body_bytes.data(), body_bytes.size());

    BufferWriter file;
    file.write_raw(body_bytes.data(), body_bytes.size());
    binary::encode(static_cast<int32>(1), file);
    binary::encode(static_cast<int32>(0), file);
    binary::encode(body_crc, file);
    binary::encode(static_cast<int64>(body_bytes.size() + kTrailerBytes), file);
    binary::encode(kMagic, file);
    auto bytes = file.take();
    return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
}

TEST_F(ReplicaTest, SnapshotRestoreFailureFailsPendingProposalAndRejectsNewRequests) {
    const int port = alloc_local_port();
    ASSERT_NE(port, 0);
    FakePeer peer("batch-a-peer", ReplicaID{201, 3, 1}, port);
    auto manager = std::make_unique<ReplicaManager>(base_dir_.string());
    auto replica = start_with_peer(*manager, peer);
    ASSERT_NE(replica, nullptr);
    manager.reset();

    BatchAPendingPut pending(replica, peer);
    ASSERT_TRUE(pending.wait_until_pending());

    std::string truncated = encode_single_kv_snapshot(100, replica->current_term(), "truncated-key", "tv");
    truncated.resize(truncated.size() - 4);
    Status status = replica->handle_install_snapshot(InstallSnapshotParam{
        replica_id_, replica_id_, replica->current_term(), 100, replica->current_term(),
        0, truncated, true});
    EXPECT_TRUE(status.fail()) << test::status_debug_string(status);

    EXPECT_FALSE(fs::exists(PersistEngine::snapshot_file(replica_directory())));
    pending.expect_fault_error();
    expect_batch_a_faulted(replica);
}

TEST_F(ReplicaTest, SnapshotWorkerWriteFailureFailsPendingProposalAndRejectsNewRequests) {
    const int port = alloc_local_port();
    ASSERT_NE(port, 0);
    FakePeer peer("batch-a-peer", ReplicaID{201, 3, 1}, port);
    auto manager = std::make_unique<ReplicaManager>(ReplicaRuntimeOptions{base_dir_.string(), 1000, 3});
    auto replica = start_with_peer(*manager, peer);
    ASSERT_NE(replica, nullptr);
    manager.reset();

    BatchAScopedPause first_worker("snapshot_worker.candidate_written");
    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE(replica->put(PutParam{"worker-" + std::to_string(i), "value",
                                          "worker-request-" + std::to_string(i)}).ok());
        if (i == 0) {
            ASSERT_TRUE(first_worker.wait_until_arrived());
        }
    }
    BatchAPendingPut pending(replica, peer);
    ASSERT_TRUE(pending.wait_until_pending());
    ASSERT_TRUE(fs::create_directory(replica_directory() / "wal.log.tmp"));
    first_worker.release();
    ASSERT_TRUE(wait_for([&] { return replica->snapshot_index() == 3; }));

    ASSERT_TRUE(wait_for([&] {
        if (replica->get_status() == ReplicaStatus::FAULTED) return true;
        AppendEntriesResult result;
        Status status = replica->handle_append_entries(AppendEntriesParam{
            replica_id_, replica_id_, replica->current_term() - 1, {}, 0, 0, 0}, result);
        EXPECT_TRUE(status.ok() || status.code() == StatusCode::IS_FAULTED)
            << test::status_debug_string(status);
        return replica->get_status() == ReplicaStatus::FAULTED;
    }));
    pending.expect_fault_error();
    expect_batch_a_faulted(replica);
}

TEST_F(ReplicaTest, SnapshotWalTruncateFailureFailsPendingProposalAndRejectsNewRequests) {
    const int port = alloc_local_port();
    ASSERT_NE(port, 0);
    FakePeer peer("batch-a-peer", ReplicaID{201, 3, 1}, port);
    auto manager = std::make_unique<ReplicaManager>(ReplicaRuntimeOptions{base_dir_.string(), 1000, 3});
    auto replica = start_with_peer(*manager, peer);
    ASSERT_NE(replica, nullptr);
    manager.reset();

    BatchAScopedPause worker("snapshot_worker.candidate_written");
    ASSERT_TRUE(replica->put(PutParam{"truncate-trigger", "value", "truncate-trigger"}).ok());
    ASSERT_TRUE(worker.wait_until_arrived());
    BatchAPendingPut pending(replica, peer);
    ASSERT_TRUE(pending.wait_until_pending());
    ASSERT_TRUE(fs::create_directory(replica_directory() / "wal.log.tmp"));
    worker.release();
    pending.expect_fault_error();
    expect_batch_a_faulted(replica);
}

TEST_F(ReplicaTest, FaultedManagerRecoverKeepsOldObjectFaultedAndRebuildRestoresData) {
    auto manager = std::make_unique<ReplicaManager>(base_dir_.string());
    auto replica = add_single_replica(*manager);
    ASSERT_NE(replica, nullptr);
    manager->start_tick();
    replica = wait_until_leader(*manager);
    ASSERT_EQ(replica->get_role(), ReplicaRole::LEADER);
    ASSERT_TRUE(replica->put(PutParam{"batch-a-baseline", "value", "batch-a-baseline"}).ok());
    const Term cur_term = replica->current_term();
    EXPECT_EQ(replica->handle_install_snapshot(InstallSnapshotParam{
                  replica_id_, replica_id_, cur_term, 100, cur_term,
                  0, "aaaa", false}).code(), StatusCode::OK);
    const fs::path replica_dir =
        base_dir_ / (std::to_string(replica_id_.table_id) + "-" +
                     std::to_string(replica_id_.shard_index));
    const std::string session_prefix = "snapshot.receiving.100.";
    std::string session_file;
    for (const auto& item : fs::directory_iterator(replica_dir)) {
        const std::string name = item.path().filename().string();
        if (name.rfind(session_prefix, 0) == 0 && name.size() > 4 && name.substr(name.size() - 4) == ".tmp") {
            session_file = item.path().string();
            break;
        }
    }
    ASSERT_FALSE(session_file.empty()) << "receive session tmp file not found in " << replica_dir;
    {
        std::ofstream tmp(session_file, std::ios::binary | std::ios::app);
        ASSERT_TRUE(tmp.is_open()) << session_file;
        tmp << "garbage";
    }
    EXPECT_EQ(replica->handle_install_snapshot(InstallSnapshotParam{
                  replica_id_, replica_id_, cur_term, 100, cur_term,
                  4, "aaaa", false}).code(), StatusCode::ERROR);
    expect_batch_a_faulted(replica);

    manager->recover();
    EXPECT_EQ(manager->get_replica_by_id(replica_id_), replica);
    expect_batch_a_faulted(replica);
    manager.reset();
    std::weak_ptr<Replica> old = replica;
    replica.reset();
    ASSERT_TRUE(old.expired());

    ReplicaManager rebuilt(base_dir_.string());
    rebuilt.recover();
    replica = rebuilt.get_replica_by_id(replica_id_);
    ASSERT_NE(replica, nullptr);
    EXPECT_EQ(replica->get_status(), ReplicaStatus::READY);
    rebuilt.start_tick();
    replica = wait_until_leader(rebuilt);
    ASSERT_EQ(replica->get_role(), ReplicaRole::LEADER);
    Value value;
    ASSERT_TRUE(replica->get(GetParam{"batch-a-baseline"}, value).ok());
    EXPECT_EQ(value, "value");
    EXPECT_TRUE(replica->put(PutParam{"after-rebuild", "value", "after-rebuild"}).ok());
}

TEST_F(ReplicaTest, DeleteReplicaWaitsForSnapshotWorkerAndRepeatedDestructorIsSafe) {
    for (int iteration = 0; iteration < 10; ++iteration) {
        SCOPED_TRACE(iteration);
        ReplicaManager manager(ReplicaRuntimeOptions{base_dir_.string(), 1000, 3});
        auto replica = add_single_replica(manager);
        ASSERT_NE(replica, nullptr);
        manager.start_tick();
        replica = wait_until_leader(manager);
        ASSERT_EQ(replica->get_role(), ReplicaRole::LEADER);
        BatchAScopedPause worker("snapshot_worker.candidate_written");
        ASSERT_TRUE(replica->put(PutParam{"shutdown-1", "value", "shutdown-request-1"}).ok());
        ASSERT_TRUE(replica->put(PutParam{"shutdown-2", "value", "shutdown-request-2"}).ok());
        ASSERT_TRUE(worker.wait_until_arrived());

        auto deletion = std::async(std::launch::async, [&] { return manager.delete_replica(replica_id_); });
        struct ReleaseOnExit {
            BatchAScopedPause& pause;
            ~ReleaseOnExit() { pause.release(); }
        } release_on_exit{worker};
        ASSERT_TRUE(wait_for([&] { return replica->get_status() == ReplicaStatus::FAULTED; }));
        EXPECT_EQ(deletion.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout)
            << "shutdown returned while snapshot worker still held its context";
        worker.release();
        ASSERT_EQ(deletion.wait_for(std::chrono::seconds(2)), std::future_status::ready);
        EXPECT_TRUE(deletion.get().ok());
        EXPECT_EQ(manager.get_replica_by_id(replica_id_), nullptr);
        EXPECT_FALSE(fs::exists(replica_directory()));
        std::weak_ptr<Replica> old = replica;
        replica.reset();
        EXPECT_TRUE(old.expired());
    }
}


static LogIndex capture_index_of(const std::string& candidate_name) {
    return to<LogIndex>(std::stoll(candidate_name.substr(9, candidate_name.size() - 13)));
}

static fs::path find_session_tmp_file(const fs::path& replica_dir, const std::string& prefix) {
    for (const auto& item : fs::directory_iterator(replica_dir)) {
        const std::string name = item.path().filename().string();
        if (name.rfind(prefix, 0) == 0 && name.size() > 4 && name.substr(name.size() - 4) == ".tmp") {
            return item.path();
        }
    }
    return {};
}

TEST_F(ReplicaTest, RemoteSnapshotBeatsStaleLocalCandidate) {
    const int port = alloc_local_port();
    ASSERT_NE(port, 0);
    FakePeer peer("batch-b-peer", ReplicaID{201, 3, 1}, port);
    auto manager = std::make_unique<ReplicaManager>(ReplicaRuntimeOptions{base_dir_.string(), 1000, 3});
    auto replica = start_with_peer(*manager, peer);
    ASSERT_NE(replica, nullptr);

    BatchAScopedPause worker("snapshot_worker.candidate_written");
    for (int i = 0; i < 4; ++i) {
        Status status = replica->put(
            PutParam{"s-key-" + std::to_string(i), "sv", "s-request-" + std::to_string(i)});
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        if (i == 0) {
            ASSERT_TRUE(worker.wait_until_arrived()) << "worker never reached pause point";
        }
    }

    std::string candidate_name;
    for (const auto& item : fs::directory_iterator(replica_directory())) {
        const std::string name = item.path().filename().string();
        if (name.rfind("snapshot.", 0) == 0 && name.rfind("snapshot.receiving.", 0) != 0 &&
            name.size() > 4 && name.substr(name.size() - 4) == ".tmp") {
            candidate_name = name;
            break;
        }
    }
    ASSERT_FALSE(candidate_name.empty()) << "local candidate for S not found";
    const LogIndex capture_index = capture_index_of(candidate_name);
    ASSERT_GT(capture_index, 0);

    const Term cur_term = replica->current_term();
    const std::string t_payload = encode_single_kv_snapshot(100, cur_term, "t-key", "tv");
    Status status = replica->handle_install_snapshot(InstallSnapshotParam{
        replica_id_, replica_id_, cur_term, 100, cur_term, 0, t_payload, true});
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    peer.service()->set_own_term(cur_term);

    bool t_published = wait_for([&] { return replica->snapshot_index() == 100; });
    ASSERT_TRUE(t_published) << "remote snapshot T was not published";
    replica = wait_until_leader(*manager);
    ASSERT_NE(replica, nullptr);
    ASSERT_TRUE(replica->put(PutParam{"post-t", "v", "post-t-request"}).ok());
    Value value;
    status = replica->get(GetParam{"t-key"}, value);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(value, "tv");
    status = replica->get(GetParam{"s-key-0"}, value);
    EXPECT_EQ(status.code(), StatusCode::KEY_NOT_FOUND)
        << "S data must not be visible after T restore";

    worker.release();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    ASSERT_FALSE(fs::exists(replica_directory() / candidate_name))
        << "stale local candidate must be deleted after monotonic adjudication";

    EXPECT_EQ(replica->snapshot_index(), 100);
    EXPECT_EQ(replica->get_status(), ReplicaStatus::READY);
    status = replica->get(GetParam{"t-key"}, value);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(value, "tv");
}

TEST_F(ReplicaTest, LocalCandidateAndReceiveSessionDoNotShareFiles) {
    const int port = alloc_local_port();
    ASSERT_NE(port, 0);
    FakePeer peer("batch-b-peer", ReplicaID{201, 3, 1}, port);
    auto manager = std::make_unique<ReplicaManager>(ReplicaRuntimeOptions{base_dir_.string(), 1000, 3});
    auto replica = start_with_peer(*manager, peer);
    ASSERT_NE(replica, nullptr);

    BatchAScopedPause worker("snapshot_worker.candidate_written");
    for (int i = 0; i < 4; ++i) {
        Status status = replica->put(
            PutParam{"mix-s-" + std::to_string(i), "sv", "mix-s-request-" + std::to_string(i)});
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        if (i == 0) {
            ASSERT_TRUE(worker.wait_until_arrived());
        }
    }
    std::string candidate_name;
    for (const auto& item : fs::directory_iterator(replica_directory())) {
        const std::string name = item.path().filename().string();
        if (name.rfind("snapshot.", 0) == 0 && name.rfind("snapshot.receiving.", 0) != 0 &&
            name.size() > 4 && name.substr(name.size() - 4) == ".tmp") {
            candidate_name = name;
        }
    }
    ASSERT_FALSE(candidate_name.empty());

    const Term cur_term = replica->current_term();
    Status status = replica->handle_install_snapshot(InstallSnapshotParam{
        replica_id_, replica_id_, cur_term, 100, cur_term, 0, "TTTT", false});
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    const std::string session_prefix = "snapshot.receiving.100.";
    fs::path session_file = find_session_tmp_file(replica_directory(), session_prefix);
    ASSERT_FALSE(session_file.empty()) << "T receive session file not found";
    EXPECT_NE(session_file.filename().string(), candidate_name);
    EXPECT_FALSE(fs::exists(replica_directory() / "snapshot"));
    {
        auto size = fs::file_size(replica_directory() / candidate_name);
        EXPECT_GT(size, 0U);
    }

    status = replica->handle_install_snapshot(InstallSnapshotParam{
        replica_id_, replica_id_, cur_term, 100, cur_term, 4, "UUUU", false});
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(fs::file_size(session_file), 8U);
    peer.service()->set_own_term(cur_term);

    worker.release();
    bool s_published = wait_for(
        [&] { return replica->snapshot_index() >= capture_index_of(candidate_name); });
    ASSERT_TRUE(s_published) << "local candidate S was not published";
    EXPECT_TRUE(fs::exists(replica_directory() / "snapshot"));
    EXPECT_FALSE(fs::exists(replica_directory() / candidate_name));
    EXPECT_TRUE(fs::exists(session_file));
    replica = wait_until_leader(*manager);
    ASSERT_NE(replica, nullptr);
    Value value;
    bool read_ready = wait_for([&] {
        status = replica->get(GetParam{"mix-s-3"}, value);
        return status.ok() || status.code() != StatusCode::READ_INDEX_NOT_READY;
    });
    ASSERT_TRUE(read_ready && status.ok()) << test::status_debug_string(status);
    EXPECT_EQ(value, "sv");
}


TEST_F(ReplicaTest, GetNeverMarksSnapshotInflight) {
    const int port = alloc_local_port();
    ASSERT_NE(port, 0);
    FakePeer peer("batch-c-peer", ReplicaID{201, 3, 1}, port);
    auto manager = std::make_unique<ReplicaManager>(ReplicaRuntimeOptions{base_dir_.string(), 1000, 100});
    auto replica = start_with_peer(*manager, peer);
    ASSERT_NE(replica, nullptr);

    Value value;
    for (int i = 0; i < 5; ++i) {
        Status status = replica->get(GetParam{"batch-a-baseline"}, value);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
        EXPECT_EQ(value, "value");
        EXPECT_EQ(replica->snapshot_inflight_index_for_test(peer.replica_id()), 0u)
            << "linearizable get must not mark snapshot inflight";
    }

    const LogIndex next_before = replica->next_index_for_test(peer.replica_id());
    for (int i = 0; i < 5; ++i) {
        Status status = replica->get(GetParam{"batch-a-baseline"}, value);
        ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    }
    EXPECT_EQ(replica->next_index_for_test(peer.replica_id()), next_before)
        << "read quorum responses must not move next/match index";
    EXPECT_EQ(replica->snapshot_inflight_index_for_test(peer.replica_id()), 0u);

    peer.stop();
    Status failed;
    bool settled = wait_for([&] {
        failed = replica->get(GetParam{"batch-a-baseline"}, value);
        return failed.code() != StatusCode::NOT_YET_COMMIT && failed.code() != StatusCode::IS_RECOVERING;
    });
    ASSERT_TRUE(settled) << test::status_debug_string(failed);
    EXPECT_EQ(failed.code(), StatusCode::NOT_LEADER)
        << "get without reachable quorum must fail as NOT_LEADER, status: "
        << test::status_debug_string(failed);
    EXPECT_EQ(replica->snapshot_inflight_index_for_test(peer.replica_id()), 0u)
        << "failed get must not leave orphan inflight marker";
}

TEST_F(ReplicaTest, GetCompletesWithOneBlockedFollower) {
    const int blocked_port = alloc_local_port();
    const int healthy_port = alloc_local_port();
    ASSERT_NE(blocked_port, 0);
    ASSERT_NE(healthy_port, 0);

    ReplicaID blocked_id{replica_id_.table_id, replica_id_.shard_index, 1};
    ReplicaID healthy_id{replica_id_.table_id, replica_id_.shard_index, 2};
    FakePeer blocked_peer("blocked-follower", blocked_id, blocked_port);
    FakePeer healthy_peer("healthy-follower", healthy_id, healthy_port);

    ReplicaManager manager(base_dir_.string());
    ReplicaInitParam param = make_param();
    param.members = {
        PeerMember{"self", replica_id_, Endpoint{"127.0.0.1", 19091}},
        blocked_peer.member(),
        healthy_peer.member(),
    };
    ASSERT_TRUE(manager.add_replica(param).ok());
    auto replica = manager.get_replica_by_id(replica_id_);
    ASSERT_NE(replica, nullptr);
    manager.start_tick();
    replica = wait_until_leader(manager);
    ASSERT_NE(replica, nullptr);

    BatchAGate blocked_gate;
    blocked_peer.service()->set_append_behavior([&blocked_gate](const AppendEntriesParam& p) {
        blocked_gate.arrive_and_wait();
        return FakePeerService::AppendBehavior{p.term, true};
    });

    ASSERT_TRUE(replica->put(PutParam{"c-key", "cv", "c-key-request"}).ok());

    Value value;
    bool read_done = wait_for([&] {
        Status s = replica->get(GetParam{"c-key"}, value);
        return s.ok() || (s.code() != StatusCode::NOT_YET_COMMIT &&
                          s.code() != StatusCode::IS_RECOVERING &&
                          s.code() != StatusCode::NOT_LEADER);
    });
    Status final_status = replica->get(GetParam{"c-key"}, value);
    bool done = read_done || final_status.ok();
    ASSERT_TRUE(done) << test::status_debug_string(final_status);
    Value value2;
    Status ok_status = replica->get(GetParam{"c-key"}, value2);
    ASSERT_TRUE(ok_status.ok()) << test::status_debug_string(ok_status);
    EXPECT_EQ(value2, "cv");
    blocked_gate.release();
}


TEST_F(ReplicaTest, LocalPublishCrashRecoversWithSnapshotAsAuthority) {
    auto crash_child = [this]() {
        ::setenv("ADVISKV_ENABLE_CRASH_POINT", "persist.after_local_snapshot_publish", 1);
        PersistEngine persist(base_dir_.string(), replica_id_);
        if (persist.init().fail()) ::_exit(3);
        if (persist.wal().append_batch({
                LogEntry{1, 1, WriteOpType::PUT, "k1", "v1"},
                LogEntry{1, 2, WriteOpType::PUT, "k2", "v2"},
                LogEntry{1, 3, WriteOpType::PUT, "k3", "v3"},
            }).fail()) ::_exit(4);
        SnapshotData snap;
        snap.header.apply_index = 2;
        snap.header.apply_term = 1;
        snap.kvs = {KV{"k1", "v1"}, KV{"k2", "v2"}};
        if (persist.snapshot().write_local_snapshot_candidate(snap, {}, 2).fail()) ::_exit(5);
        bool published = false;
        if (persist.snapshot().publish_local_snapshot(2, published).fail() || !published) ::_exit(6);
        ::_exit(0);
    };

    ASSERT_EXIT((crash_child()), ::testing::ExitedWithCode(137), "");

    PersistEngine recovered(base_dir_.string(), replica_id_);
    ASSERT_TRUE(recovered.init().ok());
    PersistEngine::RecoverResult result;
    Status status = recovered.recover(result);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    ASSERT_NE(result.snapshot, nullptr);
    EXPECT_EQ(result.snapshot->apply_index, 2);
    ASSERT_EQ(result.wal_entries.size(), 1U);
    EXPECT_EQ(result.wal_entries[0].index, 3);
    EXPECT_FALSE(result.need_recover);

    std::vector<LogEntry> reread;
    ASSERT_TRUE(recovered.wal().read_batch(reread).ok());
    ASSERT_EQ(reread.size(), 1U);
    EXPECT_EQ(reread[0].key, "k3");
}

TEST_F(ReplicaTest, RemotePublishCrashRecoversWithSnapshotAsAuthority) {
    auto crash_child = [this]() {
        ::setenv("ADVISKV_ENABLE_CRASH_POINT", "persist.after_remote_snapshot_publish", 1);
        PersistEngine persist(base_dir_.string(), replica_id_);
        if (persist.init().fail()) ::_exit(3);
        if (persist.wal().append_batch({
                LogEntry{1, 1, WriteOpType::PUT, "old-1", "v1"},
                LogEntry{1, 2, WriteOpType::PUT, "old-2", "v2"},
            }).fail()) ::_exit(4);
        InstallSnapshotParam param;
        param.from_replica_id = replica_id_;
        param.to_replica_id = replica_id_;
        param.term = 7;
        param.snapshot_index = 100;
        param.snapshot_term = 7;
        param.offset = 0;
        param.done = true;
        param.data = encode_single_kv_snapshot(100, 7, "snap-key", "snap-value");

        if (persist.snapshot().append_snapshot_chunk(param).fail()) ::_exit(5);
        if (persist.snapshot().finish_snapshot_receive(param).fail()) ::_exit(6);
        ::_exit(0);
    };

    ASSERT_EXIT((crash_child()), ::testing::ExitedWithCode(137), "");

    PersistEngine recovered(base_dir_.string(), replica_id_);
    ASSERT_TRUE(recovered.init().ok());
    PersistEngine::RecoverResult result;
    Status status = recovered.recover(result);
    ASSERT_TRUE(status.ok()) << test::status_debug_string(status);
    ASSERT_NE(result.snapshot, nullptr);
    EXPECT_EQ(result.snapshot->apply_index, 100);
    EXPECT_EQ(result.snapshot->apply_term, 7);
    EXPECT_TRUE(result.wal_entries.empty());
    EXPECT_FALSE(result.need_recover);
}

}  // namespace
}  // namespace adviskv::storage
