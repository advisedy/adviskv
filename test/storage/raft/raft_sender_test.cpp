#include "storage/raft/raft_sender.h"

#include <gtest/gtest.h>

#include <condition_variable>
#include <filesystem>
#include <memory>
#include <thread>

#include "storage/raft/state_machine/kv_state_machine.h"
#include "test/test_env.h"

namespace fs = std::filesystem;

namespace adviskv::storage {
namespace {

class BlockingInstallSnapshotTransport final : public IRaftRpcTransport {
   public:
    Status request_vote(const PeerMember&, const RequestVoteParam&, int32_t,
                        RequestVoteResult&) const override {
        return Status::ERROR("unused");
    }

    Status append_entries(const PeerMember&, const AppendEntriesParam&, int32_t,
                          AppendEntriesResult&) const override {
        return Status::ERROR("unused");
    }

    Status install_snapshot_chunk(
        const PeerMember&, const InstallSnapshotParam& param, int32_t,
        InstallSnapshotResult& result) const override {
        std::unique_lock lock(mutex_);
        ++install_snapshot_call_count_;
        cv_.notify_all();
        cv_.wait(lock, [&] { return released_; });
        result.term = param.term;
        result.status = Status::OK();
        result.snapshot_watermark = param.snapshot_index;
        return Status::OK();
    }

    bool wait_until_call_count(int expected, Milliseconds timeout) const {
        std::unique_lock lock(mutex_);
        return cv_.wait_for(lock, timeout, [&] {
            return install_snapshot_call_count_ >= expected;
        });
    }

    void release() {
        std::lock_guard lock(mutex_);
        released_ = true;
        cv_.notify_all();
    }

    int install_snapshot_call_count() const {
        std::lock_guard lock(mutex_);
        return install_snapshot_call_count_;
    }

   private:
    mutable std::mutex mutex_;
    mutable std::condition_variable cv_;
    mutable bool released_{false};
    mutable int install_snapshot_call_count_{0};
};

class RaftSenderTest : public ::testing::Test {
   protected:
    void SetUp() override {
        base_dir_ =
            adviskv::test::make_unique_test_dir("raft_sender", sequence_++);
        ASSERT_TRUE(fs::create_directories(base_dir_)) << base_dir_.string();

        member_.node_id = "node-2";
        member_.replica_id = ReplicaID{101, 7, 1};
        member_.endpoint = Endpoint{"127.0.0.1", 50052};
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(base_dir_, ec);
    }

    PersistEngine make_snapshot_persist(const ReplicaID& replica_id) const {
        return PersistEngine(base_dir_.string(), replica_id);
    }

    InstallSnapshotParam prepare_snapshot(PersistEngine* persist,
                                          const std::string& value) const {
        EXPECT_NE(persist, nullptr);
        EXPECT_TRUE(persist->init().ok());

        KvStateMachine machine(EngineType::MAP);
        EXPECT_TRUE(
            machine.apply(LogEntry{1, 1, WriteOpType::PUT, "snap-key", value})
                .ok());
        EXPECT_TRUE(persist->write_snapshot(machine).ok());

        return InstallSnapshotParam{
            ReplicaID{101, 7, 0}, member_.replica_id, 3, 1, 1, 0, "", false,
        };
    }

    static inline int sequence_{0};
    fs::path base_dir_;
    PeerMember member_;
};

TEST_F(RaftSenderTest, InstallSnapshotInflightIsNotOwnedBySender) {
    PersistEngine persist = make_snapshot_persist(ReplicaID{101, 7, 0});
    InstallSnapshotParam param = prepare_snapshot(&persist, "snap-value");

    auto transport = std::make_unique<BlockingInstallSnapshotTransport>();
    BlockingInstallSnapshotTransport* raw_transport = transport.get();
    RaftSender sender(std::move(transport), 250);

    InstallSnapshotResult first_result;
    Status first_status = Status::ERROR("not finished");
    std::thread first_sender([&] {
        first_status =
            sender.send_install_snapshot(member_, param, persist, first_result);
    });

    bool first_entered =
        raw_transport->wait_until_call_count(1, Milliseconds(1000));
    if (!first_entered) {
        raw_transport->release();
        first_sender.join();
    }
    ASSERT_TRUE(first_entered);

    InstallSnapshotResult second_result;
    Status second_status = Status::ERROR("not finished");
    std::thread second_sender([&] {
        second_status = sender.send_install_snapshot(member_, param, persist,
                                                     second_result);
    });

    bool second_entered =
        raw_transport->wait_until_call_count(2, Milliseconds(1000));

    raw_transport->release();
    first_sender.join();
    second_sender.join();

    ASSERT_TRUE(second_entered);
    ASSERT_TRUE(first_status.ok()) << first_status.msg();
    ASSERT_TRUE(second_status.ok()) << second_status.msg();
    ASSERT_TRUE(first_result.status.ok()) << first_result.status.to_string();
    ASSERT_TRUE(second_result.status.ok()) << second_result.status.to_string();
    EXPECT_EQ(raw_transport->install_snapshot_call_count(), 2);
}

}  // namespace
}  // namespace adviskv::storage
