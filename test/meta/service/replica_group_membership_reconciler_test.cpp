#include <gtest/gtest.h>

#include <vector>

#include "meta/service/replica_group_service.h"
#include "meta/store/meta_store.h"

namespace adviskv::meta {
namespace {

constexpr TableID K_TABLE_ID = 42;
const ShardID K_SHARD_ID{K_TABLE_ID, 0};

void seed_group(MetaStore& store, int32_t target_count, const std::vector<ReplicaPhase>& phases) {
    ASSERT_TRUE(store.write_with([&](MetaStoreTxn& txn) -> Status {
        Table table;
        table.table_id = K_TABLE_ID;
        table.spec.shard_ranges = make_shard_ranges(1);
        table.spec.target_replica_count = target_count;
        RETURN_IF_INVALID_STATUS(txn.put_table(table))

        ReplicaGroup group;
        group.shard_id = K_SHARD_ID;
        group.target_replica_count = target_count;
        for (size_t i = 0; i < phases.size(); ++i) {
            Replica replica;
            replica.replica_id = ReplicaID{K_TABLE_ID, 0, static_cast<ReplicaSeq>(i)};
            replica.state.phase = phases[i];
            group.desired_members.push_back(replica.replica_id);
            RETURN_IF_INVALID_STATUS(txn.put_replica(replica))
        }
        return txn.put_replica_group(group);
    }).ok());
}

ReplicaGroup get_group(MetaStore& store) {
    ReplicaGroupOr group;
    EXPECT_TRUE(store.read_with([&](const MetaStoreTxn& txn) { return txn.get_replica_group(K_SHARD_ID, group); }).ok());
    EXPECT_TRUE(group.has_value());
    return group.value();
}

Replica get_replica(MetaStore& store, ReplicaSeq seq) {
    ReplicaOr replica;
    EXPECT_TRUE(store.read_with([&](const MetaStoreTxn& txn) {
        return txn.get_replica(ReplicaID{K_TABLE_ID, 0, seq}, replica);
    }).ok());
    EXPECT_TRUE(replica.has_value());
    return replica.value();
}

}  // namespace

TEST(ReplicaGroupMembershipReconcilerTest, CountBasedRetirementMarksAllReplicasDeleting) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    seed_group(store, 0, {ReplicaPhase::READY, ReplicaPhase::READY});

    ReplicaGroupService service(&store, nullptr);
    ASSERT_TRUE(service.reconcile_all().ok());

    EXPECT_TRUE(get_group(store).desired_members.empty());
    for (ReplicaSeq seq = 0; seq < 2; ++seq) {
        Replica replica = get_replica(store, seq);
        EXPECT_EQ(replica.state.desired, ReplicaDesired::ABSENT);
        EXPECT_EQ(replica.state.phase, ReplicaPhase::DELETING);
    }
    EXPECT_TRUE(service.reconcile_all().ok());
    EXPECT_TRUE(get_group(store).desired_members.empty());
}

TEST(ReplicaGroupMembershipReconcilerTest, SpecificRetirementOnlyMarksBadReplicaDeleting) {
    MetaStore store{MemoryMetaStoreParam{}};
    ASSERT_TRUE(store.init().ok());
    seed_group(store, 1, {ReplicaPhase::READY, ReplicaPhase::ERROR});

    ReplicaGroupService service(&store, nullptr);
    ASSERT_TRUE(service.reconcile_all().ok());

    EXPECT_EQ(get_group(store).desired_members, (std::vector<ReplicaID>{ReplicaID{K_TABLE_ID, 0, 0}}));
    EXPECT_EQ(get_replica(store, 0).state.desired, ReplicaDesired::PRESENT);
    Replica bad = get_replica(store, 1);
    EXPECT_EQ(bad.state.desired, ReplicaDesired::ABSENT);
    EXPECT_EQ(bad.state.phase, ReplicaPhase::DELETING);
    EXPECT_TRUE(service.reconcile_all().ok());
    EXPECT_EQ(get_group(store).desired_members, (std::vector<ReplicaID>{ReplicaID{K_TABLE_ID, 0, 0}}));
}

}  // namespace adviskv::meta
