#include "storage/replica/read_tracker.h"

#include <gtest/gtest.h>

#include "common/model/model.h"

namespace adviskv::storage {
namespace {

ReplicaID make_voter(ReplicaSeq seq) { return ReplicaID{101, 7, seq}; }

ReadRequestState make_read() {
    ReadRequestState read;
    read.key = "k";
    read.read_index = 10;
    read.read_term = 5;
    read.read_seq = 42;
    read.read_quorum_term = 5;
    read.voter_ids = {make_voter(1), make_voter(2), make_voter(3)};
    return read;
}

TEST(ReadQuorumResponseCountsTest, CountsFreshVoterResponse) {
    ReadRequestState read = make_read();
    EXPECT_TRUE(read_quorum_response_counts(read, make_voter(2), 42, 5));
    EXPECT_EQ(read.confirmed_voters.count(make_voter(2)), 1U);
    EXPECT_EQ(read.confirmed_voters.size(), 1U);
}

TEST(ReadQuorumResponseCountsTest, RejectsStaleSequence) {
    ReadRequestState read = make_read();
    EXPECT_FALSE(read_quorum_response_counts(read, make_voter(2), 41, 5));
    EXPECT_TRUE(read.confirmed_voters.empty());
}

TEST(ReadQuorumResponseCountsTest, RejectsMismatchedTerm) {
    ReadRequestState read = make_read();
    EXPECT_FALSE(read_quorum_response_counts(read, make_voter(2), 42, 6));
    EXPECT_FALSE(read_quorum_response_counts(read, make_voter(2), 42, 4));
    EXPECT_TRUE(read.confirmed_voters.empty());
}

TEST(ReadQuorumResponseCountsTest, RejectsNonVoter) {
    ReadRequestState read = make_read();
    const ReplicaID learner = make_voter(9);
    EXPECT_FALSE(read_quorum_response_counts(read, learner, 42, 5));
    EXPECT_TRUE(read.confirmed_voters.empty());
}

TEST(ReadQuorumResponseCountsTest, DeduplicatesSameVoter) {
    ReadRequestState read = make_read();
    EXPECT_TRUE(read_quorum_response_counts(read, make_voter(2), 42, 5));
    EXPECT_FALSE(read_quorum_response_counts(read, make_voter(2), 42, 5));
    EXPECT_EQ(read.confirmed_voters.size(), 1U);
    EXPECT_TRUE(read_quorum_response_counts(read, make_voter(3), 42, 5));
    EXPECT_EQ(read.confirmed_voters.size(), 2U);
}

TEST(ReadQuorumResponseCountsTest, EmptyVoterSetCountsNothing) {
    ReadRequestState read = make_read();
    read.voter_ids.clear();
    EXPECT_FALSE(read_quorum_response_counts(read, make_voter(1), 42, 5));
}

}
}
