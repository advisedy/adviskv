#include <string>

#include <gtest/gtest.h>

#include "common/proto/codec.h"

namespace adviskv {
namespace {

TEST(CodecTest, EndpointRoundTrip) {
    Endpoint in{"127.0.0.1", 8080};
    pb::Endpoint pb_obj;
    codec::encode(in, pb_obj);

    Endpoint out;
    ASSERT_TRUE(codec::decode(pb_obj, out));
    EXPECT_EQ(in, out);
}

TEST(CodecTest, ReplicaIdRoundTrip) {
    ReplicaID in{1, 2, 3};
    pb::ReplicaID pb_obj;
    codec::encode(in, pb_obj);

    ReplicaID out;
    ASSERT_TRUE(codec::decode(pb_obj, out));
    EXPECT_EQ(in, out);
}

TEST(CodecTest, PeerMemberRoundTrip) {
    PeerMember in{"node-1", ReplicaID{1, 2, 3}, Endpoint{"10.0.0.1", 9000}};
    pb::PeerMember pb_obj;
    codec::encode(in, pb_obj);

    PeerMember out;
    ASSERT_TRUE(codec::decode(pb_obj, out));
    EXPECT_EQ(in, out);
}

TEST(CodecTest, RaftMemberRoundTrip) {
    RaftMember in;
    in.peer = PeerMember{"node-1", ReplicaID{1, 2, 3}, Endpoint{"10.0.0.1", 9000}};
    in.member_type = RaftMemberType::LEARNER;
    pb::RaftMember pb_obj;
    codec::encode(in, pb_obj);

    RaftMember out;
    ASSERT_TRUE(codec::decode(pb_obj, out));
    EXPECT_EQ(in, out);
}

TEST(CodecTest, ExpectedReplicaRoundTrip) {
    ExpectedReplica in;
    in.type = ExpectedReplicaType::ADD_MEMBER;
    in.replica_id = ReplicaID{4, 5, 6};
    in.engine_type = EngineType::MAP;
    in.initial_members = {PeerMember{"node-1", ReplicaID{1, 2, 3}, Endpoint{"10.0.0.1", 9000}},
                          PeerMember{"node-2", ReplicaID{1, 2, 4}, Endpoint{"10.0.0.2", 9000}}};
    pb::ExpectedReplica pb_obj;
    codec::encode(in, pb_obj);

    ExpectedReplica out;
    ASSERT_TRUE(codec::decode(pb_obj, out));
    EXPECT_EQ(in.type, out.type);
    EXPECT_EQ(in.replica_id, out.replica_id);
    EXPECT_EQ(in.engine_type, out.engine_type);
    ASSERT_EQ(in.initial_members.size(), out.initial_members.size());
    EXPECT_EQ(in.initial_members, out.initial_members);
}

TEST(CodecTest, LogEntryRoundTrip) {
    LogEntry in{7, 42, WriteOpType::CAS, "key", "value"};
    in.expected = "old_value";
    in.request_id = "req-1";
    in.config_member = PeerMember{"node-1", ReplicaID{1, 2, 3}, Endpoint{"10.0.0.1", 9000}};
    in.config_replica_id = ReplicaID{1, 2, 3};
    pb::LogEntry pb_obj;
    codec::encode(in, pb_obj);

    LogEntry out;
    ASSERT_TRUE(codec::decode(pb_obj, out));
    EXPECT_EQ(in, out);
}

TEST(CodecTest, InvalidEnumDecodeReturnsFalse) {
    // 超出 RaftMemberType::MAX_VALUE 的 member_type 应被拒绝。
    pb::RaftMember pb_member;
    RaftMember member;
    member.peer = PeerMember{"node-1", ReplicaID{1, 2, 3}, Endpoint{"10.0.0.1", 9000}};
    member.member_type = RaftMemberType::VOTER;
    codec::encode(member, pb_member);
    pb_member.set_member_type(to<int32>(RaftMemberType::MAX_VALUE) + 1);
    RaftMember decoded_member;
    EXPECT_FALSE(codec::decode(pb_member, decoded_member));

    // 超出 WriteOpType::MAX_VALUE 的 op_type 应被拒绝。
    pb::LogEntry pb_entry;
    codec::encode(LogEntry{1, 1, WriteOpType::PUT, "k", "v"}, pb_entry);
    pb_entry.set_op_type(to<int32>(WriteOpType::MAX_VALUE) + 1);
    LogEntry decoded_entry;
    EXPECT_FALSE(codec::decode(pb_entry, decoded_entry));

    // 超出 ExpectedReplicaType::MAX_VALUE 的 type 应被拒绝。
    pb::ExpectedReplica pb_expect;
    ExpectedReplica expect;
    expect.type = ExpectedReplicaType::PRESENT;
    expect.replica_id = ReplicaID{1, 2, 3};
    codec::encode(expect, pb_expect);
    pb_expect.set_type(to<int32>(ExpectedReplicaType::MAX_VALUE) + 1);
    ExpectedReplica decoded_expect;
    EXPECT_FALSE(codec::decode(pb_expect, decoded_expect));

    // 非法 StatusCode 的 BaseRsp 应被拒绝。
    pb::BaseRsp pb_rsp;
    pb_rsp.set_code(-1);
    pb_rsp.set_msg("bad");
    Status decoded_status;
    EXPECT_FALSE(codec::decode(pb_rsp, decoded_status));
}

TEST(CodecTest, RspStatusRoundTrip) {
    storage_rpc::PutResponse rsp;
    codec::status_to_base_rsp(&rsp, Status{StatusCode::NOT_LEADER, "leader is 3"});

    Status out = codec::base_rsp_to_status(rsp.base_rsp());
    EXPECT_EQ(out.code(), StatusCode::NOT_LEADER);
    EXPECT_EQ(out.msg(), "leader is 3");
}

TEST(CodecTest, UnknownRspStatusCodeFallsBackToError) {
    pb::BaseRsp pb_rsp;
    pb_rsp.set_code(999999);
    pb_rsp.set_msg("remote says no");
    Status out = codec::base_rsp_to_status(pb_rsp);
    EXPECT_EQ(out.code(), StatusCode::ERROR);
    EXPECT_NE(out.msg().find("999999"), std::string::npos);
    EXPECT_NE(out.msg().find("remote says no"), std::string::npos);
}

}  // namespace
}  // namespace adviskv
