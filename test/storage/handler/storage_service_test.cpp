#include "storage/handler/storage_service_impl.h"

#include <gtest/gtest.h>
#include <grpcpp/server_context.h>

#include <memory>

#include "common/status.h"

namespace adviskv::storage {
namespace {

TEST(StorageServiceTest, ManagementRpcsRequireTestApiFlag) {
    StorageServiceImpl service{std::unique_ptr<ReplicaManager>{}};
    grpc::ServerContext context;

    rpc::CreateReplicaRequest create_request;
    rpc::CreateReplicaResponse create_response;
    ASSERT_TRUE(
        service.CreateReplica(&context, &create_request, &create_response).ok());
    EXPECT_EQ(create_response.base_rsp().code(),
              to_rpc_code(StatusCode::ERROR));
    EXPECT_EQ(create_response.base_rsp().msg(), "enable_test_api is false");

    rpc::DeleteReplicaRequest delete_request;
    rpc::DeleteReplicaResponse delete_response;
    ASSERT_TRUE(
        service.DeleteReplica(&context, &delete_request, &delete_response).ok());
    EXPECT_EQ(delete_response.base_rsp().code(),
              to_rpc_code(StatusCode::ERROR));
    EXPECT_EQ(delete_response.base_rsp().msg(), "enable_test_api is false");

    rpc::GetReplicaInfoRequest info_request;
    rpc::GetReplicaInfoResponse info_response;
    ASSERT_TRUE(
        service.GetReplicaInfo(&context, &info_request, &info_response).ok());
    EXPECT_EQ(info_response.base_rsp().code(), to_rpc_code(StatusCode::ERROR));
    EXPECT_EQ(info_response.base_rsp().msg(), "enable_test_api is false");
}

}  // namespace
}  // namespace adviskv::storage
