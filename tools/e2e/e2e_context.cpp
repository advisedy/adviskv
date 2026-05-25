#include "e2e_context.h"

#include <fmt/core.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <chrono>

#include "e2e_assert.h"
#include "meta/catalog/meta_types.h"

namespace adviskv::e2e {


E2EContext::E2EContext(const Options& options) : options_(options) {
    const std::string meta_target =
        fmt::format("{}:{}", options_.meta_host, options_.meta_port);
    const std::string sdm_target =
        fmt::format("{}:{}", options_.sdm_host, options_.sdm_port);

    print_step(fmt::format("meta target: {}", meta_target));
    print_step(fmt::format("sdm target: {}", sdm_target));

    auto meta_channel =
        grpc::CreateChannel(meta_target, grpc::InsecureChannelCredentials());
    meta_stub_ = rpc::MetaService::NewStub(meta_channel);

    auto sdm_channel =
        grpc::CreateChannel(sdm_target, grpc::InsecureChannelCredentials());
    sdm_stub_ = rpc::ShardingManagerService::NewStub(sdm_channel);
}

bool create_db(E2EContext* context, std::string* error) {
    const Options& options = context->options();
    rpc::CreateDBRequest request;
    request.set_db_name(options.db);
    request.set_zone(options.zone);

    rpc::CreateDBResponse response;
    grpc::ClientContext client_context;
    client_context.set_deadline(std::chrono::system_clock::now() +
                                std::chrono::milliseconds(options.timeout_ms));
    const grpc::Status status =
        context->meta()->CreateDB(&client_context, request, &response);
    return grpc_ok(status, "CreateDB", error) &&
           base_rsp_ok(response.base_rsp().code(), response.base_rsp().msg(),
                       "CreateDB", error);
}

bool create_table(E2EContext* context, std::string* error) {
    const Options& options = context->options();
    rpc::CreateTableRequest request;
    request.set_db_name(options.db);
    request.set_table_name(options.table);
    request.set_shard_count(options.shard_count);
    request.set_replica_count(options.replica_count);
    request.set_resource_pool(options.resource_pool);

    rpc::CreateTableResponse response;
    grpc::ClientContext client_context;
    client_context.set_deadline(std::chrono::system_clock::now() +
                                std::chrono::milliseconds(options.timeout_ms));
    const grpc::Status status =
        context->meta()->CreateTable(&client_context, request, &response);
    return grpc_ok(status, "CreateTable", error) &&
           base_rsp_ok(response.base_rsp().code(), response.base_rsp().msg(),
                       "CreateTable", error);
}

bool wait_table_normal(E2EContext* context) {
    const Options& options = context->options();
    std::string last_error;
    return eventually(
        "table normal", options,
        [&]() {
            rpc::GetTableRequest request;
            request.set_db_name(options.db);
            request.set_table_name(options.table);

            rpc::GetTableResponse response;
            grpc::ClientContext client_context;
            client_context.set_deadline(std::chrono::system_clock::now() +
                                        std::chrono::milliseconds(3000));
            const grpc::Status status =
                context->meta()->GetTable(&client_context, request, &response);
            std::string error;
            if (!grpc_ok(status, "GetTable", &error)) {
                return CheckResult::fail(error);
            }
            if (response.base_rsp().code() != to_rpc_code(StatusCode::OK)) {
                return CheckResult::fail(
                    fmt::format("GetTable code={}, msg={}",
                                response.base_rsp().code(),
                                response.base_rsp().msg()));
            }
            if (response.table_state() != (int32)meta::TableState::NORMAL) {
                return CheckResult::fail(
                    fmt::format("table_state={}, last_error={}",
                                response.table_state(),
                                response.last_error_msg()));
            }
            return CheckResult::pass(
                fmt::format("db_id={}, table_id={}, shards={}, replicas={}",
                            response.db_id(), response.table_id(),
                            response.shard_count(), response.replica_count()));
        },
        &last_error);
}

}  // namespace adviskv::e2e