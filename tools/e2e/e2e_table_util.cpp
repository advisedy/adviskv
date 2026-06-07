#include "e2e_table_util.h"

#include <fmt/core.h>

#include <chrono>

#include "e2e_assert.h"
#include "meta/catalog/meta_types.h"

namespace adviskv::e2e {

bool create_db(E2EContext* context, std::string* error) {
    const Options& options = context->options();
    rpc::CreateDBRequest request;
    request.set_db_name(options.db);
    request.set_zone(options.zone);

    rpc::CreateDBResponse response;
    grpc::ClientContext client_context;
    client_context.set_deadline(std::chrono::system_clock::now() +
                                std::chrono::milliseconds(options.timeout_ms));
    grpc::Status status =
        context->meta()->CreateDB(&client_context, request, &response);
    if (!grpc_ok(status, "CreateDB", error)) {
        return false;
    }
    int32_t code = response.base_rsp().code();
    if (code == to_rpc_code(StatusCode::OK) ||
        code == to_rpc_code(StatusCode::ALREADY_EXIST)) {
        return true;
    }
    return base_rsp_ok(code, response.base_rsp().msg(), "CreateDB", error);
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
    grpc::Status status =
        context->meta()->CreateTable(&client_context, request, &response);
    if (!grpc_ok(status, "CreateTable", error)) {
        return false;
    }
    int32_t code = response.base_rsp().code();
    if (code == to_rpc_code(StatusCode::OK) ||
        code == to_rpc_code(StatusCode::ALREADY_EXIST)) {
        return true;
    }
    return base_rsp_ok(code, response.base_rsp().msg(), "CreateTable", error);
}

bool drop_table(E2EContext* context, std::string* error) {
    const Options& options = context->options();
    rpc::MetaDropTableRequest request;
    request.set_db_name(options.db);
    request.set_table_name(options.table);

    rpc::MetaDropTableResponse response;
    grpc::ClientContext client_context;
    client_context.set_deadline(std::chrono::system_clock::now() +
                                std::chrono::milliseconds(options.timeout_ms));
    const grpc::Status status =
        context->meta()->DropTable(&client_context, request, &response);
    return grpc_ok(status, "DropTable", error) &&
           base_rsp_ok(response.base_rsp().code(), response.base_rsp().msg(),
                       "DropTable", error);
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
                return CheckResult::fail(fmt::format(
                    "GetTable code={}, msg={}", response.base_rsp().code(),
                    response.base_rsp().msg()));
            }
            if (response.table_state() !=
                pb::MetaTableState::META_TABLE_STATE_NORMAL) {
                return CheckResult::fail(fmt::format(
                    "table_state={}, last_error={}",
                    static_cast<int32_t>(response.table_state()),
                    response.last_error_msg()));
            }
            return CheckResult::pass(
                fmt::format("db_id={}, table_id={}, shards={}, replicas={}",
                            response.db_id(), response.table_id(),
                            response.shard_count(), response.replica_count()));
        },
        &last_error);
}

bool wait_table_deleted(E2EContext* context) {
    const Options& options = context->options();
    std::string last_error;
    return eventually(
        "table deleted", options,
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
            if (response.base_rsp().code() ==
                to_rpc_code(StatusCode::TABLE_NOT_FOUND)) {
                return CheckResult::pass(response.base_rsp().msg());
            }
            if (response.table_state() ==
                pb::MetaTableState::META_TABLE_STATE_DELETED) {
                return CheckResult::pass("table state is DELETED");
            }
            return CheckResult::fail(
                fmt::format("GetTable code={}, table_state={}, msg={}",
                            response.base_rsp().code(),
                            static_cast<int32_t>(response.table_state()),
                            response.base_rsp().msg()));
        },
        &last_error);
}

bool prepare_table(E2EContext* context) {
    std::string error;
    if (!create_db(context, &error)) {
        print_fail("create database", error);
        return false;
    }
    print_pass("create database", context->options().db);

    if (!create_table(context, &error)) {
        print_fail("create table", error);
        return false;
    }
    print_pass("create table", fmt::format("{}.{}", context->options().db,
                                           context->options().table));

    return wait_table_normal(context);
}

bool remove_table(E2EContext* context) {
    std::string error;
    if (!drop_table(context, &error)) {
        print_fail("drop table", error);
        return false;
    }
    print_pass("drop table", fmt::format("{}.{}", context->options().db,
                                         context->options().table));

    return wait_table_deleted(context);
}

}  // namespace adviskv::e2e