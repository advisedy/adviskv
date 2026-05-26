#include "e2e_kv_util.h"

#include <fmt/core.h>
#include <fmt/format.h>

#include <string_view>
#include <utility>

#include "common/log.h"
#include "common/type.h"
#include "e2e_assert.h"
#include "meta/catalog/meta_types.h"
#include "sdk/config.h"
#include "sdk/log.h"
#include "sdk/model.h"

namespace adviskv::e2e {

namespace {
// TODO 这个回头可以放到convert文件里面，不在这放着
sdk::RouteReplicaRole from_pb_role(pb::ReplicaRole role) {
    switch (role) {
        case pb::ReplicaRole::LEADER:
            return sdk::RouteReplicaRole::LEADER;
        case pb::ReplicaRole::FOLLOWER:
            return sdk::RouteReplicaRole::FOLLOWER;
        default:
            return sdk::RouteReplicaRole::UNKNOWN;
    }
}

}  // namespace

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
            if (response.table_state() != (int32)meta::TableState::NORMAL) {
                return CheckResult::fail(fmt::format(
                    "table_state={}, last_error={}", response.table_state(),
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
            if (response.table_state() == (int32)meta::TableState::DELETED) {
                return CheckResult::pass("table state is DELETED");
            }
            return CheckResult::fail(
                fmt::format("GetTable code={}, table_state={}, msg={}",
                            response.base_rsp().code(), response.table_state(),
                            response.base_rsp().msg()));
        },
        &last_error);
}

bool get_route(E2EContext* context, const Key& key, sdk::RouteInfo* route,
               std::string* error) {
    if (route == nullptr) {
        *error = "route is nullptr";
        return false;
    }

    const Options& options = context->options();
    rpc::GetRouteRequest request;
    request.set_db_name(options.db);
    request.set_table_name(options.table);
    request.set_key(key);

    rpc::GetRouteResponse response;
    grpc::ClientContext client_context;
    client_context.set_deadline(std::chrono::system_clock::now() +
                                std::chrono::milliseconds(3000));
    const grpc::Status status =
        context->sdm()->GetRoute(&client_context, request, &response);
    if (!grpc_ok(status, "GetRoute", error)) {
        return false;
    }
    if (response.base_rsp().code() != to_rpc_code(StatusCode::OK)) {
        *error =
            fmt::format("GetRoute failed, code={}, msg={}",
                        response.base_rsp().code(), response.base_rsp().msg());
        return false;
    }

    route->table_id = response.table_id();
    route->shard_id = response.shard_id();
    route->replicas.clear();
    route->replicas.reserve(response.replicas_size());
    for (const auto& replica : response.replicas()) {
        sdk::RouteReplica out;
        out.endpoint =
            Endpoint{replica.endpoint().ip(), replica.endpoint().port()};
        out.role = from_pb_role(replica.role());
        route->replicas.push_back(std::move(out));
    }
    return true;
}

bool wait_route_has_leader(E2EContext* context, const Key& key,
                           sdk::RouteReplica* leader) {
    const Options& options = context->options();
    std::string last_error;
    return eventually(
        "route has leader", options,
        [&]() {
            sdk::RouteInfo route;
            std::string error;
            if (!get_route(context, key, &route, &error)) {
                return CheckResult::fail(error);
            }
            for (const sdk::RouteReplica& replica : route.replicas) {
                if (replica.role == sdk::RouteReplicaRole::LEADER) {
                    if (leader != nullptr) {
                        *leader = replica;
                    }
                    return CheckResult::pass(
                        fmt::format("{}:{} shard={}", replica.endpoint.ip,
                                    replica.endpoint.port, route.shard_id));
                }
            }
            return CheckResult::fail("leader replica not found");
        },
        &last_error);
}

sdk::KVClient make_kv_client(const Options& options) {
    sdk::KVClientConf conf;
    conf.db_name = options.db;
    conf.table_name = options.table;
    conf.sdm_host = options.sdm_host;
    conf.sdm_port = options.sdm_port;
    conf.sdm_timeout_ms = 3000;
    conf.storage_timeout_ms = 3000;
    conf.route_cache_ttl_ms = 0;
    conf.log.level = sdk::LogLevel::INFO;
    conf.log.callback = [](sdk::LogLevel level, std::string_view message) {
        switch (level) {
            case sdk::LogLevel::DEBUG:
                LOG_DEBUG("[adviskv_sdk] {}", message);
                break;
            case sdk::LogLevel::INFO:
                LOG_INFO("[adviskv_sdk] {}", message);
                break;
            case sdk::LogLevel::WARN:
                LOG_WARN("[adviskv_sdk] {}", message);
                break;
            case sdk::LogLevel::ERROR:
                LOG_ERROR("[adviskv_sdk] {}", message);
                break;
        }
    };
    return sdk::KVClient(conf);
}

bool wait_get_value(sdk::KVClient* client, const Options& options,
                    const std::string& key, const std::string& expected) {
    std::string last_error;
    return eventually(
        fmt::format("sdk get {}", key), options,
        [&]() {
            Value value;
            const Status status = client->get(key, &value);
            if (status.fail()) {
                return CheckResult::fail(status.to_string());
            }
            if (value != expected) {
                return CheckResult::fail(fmt::format(
                    "unexpected value '{}', expected '{}'", value, expected));
            }
            return CheckResult::pass(fmt::format("value={}", value));
        },
        &last_error);
}

bool wait_key_not_found(sdk::KVClient* client, const Options& options,
                        const std::string& key) {
    std::string last_error;
    return eventually(
        fmt::format("sdk get deleted {}", key), options,
        [&]() {
            Value value;
            const Status status = client->get(key, &value);
            if (status.code() == StatusCode::KEY_NOT_FOUND) {
                return CheckResult::pass(status.to_string());
            }
            return CheckResult::fail(status.to_string());
        },
        &last_error);
}

std::vector<KV> make_case_kvs(const std::string& prefix, int32_t key_count) {
    std::vector<KV> kvs;
    kvs.reserve(key_count);
    for (int32_t i = 0; i < key_count; ++i) {
        Key key = fmt::format("{}-key-{:03d}", prefix, i);
        Value value = fmt::format("{}-value-{:03d}", prefix, i);
        kvs.push_back(std::make_pair(key, value));
    }
    return kvs;
}

bool validate_key_count(const Options& options) {
    if (options.key_count <= 0) {
        print_fail("validate options", "--key_count must be positive");
        return false;
    }
    return true;
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

bool write_dataset(sdk::KVClient* client, const Options& options,
                   const std::string& prefix) {
    const std::vector<KV> kvs = make_case_kvs(prefix, options.key_count);
    for (int32_t i = 0; i < options.key_count; ++i) {
        const Key key = kvs[i].first;
        const Value value = kvs[i].second;
        if (!wait_status(fmt::format("sdk put {}", key), options,
                         [&]() { return client->put(key, value); })) {
            return false;
        }
    }
    return true;
}

bool verify_dataset(sdk::KVClient* client, const Options& options,
                    const std::string& prefix) {
    const std::vector<KV> kvs = make_case_kvs(prefix, options.key_count);
    for (int32_t i = 0; i < options.key_count; ++i) {
        const Key key = kvs[i].first;
        const Value value = kvs[i].second;
        if (!wait_get_value(client, options, key, value)) {
            return false;
        }
    }
    return true;
}

bool print_current_leader(E2EContext* context, const std::string& key) {
    sdk::RouteReplica leader;
    if (!wait_route_has_leader(context, key, &leader)) {
        return false;
    }
    fmt::print("[ ROUTE_LEADER ] {}:{}\n", leader.endpoint.ip,
               leader.endpoint.port);
    return true;
}

bool run_seed_case(const Options& options, const std::string& title,
                   const std::string& prefix, bool print_leader) {
    fmt::print("{} {}\n", colorize(COLOR::BOLD, title),
               colorize(COLOR::BLUE, "(seed data)"));

    if (!validate_key_count(options)) {
        return false;
    }

    E2EContext context(options);
    if (!prepare_table(&context)) {
        return false;
    }

    sdk::KVClient client = make_kv_client(options);
    if (!write_dataset(&client, options, prefix)) {
        return false;
    }

    if (print_leader && !print_current_leader(&context, prefix + "-key-000")) {
        return false;
    }
    return true;
}

bool run_verify_case(const Options& options, const std::string& title,
                     const std::string& prefix,
                     const std::string& after_key_suffix) {
    fmt::print("{} {}\n", colorize(COLOR::BOLD, title),
               colorize(COLOR::BLUE, "(verify data)"));

    if (!validate_key_count(options)) {
        return false;
    }

    E2EContext context(options);
    if (!wait_table_normal(&context)) {
        return false;
    }
    if (!wait_route_has_leader(&context, prefix + "-key-000", nullptr)) {
        return false;
    }

    sdk::KVClient client = make_kv_client(options);
    if (!verify_dataset(&client, options, prefix)) {
        return false;
    }

    if (after_key_suffix.empty()) return true;

    const std::string after_key = prefix + "-" + after_key_suffix;
    const std::string after_value = prefix + "-value-" + after_key_suffix;
    if (!wait_status(fmt::format("sdk put {}", after_key), options,
                     [&]() { return client.put(after_key, after_value); })) {
        return false;
    }
    return wait_get_value(&client, options, after_key, after_value);
}

}  // namespace adviskv::e2e