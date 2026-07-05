#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <fmt/base.h>
#include <fmt/format.h>

#include "common/confmgr.h"
#include "common/model/replica_role.h"
#include "common/path_util.h"
#include "common/stable_hash.h"
#include "direct_meta_client.h"
#include "meta/catalog/meta_types.h"
#include "sdk/client.h"
#include "sdk/sdm_route_client.h"
#include "common/type.h"
namespace {

using adviskv::DatabaseID;
using adviskv::ShardIndex;
using adviskv::Status;
using adviskv::TableID;
using adviskv::cli::DirectMetaClient;
using adviskv::cli::MetaCliTarget;
using adviskv::cli::TableInfo;

struct AdvisKvCtlConfig {
    MetaCliTarget meta;
    adviskv::sdk::KVClientConf sdk;
};

void print_usage() {
    fmt::print(
            "usage: adviskvctl [--conf=<client.yaml>]\n"
            "\n"
            "Defaults to conf/client.yaml and enters interactive shell.\n");
}

void print_help() {
    fmt::print(
            "DDL commands:\n"
            "  create_db <db> <zone>\n"
            "  drop_db <db>\n"
            "  create_table <db> <table> <shards> <replicas> <resource_pool>\n"
            "  alter_table <db> <table> <replicas>\n"
            "  get_table <db> <table>\n"
            "  wait_table <db> <table> [timeout_ms]\n"
            "\n"
            "KV commands:\n"
            "  put <db> <table> <key> <value>\n"
            "  get <db> <table> <key>\n"
            "  delete <db> <table> <key>\n"
            "\n"
            "Debug commands:\n"
            "  route <db> <table> <key>\n"
            "  routes <db> <table>\n"
            "  status <db> <table>\n"
            "\n"
            "Local demo commands:\n"
            "  demo status\n"
            "  demo start [service]\n"
            "  demo stop [service]\n"
            "  demo kill <service>\n"
            "  demo restart <service>\n"
            "\n"
            "Shell:\n"
            "  help\n"
            "  exit | quit\n");
}

std::vector<std::string> split_tokens(const std::string& line) {
    std::istringstream iss(line);
    std::vector<std::string> tokens;
    std::string token;
    while (iss >> token) {
        tokens.push_back(std::move(token));
    }
    return tokens;
}

bool parse_i32(const std::string& value, int32_t* out) {
    if (out == nullptr)
        return false;
    try {
        size_t pos = 0;
        const int parsed = std::stoi(value, &pos);
        if (pos != value.size())
            return false;
        *out = static_cast<int32_t>(parsed);
        return true;
    } catch (...) {
        return false;
    }
}

void print_status(const Status& status) {
    if (status.ok()) {
        fmt::print("OK\n");
        return;
    }
    fmt::print("ERR code={} msg={}\n", static_cast<int>(status.code()), status.msg());
}

std::string table_state_to_string(adviskv::meta::TableState state) {
    switch (state) {
        case adviskv::meta::TableState::ADDING:
            return "ADDING";
        case adviskv::meta::TableState::NORMAL:
            return "NORMAL";
        case adviskv::meta::TableState::FAILED:
            return "FAILED";
        case adviskv::meta::TableState::DROPPING:
            return "DROPPING";
        case adviskv::meta::TableState::DELETED:
            return "DELETED";
        case adviskv::meta::TableState::ALTERING:
            return "ALTERING";
    }
    return "UNKNOWN";
}

std::string replica_role_to_string(adviskv::ReplicaRole role) {
    switch (role) {
        case adviskv::ReplicaRole::FOLLOWER:
            return "FOLLOWER";
        case adviskv::ReplicaRole::LEADER:
            return "LEADER";
        case adviskv::ReplicaRole::CANDIDATE:
            return "CANDIDATE";
    }
    return "UNKNOWN";
}

std::string quote_value(const std::string& value) {
    std::string quoted;
    quoted.reserve(value.size() + 2);
    quoted.push_back('"');
    for (char ch : value) {
        if (ch == '\\' || ch == '"') {
            quoted.push_back('\\');
        }
        quoted.push_back(ch);
    }
    quoted.push_back('"');
    return quoted;
}

std::optional<std::string> probe_key_for_shard(ShardIndex shard, int32_t shard_count) {
    for (int64_t attempt = 0; attempt < 1000000; ++attempt) {
        std::string key = fmt::format("__adviskvctl_route_probe_{}_{}", shard, attempt);
        if (adviskv::stable_shard_index(key, shard_count) == shard) {
            return key;
        }
    }
    return std::nullopt;
}

bool load_config(const std::string& path, AdvisKvCtlConfig* config) {
    if (config == nullptr)
        return false;
    try {
        adviskv::ConfMgr::get_instance().LoadFromFile(adviskv::path_from_project_root(path).string());

        config->meta.endpoint.ip = CONF_GET_STR("meta_host");
        config->meta.endpoint.port = CONF_GET_INT("meta_port");
        config->meta.timeout_ms = CONF_GET_INT("meta_timeout_ms");

        config->sdk.sdm_host = CONF_GET_STR("sdm_host");
        config->sdk.sdm_port = CONF_GET_INT("sdm_port");
        config->sdk.sdm_timeout_ms = CONF_GET_INT("sdm_timeout_ms");
        config->sdk.storage_timeout_ms = CONF_GET_INT("storage_timeout_ms");
        return config->meta.validate().ok() && config->sdk.validate().ok();
    } catch (const std::exception& e) {
        fmt::print(stderr, "load config failed: {}\n", e.what());
        return false;
    }
}

class AdvisKvCtl {
public:
    explicit AdvisKvCtl(AdvisKvCtlConfig config) : config_(std::move(config)), meta_client_(config_.meta) {
    }

    bool execute(const std::vector<std::string>& tokens) {
        if (tokens.empty())
            return true;
        const std::string& command = tokens[0];
        if (command == "quit" || command == "exit")
            return false;
        if (command == "help") {
            print_help();
            return true;
        }
        if (command == "create_db" || command == "create-db") {
            create_db(tokens);
        } else if (command == "drop_db" || command == "drop-db") {
            drop_db(tokens);
        } else if (command == "create_table" || command == "create-table") {
            create_table(tokens);
        } else if (command == "alter_table" || command == "alter-table" || command == "alter_replica_count" ||
                   command == "alter-replica-count") {
            alter_table(tokens);
        } else if (command == "get_table" || command == "get-table" || command == "status") {
            get_table(tokens);
        } else if (command == "wait_table" || command == "wait-table") {
            wait_table(tokens);
        } else if (command == "put") {
            put(tokens);
        } else if (command == "get") {
            get(tokens);
        } else if (command == "delete" || command == "del") {
            del(tokens);
        } else if (command == "route") {
            route(tokens);
        } else if (command == "routes" || command == "table_routes" || command == "table-routes") {
            routes(tokens);
        } else if (command == "demo") {
            demo(tokens);
        } else {
            fmt::print("unknown command: {}\n", command);
            print_help();
        }
        return true;
    }

private:
    adviskv::sdk::KVClient make_kv_client(const std::string& db, const std::string& table) const {
        adviskv::sdk::KVClientConf conf = config_.sdk;
        conf.db_name = db;
        conf.table_name = table;
        return adviskv::sdk::KVClient(conf);
    }

    adviskv::sdk::SdmRouteClient make_route_client(const std::string& db, const std::string& table) const {
        adviskv::sdk::KVClientConf conf = config_.sdk;
        conf.db_name = db;
        conf.table_name = table;
        return adviskv::sdk::SdmRouteClient(conf);
    }

    void create_db(const std::vector<std::string>& tokens) const {
        if (tokens.size() != 3) {
            fmt::print("usage: create_db <db> <zone>\n");
            return;
        }
        DatabaseID db_id = -1;
        Status status = meta_client_.create_db(tokens[1], tokens[2], &db_id);
        if (status.ok()) {
            fmt::print("OK db_id={}\n", db_id);
        } else {
            print_status(status);
        }
    }

    void drop_db(const std::vector<std::string>& tokens) const {
        if (tokens.size() != 2) {
            fmt::print("usage: drop_db <db>\n");
            return;
        }
        DatabaseID db_id = -1;
        Status status = meta_client_.drop_db(tokens[1], &db_id);
        if (status.ok()) {
            fmt::print("OK db_id={}\n", db_id);
        } else {
            print_status(status);
        }
    }

    void create_table(const std::vector<std::string>& tokens) const {
        if (tokens.size() != 6) {
            fmt::print(
                    "usage: create_table <db> <table> <shards> <replicas> "
                    "<resource_pool>\n");
            return;
        }
        int32_t shards = 0;
        int32_t replicas = 0;
        if (!parse_i32(tokens[3], &shards) || !parse_i32(tokens[4], &replicas)) {
            fmt::print("shards and replicas should be integers\n");
            return;
        }
        TableID table_id = -1;
        Status status = meta_client_.create_table(tokens[1], tokens[2], shards, replicas, &table_id, tokens[5]);
        if (status.ok()) {
            fmt::print("OK table_id={}\n", table_id);
        } else {
            print_status(status);
        }
    }

    void alter_table(const std::vector<std::string>& tokens) const {
        if (tokens.size() != 4) {
            fmt::print("usage: alter_table <db> <table> <replicas>\n");
            return;
        }
        int32_t replicas = 0;
        if (!parse_i32(tokens[3], &replicas)) {
            fmt::print("replicas should be an integer\n");
            return;
        }
        TableID table_id = -1;
        Status status = meta_client_.alter_table_replica_count(tokens[1], tokens[2], replicas, &table_id);
        if (status.ok()) {
            fmt::print("OK table_id={} replica_count={}\n", table_id, replicas);
        } else {
            print_status(status);
        }
    }

    bool fetch_table(const std::string& db, const std::string& table, TableInfo* table_info) const {
        Status status = meta_client_.get_table(db, table, table_info);
        if (status.fail()) {
            print_status(status);
            return false;
        }
        return true;
    }

    void print_table(const TableInfo& table_info) const {
        fmt::print(
                "OK db_id={} table_id={} shard_count={} replica_count={} "
                "table_state={} last_error_msg={}\n",
                table_info.db_id, table_info.table_id, table_info.shard_count, table_info.replica_count,
                table_state_to_string(table_info.table_state), table_info.last_error_msg);
    }

    void get_table(const std::vector<std::string>& tokens) const {
        if (tokens.size() != 3) {
            fmt::print("usage: {} <db> <table>\n", tokens[0]);
            return;
        }
        TableInfo table_info;
        if (fetch_table(tokens[1], tokens[2], &table_info)) {
            print_table(table_info);
        }
    }

    void wait_table(const std::vector<std::string>& tokens) const {
        if (tokens.size() != 3 && tokens.size() != 4) {
            fmt::print("usage: wait_table <db> <table> [timeout_ms]\n");
            return;
        }
        int32_t timeout_ms = 30000;
        if (tokens.size() == 4 && !parse_i32(tokens[3], &timeout_ms)) {
            fmt::print("timeout_ms should be an integer\n");
            return;
        }
        const auto deadline = std::chrono::steady_clock::now() + adviskv::Milliseconds(timeout_ms);
        TableInfo table_info;
        while (std::chrono::steady_clock::now() < deadline) {
            if (!fetch_table(tokens[1], tokens[2], &table_info))
                return;
            if (table_info.table_state == adviskv::meta::TableState::NORMAL) {
                fmt::print("OK table_state=NORMAL\n");
                return;
            }
            if (table_info.table_state == adviskv::meta::TableState::FAILED) {
                print_table(table_info);
                return;
            }
            std::this_thread::sleep_for(adviskv::Milliseconds(500));
        }
        fmt::print("ERR wait_table timeout\n");
        print_table(table_info);
    }

    void put(const std::vector<std::string>& tokens) const {
        if (tokens.size() != 5) {
            fmt::print("usage: put <db> <table> <key> <value>\n");
            return;
        }
        auto client = make_kv_client(tokens[1], tokens[2]);
        print_status(client.put(tokens[3], tokens[4]));
    }

    void get(const std::vector<std::string>& tokens) const {
        if (tokens.size() != 4) {
            fmt::print("usage: get <db> <table> <key>\n");
            return;
        }
        auto client = make_kv_client(tokens[1], tokens[2]);
        std::string value;
        Status status = client.get(tokens[3], &value);
        if (status.ok()) {
            fmt::print("OK value={}\n", quote_value(value));
        } else {
            print_status(status);
        }
    }

    void del(const std::vector<std::string>& tokens) const {
        if (tokens.size() != 4) {
            fmt::print("usage: delete <db> <table> <key>\n");
            return;
        }
        auto client = make_kv_client(tokens[1], tokens[2]);
        print_status(client.del(tokens[3]));
    }

    void route(const std::vector<std::string>& tokens) const {
        if (tokens.size() != 4) {
            fmt::print("usage: route <db> <table> <key>\n");
            return;
        }
        adviskv::sdk::RouteInfo route_info;
        auto route_client = make_route_client(tokens[1], tokens[2]);
        Status status = route_client.get_route(tokens[3], &route_info);
        if (status.fail()) {
            print_status(status);
            return;
        }
        fmt::print("Route\n");
        fmt::print("  table: {}.{}\n", tokens[1], tokens[2]);
        fmt::print("  key:   {}\n", tokens[3]);
        print_route(route_info, "  ");
    }

    void routes(const std::vector<std::string>& tokens) const {
        if (tokens.size() != 3) {
            fmt::print("usage: routes <db> <table>\n");
            return;
        }

        TableInfo table_info;
        if (!fetch_table(tokens[1], tokens[2], &table_info)) {
            return;
        }
        if (table_info.shard_count <= 0) {
            fmt::print("ERR invalid shard_count={}\n", table_info.shard_count);
            return;
        }

        fmt::print("Routes\n");
        fmt::print("  table: {}.{}\n", tokens[1], tokens[2]);
        fmt::print("  id:    table_id={} shard_count={} replica_count={}\n", table_info.table_id,
                   table_info.shard_count, table_info.replica_count);

        auto route_client = make_route_client(tokens[1], tokens[2]);
        for (ShardIndex shard = 0; shard < table_info.shard_count; ++shard) {
            std::optional<std::string> key = probe_key_for_shard(shard, table_info.shard_count);
            if (!key.has_value()) {
                fmt::print("  shard {}\n", shard);
                fmt::print("    ERR failed to find probe key\n");
                continue;
            }

            adviskv::sdk::RouteInfo route_info;
            Status status = route_client.get_route(*key, &route_info);
            if (status.fail()) {
                fmt::print("  shard {}\n", shard);
                fmt::print("    ERR code={} msg={}\n", static_cast<int>(status.code()), status.msg());
                continue;
            }
            print_route(route_info, "  ");
        }
    }

    void print_route(const adviskv::sdk::RouteInfo& route_info, const std::string& indent) const {
        fmt::print("{}shard: table_id={} shard_id={}\n", indent, route_info.table_id, route_info.shard_id);
        fmt::print("{}replicas:\n", indent);
        for (const auto& replica : route_info.replicas) {
            fmt::print("{}  - replica_id={} endpoint={}:{} role={}\n", indent, replica.replica_id.to_string(),
                       replica.endpoint.ip, replica.endpoint.port, replica_role_to_string(replica.role));
        }
    }

    static bool valid_demo_token(const std::string& token) {
        for (char ch : token) {
            if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-' ||
                ch == '_') {
                continue;
            }
            return false;
        }
        return !token.empty();
    }

    void demo(const std::vector<std::string>& tokens) const {
        if (tokens.size() < 2 || tokens.size() > 3) {
            fmt::print("usage: demo <status|start|stop|kill|restart> [service]\n");
            return;
        }
        const std::string& action = tokens[1];
        if (!valid_demo_token(action)) {
            fmt::print("invalid demo action\n");
            return;
        }
        std::string command =
                "cd " + adviskv::project_root_dir().string() + " && python3 scripts/local_cluster.py " + action;
        if (tokens.size() == 3) {
            if (!valid_demo_token(tokens[2])) {
                fmt::print("invalid service name\n");
                return;
            }
            command += " " + tokens[2];
        }
        std::fflush(stdout);
        int rc = std::system(command.c_str());
        if (rc != 0) {
            fmt::print("ERR demo command failed, rc={}\n", rc);
        }
    }

    AdvisKvCtlConfig config_;
    DirectMetaClient meta_client_;
};

bool parse_global_args(int argc, char* argv[], std::string* conf_file, std::vector<std::string>* command_tokens) {
    if (conf_file == nullptr || command_tokens == nullptr)
        return false;
    *conf_file = "conf/client.yaml";
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            print_usage();
            print_help();
            return false;
        }
        if (arg.rfind("--conf=", 0) == 0) {
            *conf_file = arg.substr(std::string("--conf=").size());
            continue;
        }
        command_tokens->push_back(std::move(arg));
    }
    return true;
}

void run_interactive(AdvisKvCtl* ctl) {
    fmt::print("AdvisKV shell. Type \"help\" for commands.\n");
    std::string line;
    while (true) {
        fmt::println("");
        fmt::print("adviskv> ");
        std::cout.flush();
        if (!std::getline(std::cin, line)) {
            fmt::print("\n");
            break;
        }
        if (!ctl->execute(split_tokens(line))) {
            fmt::print("Bye.\n");
            break;
        }
    }
}

}  // namespace

int main(int argc, char* argv[]) {
    std::string conf_file;
    std::vector<std::string> command_tokens;
    if (!parse_global_args(argc, argv, &conf_file, &command_tokens)) {
        print_usage();
        return 1;
    }

    AdvisKvCtlConfig config;
    if (!load_config(conf_file, &config))
        return 1;

    fmt::print("connected meta {}:{}, sdm {}:{}\n", config.meta.endpoint.ip, config.meta.endpoint.port,
               config.sdk.sdm_host, config.sdk.sdm_port);
    AdvisKvCtl ctl(std::move(config));
    if (!command_tokens.empty()) {
        ctl.execute(command_tokens);
        return 0;
    }
    run_interactive(&ctl);
    return 0;
}
