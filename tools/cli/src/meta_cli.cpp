#include <fmt/format.h>
#include <yaml-cpp/yaml.h>

#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "direct_meta_client.h"

namespace {

using adviskv::DatabaseID;
using adviskv::Status;
using adviskv::TableID;
using adviskv::cli::DirectMetaClient;
using adviskv::cli::MetaCliTarget;
using adviskv::cli::TableInfo;

void print_usage() {
    fmt::print(
        "usage: meta_cli --conf <conf.yaml>\n"
        "   or: meta_cli --host <host> --port <port> [--timeout_ms <ms>]\n");
}

void print_help() {
    fmt::print("commands:\n");
    fmt::print("  create_db <db_name> <zone>\n");
    fmt::print("  create_table <db_name> <table_name> <shard_count> <replica_count>\n");
    fmt::print("  get_table <db_name> <table_name>\n");
    fmt::print("  help\n");
    fmt::print("  quit\n");
}

bool parse_i32(const std::string& value, int32_t* out) {
    if (out == nullptr) return false;
    try {
        size_t pos = 0;
        const int parsed = std::stoi(value, &pos);
        if (pos != value.size()) return false;
        *out = static_cast<int32_t>(parsed);
        return true;
    } catch (...) {
        return false;
    }
}

bool load_conf(const std::string& path, MetaCliTarget* target) {
    if (target == nullptr) return false;
    try {
        YAML::Node conf = YAML::LoadFile(path);
        target->endpoint.ip = conf["host"].as<std::string>();
        target->endpoint.port = conf["port"].as<int32_t>();
        if (conf["timeout_ms"]) {
            target->timeout_ms = conf["timeout_ms"].as<int32_t>();
        }
        return true;
    } catch (const std::exception& e) {
        fmt::print(stderr, "load conf failed: {}\n", e.what());
        return false;
    }
}

bool parse_args(int argc, char* argv[], MetaCliTarget* target) {
    if (target == nullptr) return false;
    for (int i = 1; i < argc; i += 2) {
        if (i + 1 >= argc) return false;
        const std::string flag = argv[i];
        const std::string value = argv[i + 1];
        if (flag == "--conf") {
            if (!load_conf(value, target)) return false;
        } else if (flag == "--host") {
            target->endpoint.ip = value;
        } else if (flag == "--port") {
            if (!parse_i32(value, &target->endpoint.port)) return false;
        } else if (flag == "--timeout_ms") {
            if (!parse_i32(value, &target->timeout_ms)) return false;
        } else {
            return false;
        }
    }
    return target->validate().ok();
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

void print_status(const Status& status) {
    if (status.ok()) {
        fmt::print("OK\n");
        return;
    }
    fmt::print("ERR code={} msg={}\n", static_cast<int>(status.code()),
               status.msg());
}

}  // namespace

int main(int argc, char* argv[]) {
    MetaCliTarget target;
    if (!parse_args(argc, argv, &target)) {
        print_usage();
        return 1;
    }

    fmt::print("connected target {}:{}\n", target.endpoint.ip,
               target.endpoint.port);
    print_help();

    DirectMetaClient client(target);
    std::string line;
    while (true) {
        fmt::print("meta> ");
        std::cout.flush();
        if (!std::getline(std::cin, line)) {
            fmt::print("\n");
            break;
        }

        const std::vector<std::string> tokens = split_tokens(line);
        if (tokens.empty()) continue;

        const std::string& command = tokens[0];
        if (command == "quit" || command == "exit") break;
        if (command == "help") {
            print_help();
            continue;
        }
        if (command == "create_db") {
            if (tokens.size() != 3) {
                fmt::print("usage: create_db <db_name> <zone>\n");
                continue;
            }
            DatabaseID db_id = -1;
            const Status status = client.create_db(tokens[1], tokens[2], &db_id);
            if (status.ok()) {
                fmt::print("OK db_id={}\n", db_id);
            } else {
                print_status(status);
            }
            continue;
        }
        if (command == "create_table") {
            if (tokens.size() != 6) {
                fmt::print("usage: create_table <db_name> <table_name> "
                           "<shard_count> <replica_count> <resource_pool>\n");
                continue;
            }
            int32_t shard_count = 0;
            int32_t replica_count = 0;
            if (!parse_i32(tokens[3], &shard_count) ||
                !parse_i32(tokens[4], &replica_count)) {
                fmt::print("shard_count and replica_count should be integers\n");
                continue;
            }
            TableID table_id = -1;
            const Status status = client.create_table(
                tokens[1], tokens[2], shard_count, replica_count, &table_id, tokens[5]);
            if (status.ok()) {
                fmt::print("OK table_id={}\n", table_id);
            } else {
                print_status(status);
            }
            continue;
        }
        if (command == "get_table") {
            if (tokens.size() != 3) {
                fmt::print("usage: get_table <db_name> <table_name>\n");
                continue;
            }
            TableInfo table_info;
            const Status status =
                client.get_table(tokens[1], tokens[2], &table_info);
            if (status.ok()) {
                fmt::print("OK db_id={} table_id={} shard_count={} "
                           "replica_count={} table_state={} last_error_msg={}\n",
                           table_info.db_id, table_info.table_id,
                           table_info.shard_count, table_info.replica_count,
                           static_cast<int32_t>(table_info.table_state),
                           table_info.last_error_msg);
            } else {
                print_status(status);
            }
            continue;
        }

        fmt::print("unknown command: {}\n", command);
        print_help();
    }
    return 0;
}
