#include <fmt/format.h>
#include <yaml-cpp/yaml.h>

#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "direct_storage_client.h"

namespace {

using adviskv::Status;
using adviskv::Value;
using adviskv::cli::DirectStorageClient;
using adviskv::cli::StorageCliTarget;

void print_usage() {
    fmt::print(
        "usage: storage_cli --conf <conf.yaml>\n"
        "   or: storage_cli --host <host> --port <port> --table_id <id> "
        "--shard_id <id> [--timeout_ms <ms>]\n");
}

void print_help() {
    fmt::print("commands:\n");
    fmt::print("  put <key> <value>\n");
    fmt::print("  get <key>\n");
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

bool load_conf(const std::string& path, StorageCliTarget* target) {
    if (target == nullptr) return false;
    try {
        YAML::Node conf = YAML::LoadFile(path);
        target->endpoint.ip = conf["host"].as<std::string>();
        target->endpoint.port = conf["port"].as<int32_t>();
        target->table_id = conf["table_id"].as<int32_t>();
        target->shard_id = conf["shard_id"].as<int32_t>();
        if (conf["timeout_ms"]) {
            target->timeout_ms = conf["timeout_ms"].as<int32_t>();
        }
        return true;
    } catch (const std::exception& e) {
        fmt::print(stderr, "load conf failed: {}\n", e.what());
        return false;
    }
}

bool parse_args(int argc, char* argv[], StorageCliTarget* target) {
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
        } else if (flag == "--table_id") {
            if (!parse_i32(value, &target->table_id)) return false;
        } else if (flag == "--shard_id") {
            if (!parse_i32(value, &target->shard_id)) return false;
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
    StorageCliTarget target;
    if (!parse_args(argc, argv, &target)) {
        print_usage();
        return 1;
    }

    fmt::print("connected target {}:{}, table_id={}, shard_id={}\n",
               target.endpoint.ip, target.endpoint.port, target.table_id,
               target.shard_id);
    print_help();

    DirectStorageClient client;
    std::string line;
    while (true) {
        fmt::print("storage> ");
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
        if (command == "put") {
            if (tokens.size() != 3) {
                fmt::print("usage: put <key> <value>\n");
                continue;
            }
            print_status(client.put(target, tokens[1], tokens[2]));
            continue;
        }
        if (command == "get") {
            if (tokens.size() != 2) {
                fmt::print("usage: get <key>\n");
                continue;
            }
            Value value;
            const Status status = client.get(target, tokens[1], &value);
            if (status.ok()) {
                fmt::print("OK value={}\n", value);
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
