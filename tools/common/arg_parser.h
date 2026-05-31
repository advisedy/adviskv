#pragma once

#include <fmt/core.h>

#include <cstdint>
#include <cstdio>
#include <exception>
#include <functional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace adviskv::tools {

class ArgParser {
   public:
    void add_string(std::string name, std::string& output) {
        add(std::move(name),
            [&output](const std::string& value) { output = value; });
    }

    void add_int32(std::string name, int32_t& output) {
        add(std::move(name),
            [&output](const std::string& value) { output = std::stoi(value); });
    }

    void add_int64(std::string name, int64_t& output) {
        add(std::move(name), [&output](const std::string& value) {
            output = std::stoll(value);
        });
    }

    void add_double(std::string name, double& output) {
        add(std::move(name),
            [&output](const std::string& value) { output = std::stod(value); });
    }

    bool parse(int argc, char** argv) const {
        for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];
            if (arg.rfind("--", 0) != 0) {
                fmt::print(stderr, "invalid argument '{}'\n", arg);
                return false;
            }
            size_t eq = arg.find('=');
            if (eq == std::string::npos) {
                fmt::print(stderr, "argument '{}' must use --name=value\n",
                           arg);
                return false;
            }

            std::string name = arg.substr(2, eq - 2);
            std::string value = arg.substr(eq + 1);
            const Option* option = find(name);
            if (option == nullptr) {
                fmt::print(stderr, "unknown option '--{}'\n", name);
                return false;
            }
            try {
                option->set(value);
            } catch (const std::exception& e) {
                fmt::print(stderr, "failed to parse --{}={}: {}\n", name, value,
                           e.what());
                return false;
            }
        }
        return true;
    }

   private:
    using Setter = std::function<void(const std::string&)>;

    struct Option {
        std::string name;
        Setter set;
    };

    void add(std::string name, Setter setter) {
        options_.push_back(Option{std::move(name), std::move(setter)});
    }

    const Option* find(std::string_view name) const {
        for (const Option& option : options_) {
            if (option.name == name) {
                return &option;
            }
        }
        return nullptr;
    }

    std::vector<Option> options_;
};

}  // namespace adviskv::tools
