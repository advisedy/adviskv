#pragma once

#include <grpcpp/support/status.h>

#include <functional>
#include <string>
#include <utility>

#include "common/status.h"
#include "e2e_options.h"

namespace adviskv::e2e {

struct CheckResult {
    bool ok{false};
    std::string message;
    static CheckResult pass(std::string message = "") {
        return CheckResult{true, std::move(message)};
    }

    static CheckResult fail(std::string message = "") {
        return CheckResult{false, std::move(message)};
    }
};

enum class COLOR {
    RED,
    BLUE,
    GREEN,
    BOLD,
    RESET,
};

std::string colorize(COLOR color, const std::string& text);

void print_step(const std::string& message);
void print_pass(const std::string& name, const std::string& message);
void print_pass_bold(const std::string& message);
void print_fail(const std::string& name, const std::string& message);

void print_case_start(const std::string& name);
void print_case_pass(const std::string& name);

void print_case_fail(const std::string& name);

bool eventually(const std::string& name, const Options& options,
                const std::function<CheckResult()>& check,
                std::string* last_error);

bool wait_status(const std::string& name, const Options& options,
                 const std::function<Status()>& operation);

bool grpc_ok(const grpc::Status& status, const std::string& rpc_name,
             std::string* error);

bool base_rsp_ok(int32_t code, const std::string& msg,
                 const std::string& rpc_name, std::string* error);

}  // namespace adviskv::e2e