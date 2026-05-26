#include "e2e_assert.h"

#include <fmt/base.h>
#include <fmt/core.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <thread>

namespace adviskv::e2e {
namespace {

using Clock = std::chrono::steady_clock;

constexpr const char* kGreen = "\033[32m";
constexpr const char* kRed = "\033[31m";
constexpr const char* kBlue = "\033[34m";
constexpr const char* kBold = "\033[1m";
constexpr const char* kReset = "\033[0m";

const char* color_code(COLOR color) {
    switch (color) {
        case COLOR::RED:
            return kRed;
        case COLOR::BLUE:
            return kBlue;
        case COLOR::GREEN:
            return kGreen;
        case COLOR::BOLD:
            return kBold;
        case COLOR::RESET:
            return kReset;
    }
    return kReset;
}

}  // namespace

std::string colorize(COLOR color, const std::string& text) {
    if (std::getenv("NO_COLOR") != nullptr) {
        return text;
    }
    if (!isatty(STDOUT_FILENO) && std::getenv("FORCE_COLOR") == nullptr &&
        std::getenv("GITHUB_ACTIONS") == nullptr) {
        return text;
    }
    return fmt::format("{}{}{}", color_code(color), text, kReset);
}

void print_step(const std::string& message) {
    fmt::print("{} {}\n", colorize(COLOR::BLUE, "[ e2e ]"), message);
}

void print_pass(const std::string& name, const std::string& message) {
    fmt::print("{} {}: {}\n", colorize(COLOR::GREEN, "[ PASS ]"), name,
               message);
}

void print_pass_bold(const std::string& message) {
    fmt::print("{} {}\n", colorize(COLOR::GREEN, "[ PASS ]"),
               colorize(COLOR::BOLD, message));
}

void print_fail(const std::string& name, const std::string& message) {
    fmt::print(stderr, "{} {}: {}\n", colorize(COLOR::RED, "[ FAIL ]"), name,
               message);
}

void print_case_start(const std::string& name) {
    fmt::print("{}: start", name);
}

void print_case_pass(const std::string& name) {
    fmt::print("{} {}: {}\n", colorize(COLOR::GREEN, "[ PASS ]"), name, "pass");
}

void print_case_fail(const std::string& name) {
    fmt::print(stderr, "{} {} {}\n", colorize(COLOR::RED, "[ FAIL ]"), name,
               "fail");
}

bool eventually(const std::string& name, const Options& options,
                const std::function<CheckResult()>& check,
                std::string* last_error) {
    const auto deadline =
        Clock::now() + std::chrono::milliseconds(options.timeout_ms);
    while (Clock::now() < deadline) {
        CheckResult result = check();
        if (result.ok) {
            print_pass(name, result.message);
            return true;
        }
        *last_error = result.message;
        std::this_thread::sleep_for(
            std::chrono::milliseconds(options.poll_interval_ms));
    }
    print_fail(name, fmt::format("timed out: {}", *last_error));
    return false;
}

bool wait_status(const std::string& name, const Options& options,
                 const std::function<Status()>& operation) {
    std::string last_error;
    return eventually(
        name, options,
        [&]() {
            const Status status = operation();
            if (status.ok()) {
                return CheckResult::pass(status.to_string());
            }
            return CheckResult::fail(status.to_string());
        },
        &last_error);
}

bool grpc_ok(const grpc::Status& status, const std::string& rpc_name,
             std::string* error) {
    if (status.ok()) {
        return true;
    }
    *error = fmt::format("{} grpc failed, code={}, msg={}", rpc_name,
                         static_cast<int>(status.error_code()),
                         status.error_message());
    return false;
}

bool base_rsp_ok(int32_t code, const std::string& msg,
                 const std::string& rpc_name, std::string* error) {
    if (code == to_rpc_code(StatusCode::OK) ||
        code == to_rpc_code(StatusCode::ALREADY_EXIST)) {
        return true;
    }
    *error = fmt::format("{} failed, code={}, msg={}", rpc_name, code, msg);
    return false;
}

}  // namespace adviskv::e2e