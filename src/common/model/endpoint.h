#pragma once

#include <string>
#include <utility>

#include <fmt/format.h>

#include "common/define.h"
#include "common/types.h"

namespace adviskv {

struct Endpoint {
    std::string ip;
    int32_t port{0};

    Endpoint() = default;
    Endpoint(std::string ip, int32_t port) : ip(std::move(ip)), port(port) {}

    std::string to_string() const { return fmt::format("{}:{}", ip, port); }

    bool operator==(const Endpoint& other) const { return ip == other.ip and port == other.port; }

    DEFINE_OPERATOR_NOT_EQUAL(Endpoint)
};

}  // namespace adviskv