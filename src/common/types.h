#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

namespace adviskv {

using int64 = int64_t;
using int32 = int32_t;
using int8 = int8_t;
using uint64 = uint64_t;
using uint32 = uint32_t;
using uint8 = uint8_t;

using Key = std::string;
using Value = std::string;
using KeyView = std::string_view;
using ValueView = std::string_view;

using KV = std::pair<Key, Value>;

using DatabaseID = int32_t;
using TableID = int32_t;
using ShardIndex = int32_t;
using ReplicaIndex = int32_t;
using ReplicaSeq = int32_t;
using NodeID = std::string;

using Term = int64;

using Milliseconds = std::chrono::milliseconds;
using Microseconds = std::chrono::microseconds;
using Seconds = std::chrono::seconds;

}  // namespace adviskv