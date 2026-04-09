#pragma once


#include <cstdint>
#include <string>
#include <string_view>

namespace adviskv{

using Key = std::string;
using Value = std::string;
using KeyView = std::string_view;
using ValueView = std::string_view;

using DatabaseID = int32_t;
using TableID = int32_t;
using ShardID = int32_t;

using NodeID = std::string;

}