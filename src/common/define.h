#pragma once

/*
#define VALIDATE_BEGIN(T) \
    Status validate() const {

#define VALIDATE_END() \
    return Status::OK();}
*/

// 这里cond要填写应该合法的情况，如果cond与预期不符合，就会return，跟assert的语义有点像

#include <chrono>
#include <cstdint>
#include <type_traits>
#include <utility>

#include <fmt/core.h>

namespace adviskv {

#define RETURN_IF_INVALID_CONDITION(cond, msg)            \
    if (!(cond)) {                                        \
        return Status{StatusCode::INVALID_ARGUMENT, msg}; \
    }

#define RETURN_IF_INVALID_STATUS(s1)                     \
    if (auto&& advis_status = s1; advis_status.fail()) { \
        return advis_status;                             \
    }

#define RETURN_IF_INVALID_PARAM(param)                     \
    if (Status status = param.validate(); status.fail()) { \
        return status;                                     \
    }

#define RETURN_IF_INVALID_CONF(conf)                      \
    if (Status status = conf.validate(); status.fail()) { \
        return status;                                    \
    }

#define RETURN_IF_INVALID_READ(buf, name)                                   \
    do {                                                                    \
        if (bool success = (buf).read(name); !success) {                    \
            return Status::ERROR(fmt::format("read {} is invalid", #name)); \
        }                                                                   \
    } while (false);

#define RETURN_IF_NULLPTR(ptr, msg)                       \
    if ((ptr) == nullptr) {                               \
        return Status{StatusCode::INVALID_ARGUMENT, msg}; \
    }

#define DISALLOW_COPY_AND_ASSIGN(type) \
    type(const type&) = delete;        \
    type& operator=(const type&) = delete;

#define ALLOW_MOVE_AND_ASSIGN(type) \
    type(type&&) = default;         \
    type& operator=(type&&) = default;

#define UNUSED(x) ((void)(x));
#define IGNORE_RESULT(expr) ((void)(expr));

#define DEFINE_OPERATOR_NOT_EQUAL(type) \
    bool operator!=(const type& other) const { return !(*this == other); }

template <typename To, typename From>
constexpr To to(From&& x) noexcept {
    return static_cast<To>(std::forward<From>(x));
}

// 规定一下，枚举的统一使用int32，并且都从0开始，最后一个是 MAX_VALUE用于划分界限，StatusCode不算。
template <typename Enum>
constexpr bool is_valid_enum_value(int32_t value) noexcept {
    static_assert(std::is_enum<Enum>::value, "Enum must be an enum type");

    return value >= 0 && value <= static_cast<int32_t>(Enum::MAX_VALUE);
}

}  // namespace adviskv
