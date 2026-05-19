#pragma once

// #define VALIDATE_BEGIN(T) \
//     Status validate() const {

// #define VALIDATE_END() \
//     return Status::OK();}

// 这里cond要填写应该合法的情况，如果cond与预期不符合，就会return，跟assert的语义有点像

#include <fmt/core.h>

#include <chrono>
#define RETURN_IF_INVALID_CONDITION(cond, msg)            \
    if (!(cond)) {                                        \
        return Status{StatusCode::INVALID_ARGUMENT, msg}; \
    }

#define RETURN_IF_INVALID_STATUS(status) \
    if (status.fail()) {                 \
        return status;                   \
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

#define RETURN_IF_INVALID_READ_TYPE(buf, type, name)                        \
    do {                                                                    \
        if (bool success = (buf).read<type>(name); !success) {              \
            return Status::ERROR(                                           \
                fmt::format("read {} is invalid, type: {}", #name, #type)); \
        }                                                                   \
    } while (false);

#define RETURN_IF_NULLPTR(ptr, msg)                          \
    if ((ptr) == nullptr) {                                  \
        return Status{StatusCode::INVALID_ARGUMENT, msg};    \
    }

#define DISALLOW_COPY_AND_ASSIGN(type)   \
    type(const type&) = delete;          \
    type& operator=(const type&) = delete;

#define UNUSED(x) ((void)(x));
#define IGNORE_RESULT(expr) ((void)(expr));

#define SWITCH_TYPE_EQUAL(obj, type_a, type_b, name) \
    case type_a::name: {                             \
        obj = type_b::name;                          \
        break;                                       \
    }

#define SWITCH_TYPE_EQUAL2(obj, type_a, name_a, type_b, name_b) \
    case type_a::name_a: {                                      \
        obj = type_b::name_b;                                   \
        break;                                                  \
    }

#define DEFINE_OPERATOR_NOT_EQUAL(type) \
    bool operator!=(const type& other) const { return !(*this == other); }

using Milliseconds = std::chrono::milliseconds;
using Seconds = std::chrono::seconds;