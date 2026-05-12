#pragma once

// #define VALIDATE_BEGIN(T) \
//     Status validate() const {

// #define VALIDATE_END() \
//     return Status::OK();}

// 这里cond要填写应该合法的情况，如果cond与预期不符合，就会return，跟assert的语义有点像

#include <chrono>
#include <fmt/core.h>
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

#define RETURN_IF_INVALID_READ(buf, name)                                    \
    do {                                                                     \
        if (bool success = (buf).read(name); !success) {                     \
            return Status::ERROR(fmt::format("read {} is invalid", #name));  \
        }                                                                    \
    } while (false);

#define RETURN_IF_INVALID_READ_TYPE(buf, type, name)                           \
    do {                                                                       \
        if (bool success = (buf).read<type>(name); !success) {                 \
            return Status::ERROR(                                              \
                fmt::format("read {} is invalid, type: {}", #name, #type));    \
        }                                                                      \
    } while (false);

#define DEFINE_VECTOR_AND_RESERVE(type, name, size) \
    std::vector<type> name;                         \
    name.reserve(size);

#define UNUSED(x) ((void)(x));
#define IGNORE_RESULT(expr) ((void)(expr));

using Milliseconds = std::chrono::milliseconds;
using Seconds = std::chrono::seconds;

#include <vector>
///////////// function
namespace adviskv {

// template <typename T, typename U>
// inline void ad_erase_if(std::vector<T>& a, U f) {
//     for (auto it = a.begin(); it != a.end();) {
//         if (f(*it)) {
//             it = a.erase(it);
//         } else {
//             it++;
//         }
//     }
// }

}  // namespace adviskv