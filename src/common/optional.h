#pragma once

#include <optional>

#include "common/define.h"

namespace adviskv {

template <typename T>
class Optional : public std::optional<T> {
public:
    using std::optional<T>::optional;

    bool is_empty() const { return !std::optional<T>::has_value(); }

    bool has_value() const { return std::optional<T>::has_value(); }

    T* ptr() { return has_value() ? &this->value() : nullptr; }

    const T* ptr() const { return has_value() ? &this->value() : nullptr; }

    T& value() { return std::optional<T>::value(); }
    const T& value() const { return std::optional<T>::value(); }

    bool operator==(const Optional<T>& other) const {
        return static_cast<const std::optional<T>&>(*this) == static_cast<const std::optional<T>&>(other);
    }

    DEFINE_OPERATOR_NOT_EQUAL(Optional<T>)
};

}  // namespace adviskv