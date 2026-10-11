#pragma once

#include <memory>
#include <utility>

#include "meta/store/meta_data.h"
#include "meta/store/meta_index.h"

namespace adviskv::meta {

struct MetaSnapshot {
    std::shared_ptr<const DurableData> durable{std::make_shared<const DurableData>()};
    std::shared_ptr<const RuntimeData> runtime{std::make_shared<const RuntimeData>()};
    std::shared_ptr<const MetaIndex> index{std::make_shared<const MetaIndex>()};
};

template <typename T>
class CowValue {
public:
    CowValue() = default;
    explicit CowValue(std::shared_ptr<const T> shared) : shared_(std::move(shared)) {}

    const T& get() const { return cloned_ != nullptr ? *cloned_ : *shared_; }

    T& mutable_of() {
        if (cloned_ == nullptr) {
            cloned_ = shared_ == nullptr ? std::make_shared<T>() : std::make_shared<T>(*shared_);
        }
        return *cloned_;
    }

    bool cloned() const { return cloned_ != nullptr; }

    std::shared_ptr<const T> share() const { return cloned_ != nullptr ? cloned_ : shared_; }

private:
    std::shared_ptr<const T> shared_;
    std::shared_ptr<T> cloned_;
};

}  // namespace adviskv::meta
