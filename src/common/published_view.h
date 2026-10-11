#pragma once

#include <atomic>
#include <functional>
#include <memory>

#include "common/define.h"

namespace adviskv {

template <typename T>
class PublishedView {
public:
    using View = std::shared_ptr<const T>;
    using Builder = std::function<View()>;

    PublishedView() = default;

    DISALLOW_COPY_AND_ASSIGN(PublishedView)
    DISALLOW_MOVE_AND_ASSIGN(PublishedView)

    void set_builder(const Builder& builder) { builder_ = builder; }

    void refresh() { publish(builder_()); }

    View load() const { return std::atomic_load(&view_); }

    void publish(const View& view) { std::atomic_store(&view_, view); }

private:
    View view_{std::make_shared<const T>()};
    Builder builder_;
};

}
