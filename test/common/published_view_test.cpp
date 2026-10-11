#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <thread>
#include <type_traits>
#include <vector>

#include "common/published_view.h"

namespace adviskv {
namespace {

struct ViewData {
    int generation{0};
    int value{0};
};

using TestView = PublishedView<ViewData>;

static_assert(!std::is_copy_constructible_v<TestView>);
static_assert(!std::is_copy_assignable_v<TestView>);
static_assert(!std::is_move_constructible_v<TestView>);
static_assert(!std::is_move_assignable_v<TestView>);
static_assert(std::is_same_v<decltype(std::declval<const TestView&>().load()),
                             std::shared_ptr<const ViewData>>);

TEST(PublishedViewTest, StartsWithANonNullDefaultView) {
    TestView published;
    const auto view = published.load();
    ASSERT_NE(view, nullptr);
    EXPECT_EQ(view->generation, 0);
    EXPECT_EQ(view->value, 0);
}

TEST(PublishedViewTest, RefreshBuildsAndPublishesTheLatestView) {
    TestView published;
    int generation = 1;
    published.set_builder([&generation]() {
        return std::make_shared<const ViewData>(ViewData{generation, generation * 2});
    });
    published.refresh();
    const auto first = published.load();
    EXPECT_EQ(first->generation, 1);
    EXPECT_EQ(first->value, 2);

    generation = 2;
    published.refresh();
    const auto second = published.load();
    EXPECT_NE(first, second);
    EXPECT_EQ(second->generation, 2);
    EXPECT_EQ(second->value, 4);
    EXPECT_EQ(first->generation, 1);
    EXPECT_EQ(first->value, 2);
}

TEST(PublishedViewTest, PublishingKeepsPreviouslyLoadedViewsAlive) {
    TestView published;
    auto first = std::make_shared<const ViewData>(ViewData{1, 2});
    std::weak_ptr<const ViewData> weak = first;
    published.publish(first);
    auto held = published.load();
    first.reset();
    auto second = std::make_shared<const ViewData>(ViewData{2, 4});
    published.publish(second);
    EXPECT_EQ(published.load(), second);
    EXPECT_FALSE(weak.expired());
    EXPECT_EQ(held->generation, 1);
    held.reset();
    EXPECT_TRUE(weak.expired());
}

TEST(PublishedViewTest, ConcurrentReadersOnlySeeCompleteViews) {
    TestView published;
    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    std::atomic<bool> done{false};
    std::atomic<bool> complete{true};
    std::vector<std::thread> readers;
    for (int i = 0; i < 4; ++i) {
        readers.emplace_back([&]() {
            ready.fetch_add(1);
            while (!start.load()) {
                std::this_thread::yield();
            }
            do {
                const auto view = published.load();
                if (!view || view->value != view->generation * 2) {
                    complete.store(false);
                }
            } while (!done.load());
        });
    }
    while (ready.load() != 4) {
        std::this_thread::yield();
    }
    start.store(true);
    for (int generation = 1; generation <= 10000; ++generation) {
        published.publish(std::make_shared<const ViewData>(ViewData{generation, generation * 2}));
    }
    done.store(true);
    for (auto& reader : readers) {
        reader.join();
    }
    EXPECT_TRUE(complete.load());
    EXPECT_EQ(published.load()->generation, 10000);
}

}
}
