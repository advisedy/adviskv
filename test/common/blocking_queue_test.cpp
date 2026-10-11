#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/blocking_queue.h"

namespace adviskv {
namespace {

TEST(BlockingQueueTest, PushAndPopPreserveFifoOrder) {
    BlockingQueue<int> queue;
    ASSERT_TRUE(queue.push(1));
    ASSERT_TRUE(queue.push(2));
    ASSERT_TRUE(queue.push(3));

    int value = 0;
    EXPECT_TRUE(queue.pop(value));
    EXPECT_EQ(value, 1);
    EXPECT_TRUE(queue.pop(value));
    EXPECT_EQ(value, 2);
    EXPECT_TRUE(queue.pop(value));
    EXPECT_EQ(value, 3);
}

TEST(BlockingQueueTest, TryPopOnEmptyQueueDoesNotChangeOutput) {
    BlockingQueue<int> queue;
    int value = 17;

    EXPECT_FALSE(queue.try_pop(value));
    EXPECT_EQ(value, 17);
}

TEST(BlockingQueueTest, PopWaitsUntilAnItemIsPushed) {
    BlockingQueue<int> queue;
    std::promise<void> entered;
    auto entered_future = entered.get_future();
    auto result = std::async(std::launch::async, [&] {
        int value = -1;
        entered.set_value();
        const bool popped = queue.pop(value);
        return std::make_pair(popped, value);
    });
    entered_future.wait();
    ASSERT_TRUE(queue.push(42));

    const auto ready = result.wait_for(std::chrono::seconds(2));
    if (ready != std::future_status::ready) {
        queue.close();
    }
    ASSERT_EQ(ready, std::future_status::ready);
    const auto [popped, value] = result.get();
    EXPECT_TRUE(popped);
    EXPECT_EQ(value, 42);
}

TEST(BlockingQueueTest, SupportsMoveOnlyItems) {
    BlockingQueue<std::unique_ptr<int>> queue;
    ASSERT_TRUE(queue.push(std::make_unique<int>(31)));

    std::unique_ptr<int> value;
    EXPECT_TRUE(queue.pop(value));
    ASSERT_NE(value, nullptr);
    EXPECT_EQ(*value, 31);
}

TEST(BlockingQueueTest, CloseWakesConsumerWaitingOnEmptyQueue) {
    BlockingQueue<int> queue;
    std::promise<void> entered;
    auto entered_future = entered.get_future();
    auto result = std::async(std::launch::async, [&] {
        int value = 23;
        entered.set_value();
        const bool popped = queue.pop(value);
        return std::make_pair(popped, value);
    });
    entered_future.wait();
    queue.close();

    const auto ready = result.wait_for(std::chrono::seconds(2));
    if (ready != std::future_status::ready) {
        queue.close();
    }
    ASSERT_EQ(ready, std::future_status::ready);
    const auto [popped, value] = result.get();
    EXPECT_FALSE(popped);
    EXPECT_EQ(value, 23);
}

TEST(BlockingQueueTest, CloseDiscardsItemsAndPermanentlyRejectsOperations) {
    BlockingQueue<int> queue;
    ASSERT_TRUE(queue.push(1));
    ASSERT_TRUE(queue.push(2));

    queue.close();
    queue.close();

    int value = 19;
    EXPECT_FALSE(queue.push(3));
    EXPECT_FALSE(queue.pop(value));
    EXPECT_EQ(value, 19);
    EXPECT_FALSE(queue.try_pop(value));
    EXPECT_EQ(value, 19);
}

TEST(BlockingQueueTest, MultipleConsumersAndProducersDoNotLoseOrDuplicateItems) {
    struct Item {
        int producer;
        int sequence;
    };

    constexpr int producer_count = 4;
    constexpr int consumer_count = 3;
    constexpr int items_per_producer = 1000;
    BlockingQueue<Item> queue;
    std::mutex results_mutex;
    std::condition_variable results_cv;
    std::unordered_set<uint64_t> received;
    std::atomic<int> received_count{0};
    std::atomic<bool> duplicate{false};
    std::vector<std::thread> consumers;
    consumers.reserve(consumer_count);

    for (int i = 0; i < consumer_count; ++i) {
        consumers.emplace_back([&] {
            Item item{};
            while (queue.pop(item)) {
                const uint64_t key = static_cast<uint64_t>(item.producer) * items_per_producer + item.sequence;
                {
                    std::lock_guard lock(results_mutex);
                    if (!received.insert(key).second) {
                        duplicate.store(true);
                    }
                }
                received_count.fetch_add(1);
                results_cv.notify_all();
            }
        });
    }

    std::vector<std::thread> producers;
    producers.reserve(producer_count);
    for (int producer = 0; producer < producer_count; ++producer) {
        producers.emplace_back([&, producer] {
            for (int sequence = 0; sequence < items_per_producer; ++sequence) {
                if (!queue.push(Item{producer, sequence})) {
                    duplicate.store(true);
                    return;
                }
            }
        });
    }
    for (std::thread& producer : producers) {
        producer.join();
    }
    bool drained = false;
    {
        std::unique_lock lock(results_mutex);
        drained = results_cv.wait_for(lock, std::chrono::seconds(5), [&] {
            return received.size() == static_cast<size_t>(producer_count * items_per_producer);
        });
    }
    queue.close();
    for (std::thread& consumer : consumers) {
        consumer.join();
    }

    EXPECT_TRUE(drained);
    EXPECT_FALSE(duplicate.load());
    EXPECT_EQ(received_count.load(), producer_count * items_per_producer);
    EXPECT_EQ(received.size(), static_cast<size_t>(producer_count * items_per_producer));
}

TEST(BlockingQueueTest, PreservesEachProducersEnqueueOrderWithOneConsumer) {
    struct Item {
        int producer;
        int sequence;
    };

    constexpr int producer_count = 5;
    constexpr int items_per_producer = 500;
    BlockingQueue<Item> queue;
    std::mutex received_mutex;
    std::condition_variable received_cv;
    std::vector<Item> received;
    std::thread consumer([&] {
        Item item{};
        while (queue.pop(item)) {
            {
                std::lock_guard lock(received_mutex);
                received.push_back(item);
            }
            received_cv.notify_all();
        }
    });

    std::vector<std::thread> producers;
    producers.reserve(producer_count);
    for (int producer = 0; producer < producer_count; ++producer) {
        producers.emplace_back([&, producer] {
            for (int sequence = 0; sequence < items_per_producer; ++sequence) {
                queue.push(Item{producer, sequence});
            }
        });
    }
    for (std::thread& producer : producers) {
        producer.join();
    }
    bool drained = false;
    {
        std::unique_lock lock(received_mutex);
        drained = received_cv.wait_for(lock, std::chrono::seconds(5), [&] {
            return received.size() == static_cast<size_t>(producer_count * items_per_producer);
        });
    }
    queue.close();
    consumer.join();

    EXPECT_TRUE(drained);
    ASSERT_EQ(received.size(), static_cast<size_t>(producer_count * items_per_producer));
    std::vector<int> next_sequence(producer_count, 0);
    for (const Item& item : received) {
        ASSERT_GE(item.producer, 0);
        ASSERT_LT(item.producer, producer_count);
        EXPECT_EQ(item.sequence, next_sequence[item.producer]++);
    }
    for (int next : next_sequence) {
        EXPECT_EQ(next, items_per_producer);
    }
}

TEST(BlockingQueueTest, PushRacingWithCloseAlwaysSettles) {
    constexpr int producer_count = 6;
    constexpr int pushes_per_producer = 1000;
    BlockingQueue<int> queue;
    std::mutex start_mutex;
    std::condition_variable start_cv;
    int ready_count = 0;
    bool start = false;
    std::atomic<int> accepted{0};
    std::atomic<int> rejected{0};
    std::vector<std::thread> producers;
    producers.reserve(producer_count);

    for (int producer = 0; producer < producer_count; ++producer) {
        producers.emplace_back([&, producer] {
            {
                std::unique_lock lock(start_mutex);
                ++ready_count;
                start_cv.notify_all();
                start_cv.wait(lock, [&] { return start; });
            }
            for (int i = 0; i < pushes_per_producer; ++i) {
                if (queue.push(producer * pushes_per_producer + i)) {
                    accepted.fetch_add(1);
                } else {
                    rejected.fetch_add(1);
                }
            }
        });
    }

    {
        std::unique_lock lock(start_mutex);
        start_cv.wait(lock, [&] { return ready_count == producer_count; });
        start = true;
    }
    start_cv.notify_all();
    queue.close();
    for (std::thread& producer : producers) {
        producer.join();
    }

    EXPECT_EQ(accepted.load() + rejected.load(), producer_count * pushes_per_producer);
    int value = -1;
    EXPECT_FALSE(queue.pop(value));
    EXPECT_EQ(value, -1);
    EXPECT_FALSE(queue.try_pop(value));
    EXPECT_EQ(value, -1);
}

}
}
