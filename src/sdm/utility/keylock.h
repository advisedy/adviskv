#pragma once

#include <cassert>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
namespace adviskv::sdm {

template <typename T>
class KeyLock {
   public:
    template <template <typename> typename Lock>
    class KeyLockGuard {
        KeyLockGuard(std::shared_ptr<std::shared_mutex> mutex,
                     Lock<std::shared_mutex>&& locker)
            : mutex_(mutex), locker_(locker) {}

        KeyLockGuard(const KeyLockGuard& one) = delete;
        KeyLockGuard operator=(const KeyLockGuard& one) = delete;

       private:
        std::shared_ptr<std::shared_mutex>
            mutex_;  // 原本是想着，这样子就防止mutex会被析构的情况了，所以把mutex的shared_ptr带了出来，但是貌似好像不会有影响是吗，毕竟在map_里面貌似不会删除掉它。
        Lock<std::shared_mutex> locker_;
    };

    template <template <typename> typename Lock>
    KeyLockGuard<Lock> lock(const std::string& name) {
        {
            std::shared_lock<std::shared_mutex> lock{mutex_};
            auto it = mutex_map_.find(name);
            if (it != mutex_map_.end()) {
                return KeyLockGuard<Lock>{it->second,
                                          Lock<std::shared_mutex>{it->second}};
            }
        }
        {
            std::unique_lock<std::shared_mutex> lock{mutex_};
            auto it = mutex_map_.find(name);
            if (it != mutex_map_.end()) {
                return KeyLockGuard<Lock>{it->second,
                                          Lock<std::shared_mutex>{it->second}};
            } else {
                auto [it2, insert_flag] = mutex_map_.insert(
                    {name, std::make_shared<std::shared_mutex>()});
                assert(insert_flag == true);
                return KeyLockGuard<Lock>{it2->second,
                                          Lock<std::shared_mutex>{it2->second}};
            }
        }
    }

    static KeyLock<T>& get_instance() {
        static KeyLock<T> instance;
        return instance;
    }

    KeyLock<T>(const KeyLock<T>& one) = delete;
    KeyLock<T> operator=(const KeyLock<T>& one) = delete;

   private:
    KeyLock<T>() = default;

    std::shared_mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<std::shared_mutex>>
        mutex_map_;
};

template <typename T>
using WriteKeyLockGuard =
    typename KeyLock<T>::template KeyLockGuard<std::unique_lock>;

template <typename T>
using ReadKeyLockGuard =
    typename KeyLock<T>::template KeyLockGuard<std::shared_lock>;

#define KEY_CONCACT_INNER(x, y) x##y
#define KEY_CONCACT(x, y) KEY_CONCACT_INNER(x, y)



// 提供给外部的，这边直接设置好要锁的对象类型和这个名字就可以了。
#define WRITE_KEY_LOCK_GUARD(type, key)                     \
    WriteKeyLockGuard<type> KEY_CONCACT(locker, __LINE__) = \
        KeyLock<type>::get_instance().lock<std::unique_lock>(key);

#define READ_KEY_LOCK_GUARD(type, key)                     \
    ReadKeyLockGuard<type> KEY_CONCACT(locker, __LINE__) = \
        KeyLock<type>::get_instance().lock<std::shared_lock>(key);

#undef KEY_CONCACT
#undef KEY_CONCACT_INNER
}  // namespace adviskv::sdm