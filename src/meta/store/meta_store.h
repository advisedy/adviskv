#pragma once

#include <functional>
#include <memory>
#include <shared_mutex>
#include <string>
#include <variant>

#include "common/model/type.h"
#include "common/status.h"
#include "meta/model/model.h"
#include "meta/persist/meta_persist_engine.h"
#include "meta/store/meta_data.h"
#include "meta/store/meta_index.h"
#include "meta/store/meta_store_txn.h"

namespace adviskv::meta {

/*
get_* 未找到时返回 Status::OK，输出 Optional 为空。
*/
struct MemoryMetaStoreParam {};

struct PersistentMetaStoreParam {
    std::string data_dir;
};

using MetaStoreParam = std::variant<MemoryMetaStoreParam, PersistentMetaStoreParam>;

// 只负责锁、加载、事务提交、持久化。数据操作全部在 MetaStoreTxn 里。
class MetaStore {
public:
    explicit MetaStore(const MetaStoreParam& param);

    Status init();

    Status read_with(const std::function<Status(const MetaStoreTxn&)>& fn) const;
    Status write_with(const std::function<Status(MetaStoreTxn&)>& fn);

private:
    void rebuild_index(MetaWorkingSet& ws) const;
    Status persist_if_needed(const DurableData& durable);
    Status load_from_persist(MetaWorkingSet& ws);

    mutable std::shared_mutex mutex_;
    MetaWorkingSet committed_;
    std::unique_ptr<IMetaPersistEngine> persist_engine_;
};

}  // namespace adviskv::meta
