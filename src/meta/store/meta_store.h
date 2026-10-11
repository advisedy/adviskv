#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <variant>

#include "common/model/model.h"
#include "common/published_view.h"
#include "common/status.h"
#include "meta/model/model.h"
#include "meta/persist/meta_persist_engine.h"
#include "meta/store/meta_snapshot.h"
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

class MetaStore {
public:
    explicit MetaStore(const MetaStoreParam& param);

    Status init();

    Status read_with(const std::function<Status(const MetaStoreTxn&)>& fn) const;
    Status write_with(const std::function<Status(MetaStoreTxn&)>& fn);

private:
    static void rebuild_index(const DurableData& durable, const RuntimeData& runtime, MetaIndex& index);
    Status persist_if_needed(const DurableData& durable);
    Status load_from_persist(DurableData& durable);

    std::mutex writer_mutex_;
    PublishedView<MetaSnapshot> published_;
    std::unique_ptr<IMetaPersistEngine> persist_engine_;
};

}  // namespace adviskv::meta
