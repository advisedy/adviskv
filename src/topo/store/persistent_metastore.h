#pragma once

#include <filesystem>
#include <functional>
#include <memory>

#include "common/status.h"
#include "topo/persist/topo_persist_engine.h"
#include "topo/store/i_topo_metastore.h"

namespace adviskv::topo {

class PersistentMetaStore : public ITopoMetaStore {
public:
    explicit PersistentMetaStore(std::filesystem::path data_dir);
    PersistentMetaStore(std::unique_ptr<ITopoMetaStore> memory_store, std::filesystem::path data_dir);
    PersistentMetaStore(std::unique_ptr<ITopoMetaStore> memory_store, std::unique_ptr<ITopoPersistEngine> persist_engine);

    Status init() override;

#define X(...) __VA_ARGS__ override;
    ITOPO_METASTORE_METHODS(X)
#undef X

private:
    Status load();
    Status build_record_from_store(const ITopoMetaStore& store, TopoPersistedRecord& record) const;
    Status persist_record(const TopoPersistedRecord& record);
    Status commit_with(const std::function<Status(ITopoMetaStore&)>& mutate);

    std::unique_ptr<ITopoMetaStore> memory_store_;
    std::unique_ptr<ITopoPersistEngine> persist_engine_;
};

}  // namespace adviskv::topo
