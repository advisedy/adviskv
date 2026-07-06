#pragma once

#include <filesystem>
#include <functional>
#include <memory>

#include "common/status.h"
#include "sdm/persist/sdm_persist_engine.h"
#include "sdm/store/i_sdm_metastore.h"

namespace adviskv::sdm {

class PersistentMetaStore : public ISdmMetaStore {
   public:
    explicit PersistentMetaStore(std::filesystem::path data_dir);
    PersistentMetaStore(std::unique_ptr<ISdmMetaStore> memory_store,
                        std::filesystem::path data_dir);
    PersistentMetaStore(std::unique_ptr<ISdmMetaStore> memory_store,
                        std::unique_ptr<ISdmPersistEngine> persist_engine);

    Status init() override;

#define X(...) __VA_ARGS__ override;
    ISDM_METASTORE_METHODS(X)
#undef X

   private:
    Status load();
    Status build_record_from_store(const ISdmMetaStore& store,
                                   SdmPersistedRecord& record) const;
    Status persist_record(const SdmPersistedRecord& record);
    Status commit_with(const std::function<Status(ISdmMetaStore&)>& mutate);

    std::unique_ptr<ISdmMetaStore> memory_store_;
    std::unique_ptr<ISdmPersistEngine> persist_engine_;
};

}  // namespace adviskv::sdm
