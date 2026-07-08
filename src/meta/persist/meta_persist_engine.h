#pragma once

#include <string>

#include "meta/persist/i_meta_persist_engine.h"

namespace adviskv::meta {

class MetaPersistEngine : public IMetaPersistEngine {
public:
    explicit MetaPersistEngine(const std::string& data_dir);
    ~MetaPersistEngine() override;

    Status init() override;
    Status close() override;

    Status save_meta(const PersistedMetaRecord& record) override;
    Status load_meta(PersistedMetaRecord& record) override;

private:
    std::string data_dir_;
    std::string meta_data_path_;
    std::string meta_data_tmp_path_;
};

}  // namespace adviskv::meta
