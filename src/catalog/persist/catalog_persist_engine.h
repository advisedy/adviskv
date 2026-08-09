#pragma once

#include <string>

#include "catalog/persist/i_catalog_persist_engine.h"

namespace adviskv::catalog {

class CatalogPersistEngine : public ICatalogPersistEngine {
public:
    explicit CatalogPersistEngine(const std::string& data_dir);
    ~CatalogPersistEngine() override;

    Status init() override;
    Status close() override;

    Status save_catalog(const CatalogPersistedRecord& record) override;
    Status load_catalog(CatalogPersistedRecord& record) override;

private:
    std::string data_dir_;
    /*
        catalog_data_path_ = data_dir_ + "/catalog_data";
        catalog_data_tmp_path_ = data_dir_ + "/catalog_data.tmp";
    */
    const std::string catalog_data_path_;
    const std::string catalog_data_tmp_path_;
};

}  // namespace adviskv::catalog
