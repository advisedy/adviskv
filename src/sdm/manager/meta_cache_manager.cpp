#include "sdm/manager/meta_cache_manager.h"
#include "common/status.h"
#include <fmt/format.h>
#include <mutex>

namespace adviskv {

Status MetaCacheManager::get_table_meta(const std::string &db_name,
                                        const std::string &table_name,
                                        TableMetaCache *out) const {
  std::shared_lock lock{table_caches_mutex_};
  TableMetaCacheKey key{db_name, table_name};
  auto it = table_meta_caches_.find(key);
  if (it == table_meta_caches_.end()) {
    return Status{StatusCode::TABLE_META_CACHE_NOT_FOUND,
                  fmt::format("db_name: {}, table_name: {} not found in cache",
                              db_name, table_name)};
  } else {
    *out = it->second;
  }
  return Status::OK();
}

Status MetaCacheManager::update_table_meta(const TableMetaCache &meta) {

  std::unique_lock lock{table_caches_mutex_};
  TableMetaCacheKey key{meta.db_name, meta.table_name};
  auto it = table_meta_caches_.find(key);
  if (it == table_meta_caches_.end()) {
    table_meta_caches_.insert({key, meta});
  } else {
    it->second = meta;
  }
  return Status::OK();
}

Status MetaCacheManager::update_db_meta(const DBMetaCache &meta) {
    //TODO
//   return Status{StatusCode::NOT_SUPPORTED,
//                 "update_db_meta is not supported now"};
    std::unique_lock lock{db_caches_mutex_};
    auto it = db_meta_caches_.find(meta.db_name);
    if (it == db_meta_caches_.end()) {
        db_meta_caches_.insert({meta.db_name, meta});
    } else {
        it->second = meta;
    }
    return Status::OK();
}
Status MetaCacheManager::get_db_meta(const std::string &db_name,
                                     DBMetaCache *out) const {
                                        //TODO
//   return Status{StatusCode::NOT_SUPPORTED, "get_db_meta is not supported now"};
    std::shared_lock lock{db_caches_mutex_};
    auto it = db_meta_caches_.find(db_name);
    if (it == db_meta_caches_.end()) {
        return Status{StatusCode::DB_META_CACHE_NOT_FOUND,
                      fmt::format("db_name: {} not found in cache", db_name)};
    } else {
        *out = it->second;
    }
    return Status::OK();
}

} // namespace adviskv
