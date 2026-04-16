// #pragma once

// #include "common/type.h"
// #include <cstdint>
// #include <string>

// namespace adviskv {

// // struct TableMetaCacheKey {
// //   std::string db_name;
// //   std::string table_name;

// //   bool operator==(const TableMetaCacheKey &other) const {
// //     return db_name == other.db_name && table_name == other.table_name;
// //   }
// // };

// // struct TableMetaCacheKeyHash {
// //   uint64_t operator()(const TableMetaCacheKey &key) const {
// //     std::string res = key.db_name + "***&&**" + key.table_name;
// //     return std::hash<std::string>{}(res);
// //   }
// // };

// // struct TableMetaCache {
// //   std::string db_name;
// //   std::string table_name;
// //   TableID table_id;
// //   DatabaseID db_id;
// //   int32_t shard_count;
// //   int32_t replica_count;
// // };

// // struct DBMetaCache {
// //   std::string db_name;
// //   DatabaseID db_id;
// //   std::string zone;
// // };

// } // namespace adviskv