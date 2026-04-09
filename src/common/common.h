#pragma once
#include "common/status.h"
#include "common/log.h"
#include "common/confmgr.h"
#include "common/type.h"
#include <cstdint>
#include <google/protobuf/stubs/port.h>

/*
message PutRequest {
  int32 table_id = 1;
  int32 shard_id = 2;
  bytes key = 3;
  bytes value = 4;
  int64 expire_ts = 5;
}
*/
