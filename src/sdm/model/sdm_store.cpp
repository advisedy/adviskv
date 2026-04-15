#include "sdm/model/sdm_store.h"
#include "common/status.h"
#include "sdm/model/store.h"

namespace adviskv::sdm{

Status SdmStore::put_table(const Table& table){
    return Status{StatusCode::NOT_SUPPORTED};
}

Status SdmStore::put_shard_route(const ShardRoute& route){
    return Status{StatusCode::NOT_SUPPORTED};
}

}
