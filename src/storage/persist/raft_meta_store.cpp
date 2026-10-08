#include "storage/persist/raft_meta_store.h"

#include <utility>

#include "common/define.h"
#include "common/framed_record_codec.h"
#include "storage/persist/persist_codecs.h"

namespace adviskv::storage {

RaftMetaStore::RaftMetaStore(std::string raft_meta_path, std::string dir_path)
        : raft_meta_path_(std::move(raft_meta_path)), dir_path_(std::move(dir_path)) {}

Status RaftMetaStore::save(const RaftMeta& meta) {
    return File::atomic_replace(raft_meta_path_,
                                [&meta](const File& file) { return FramedRecord<RaftMeta>::encode_to(file, meta); });
}

Status RaftMetaStore::load(RaftMeta& meta) const {
    File file;
    bool exists = false;
    RETURN_IF_INVALID_STATUS(File::open_for_read(raft_meta_path_, file, exists))
    if (!exists) {
        meta = {};
        return Status::OK();
    }
    return FramedRecord<RaftMeta>::decode_from(file, meta);
}

}  // namespace adviskv::storage
