#include "storage/persist/raft_meta_store.h"

#include <utility>

#include "common/binary/codec.h"
#include "common/define.h"
#include "storage/persist/persist_codecs.h"

namespace adviskv::storage {

RaftMetaStore::RaftMetaStore(std::string raft_meta_path, std::string dir_path)
        : raft_meta_path_(std::move(raft_meta_path)), dir_path_(std::move(dir_path)) {}

Status RaftMetaStore::save(const RaftMeta& meta) {
    return File::atomic_replace(raft_meta_path_, [&meta](const File& file) {
        FileWriter writer(file);
        RETURN_IF_INVALID_STATUS(binary::encode_crc(meta, writer))
        // 原子替换统一负责 fsync 和发布，这里先把 Writer 的缓冲交给它。
        return writer.flush();
    });
}

Status RaftMetaStore::load(RaftMeta& meta) const {
    File file;
    bool exists = false;
    RETURN_IF_INVALID_STATUS(File::open_for_read(raft_meta_path_, file, exists))
    if (!exists) {
        meta = {};
        return Status::OK();
    }
    FileReader reader(file);
    return binary::decode_crc(reader, meta);
}

}  // namespace adviskv::storage
