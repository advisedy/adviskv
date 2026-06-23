#include "storage/raft/core/raft_core.h"

namespace adviskv::storage {

// 判判断当前这个term，这个leader是否已经提交过entry了
// 原因的话是因为如果他没有提交过的话，那这个commit
// index就还没有这个及时的更新到，那么就有可能会导致客户端最终那边会读到旧的数据。
bool RaftCore::has_committed_current_term_entry() const {
    return raft_apply_.has_committed_current_term_entry(
        election_.current_term());
}

// 其实由于我们get操作会专门发送一次，所以导致同样的append_entires
// 可能会多次发送
// ，但是无伤大雅，handle_append_entries里面会判断出来new_entries是空的
Status RaftCore::build_append_entries_for_read(RaftEffects& effects,
                                               LogIndex& read_index,
                                               Term& read_term) {
    RETURN_IF_INVALID_STATUS(ensure_ready())

    if (!election_.is_leader()) return Status::NOT_LEADER("not leader");
    if (!has_committed_current_term_entry()) {
        return Status::NOT_YET_COMMIT("current term entry is not committed");
    }

    read_term = election_.current_term();
    read_index = raft_apply_.commit_index();

    replication_.broadcast_append_entries(election_.current_term(), effects);
    return Status::OK();
}

}  // namespace adviskv::storage
