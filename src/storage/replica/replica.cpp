#include "storage/replica/replica.h"

#include <cstdint>
#include <memory>

#include "common.pb.h"
#include "common/common.h"
#include "common/define.h"
#include "common/log.h"
#include "common/status.h"
#include "common/type.h"
#include "storage.pb.h"
#include "storage/engine/map_engine.h"
#include "storage/model/param.h"

namespace adviskv::storage {

Status Replica::init(const ReplicaInitParam& param) {
    shard_id_ = {.table_id = param.replica_id.table_id,
                 .shard_index = param.replica_id.shard_index};

    replica_id_ = param.replica_id;

    members_ = param.members;

    switch (param.engine_type) {
        case EngineType::MAP: {
            engine_ = std::make_unique<MapEngine>();
            break;
        }
        case EngineType::ROCKSDB: {
            // engine_ = std::make_unique<>()
            break;
        }
        default: {
            return {StatusCode::INVALID_ARGUMENT, "engine type is invaiid"};
            break;
        }
    }

    return Status::OK();
}

Status Replica::put(const PutParam& param){

    // if(!engine_){
    //     WARN("engine is nullptr, replica: table_id = {}, shard_index = {}",
    //     shard_id_.table_id, shard_id_.shard_index);
    //     return Status{StatusCode::ERROR,"engine is nullptr"};
    // }
    // RETURN_IF_INVALID_PARAM(param)
    // // TODO
    // // 判断一下是否还可以写入（空间是否够）
    // // 第一版就先不添加expire_ts了，后续考虑
    // Status status = engine_->put(param.key, param.value);
    // if(status.fail()){
    //     WARN("engine put is not ok, key = {}, value = {}, msg = {}",
    //     param.key, param.value, status.msg());
    // }
    // return status;
    RETURN_IF_INVALID_PARAM(param)
    // 之前的逻辑应该不对了，需要改了。
}

Status Replica::get(const GetParam& param, Value& value) {
    if (!engine_) {
        WARN("engine is nullptr, replica: table_id = {}, shard_index = {}",
             shard_id_.table_id, shard_id_.shard_index);
        return Status{StatusCode::ERROR, "engine is nullptr"};
    }

    RETURN_IF_INVALID_PARAM(param)

    Status status = engine_->get(param.key, value);
    if (status.fail()) {
        WARN("engine get is not ok, key = {}, msg = {}", param.key,
             status.msg());
    }
    return status;
}

Status Replica::handle_request_vote(const RequestVoteParam& param,
                                    RequestVoteResult& result) {
    result = {
        .term = current_term_,
        .vote_granted = false,
    };
    Status status = Status::OK();
    if (param.term < current_term_) {
        return Status::OK();
    } else if (param.term > current_term_) {
        // TODO
        // 这里这个函数会把自己的vote_for_给清空掉，会不会导致投了很多次票？
        // 还是说这样是没有问题的？
        status = become_follower(param.term);
        RETURN_IF_INVALID_STATUS(status)
        result.term = current_term_;
    }

    // 如果比人家的新 （是一定会新，不包括相等）
    if (later_than_other_replica(param.last_log_term, param.last_log_index)) {
        return Status::OK();
    }

    // 没有投过票，或者投过了，还是这个人
    // TODO
    // 如果投过了这个同一个人的话，其实这里vote_granted是true还是false都无所谓吧，反正对方已经拿到票了。
    if ((voted_for_.has_value() and
         voted_for_.value() == param.from_replica_id) or
        !voted_for_.has_value()) {
        result.vote_granted = true;
        voted_for_ = param.from_replica_id;
    }

    return status;
}

Term Replica::get_last_log_term() const {
    if (log_entries_.empty()) return 0;
    return log_entries_.back().term;
}
LogIndex Replica::get_last_log_index() const {
    if (log_entries_.empty()) return 0;
    return log_entries_.back().index;
}
bool Replica::later_than_other_replica(Term other_last_log_term,
                                       LogIndex other_last_log_index) const {
    if (get_last_log_term() != other_last_log_term) {
        return get_last_log_term() > other_last_log_term;
    }
    return get_last_log_index() > other_last_log_index;
}

Status Replica::become_follower(Term later_term) {
    current_term_ = later_term;
    role_ = ReplicaRole::FOLLOWER;
    voted_for_.reset();
    return Status::OK();
}

Status Replica::execute_election() {
    if (role_ == ReplicaRole::LEADER) {
        return Status::OK();
    }

    election_generation_++;
    voted_for_ = replica_id_;
    current_term_++;
    granted_vote_count_ = 1;

    for (PeerMember& member : members_) {
        if (member.replica_id == replica_id_) continue;
        if (role_ == ReplicaRole::FOLLOWER) break;

        // TODO 这里貌似还是同步发送的，所以其实election_generation_的影响不太大，以后是异步发送的时候
        // 就需要用到了

        //TODO 以后要改成异步的
        RETURN_IF_INVALID_STATUS(
            send_member_request_vote(member, election_generation_));
    }

    return Status::OK();
}

Status Replica::send_member_request_vote(const PeerMember& member,
                                         int32_t generation) {
    // TODO 我在思考，这个param我们其实相当于是混用的，
    //  本来param类应该是专门为了service那一层处理RPC的消息进行转化的时候做的，
    //  但是现在我们却在这个内部也这样用了param，会不会不太好？
    RequestVoteParam param{
        .from_replica_id = replica_id_,
        .to_replica_id = member.replica_id,
        .term = current_term_,
        .last_log_term = get_last_log_term(),
        .last_log_index = get_last_log_index(),
    };

    RequestVoteResult result;
    Status status =
        raft_sender_.send_request_vote(member, param, result); 
    RETURN_IF_INVALID_STATUS(status)
    return handle_vote_response(member, generation, result);
}

Status Replica::handle_vote_response(const PeerMember& member,
                                     int32_t generation,
                                     const RequestVoteResult& result) {
    if (generation != election_generation_) {
        return Status::OK();
    }

    if (!result.vote_granted) {
        return Status::OK();
    }

    if (result.term > current_term_) {
        become_follower(result.term);
        return Status::OK();
    }

    if (role_ != ReplicaRole::CANDIDATE) {
        return Status::OK();
    }
    ++granted_vote_count_;
    int limit_allow_leader_size = static_cast<int>(members_.size()) / 2 + 1;
    if (granted_vote_count_ >= limit_allow_leader_size) {
        role_ = ReplicaRole::LEADER;
        voted_for_ = replica_id_;
        // TODO 这里立刻发一次空 AppendEntries
    }

    return Status::OK();
}

}  // namespace adviskv::storage
