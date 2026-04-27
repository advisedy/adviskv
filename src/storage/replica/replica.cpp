#include "storage/replica/replica.h"

#include <cassert>
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
#include "storage/utility/timer.h"

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

    if (param.scheduler) {
        election_timer_ = std::make_shared<Timer>(
            param.scheduler, [this]() { this->execute_election(); });

        heartbeat_timer_ = std::make_shared<Timer>(
            param.scheduler, [this]() { this->execute_heartbeat(); });
    }

    commit_index_ = 0;
    last_applied_ = 0;
    next_index_.clear();
    match_index_.clear();

    return Status::OK();
}

Status Replica::put(const PutParam& param) {

    RETURN_IF_INVALID_PARAM(param)


    if (role_ != ReplicaRole::LEADER) {
        WARN("this replica is not leader");
        return Status{StatusCode::ERROR, "this replica is not leader"};
    }

    LogEntry entry{
        .term = current_term_,
        .index = get_last_log_index() + 1,
        .op_type = WriteOpType::PUT,
        .key = param.key,
        .value = param.value,
    };
    log_entries_.push_back(std::move(entry));

    Status status = Status::OK();
    // 给自己的member每一个人都发送entries
    RETURN_IF_INVALID_STATUS(send_members_append_entries())
    // 更新commit_idx
    RETURN_IF_INVALID_STATUS(try_update_commit_index())
    // 去检查一下last_apply 和 commit_idx的关系，进行实际状态机写入
    RETURN_IF_INVALID_STATUS(trace_commit_log_entries())
    return Status::OK();
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
        // 这里这个函数会把自己的vote_for_给清空掉，会不会导致投了很多次票
        // 但是应该没有问题，毕竟是这种情况应该是发生在多次投票里面，他们的term不一样
        // 既然不一样的话，肯定是最终term最大的那一个去当的leader了，别的会自动变成follower
        status = become_follower(param.term);
        RETURN_IF_INVALID_STATUS(status)
        result.term = current_term_;
    }

    // 如果比人家的新 （是一定会新，不包括相等）
    if (later_than_other_replica(param.last_log_term, param.last_log_index)) {
        return Status::OK();
    }

    // 没有投过票，或者投过了，还是这个人
    // 如果投过了这个同一个人的话，其实这里vote_granted是true还是false都无所谓吧，反正对方已经拿到票了。
    // 保障一致性，还是设置成true把
    if ((voted_for_.has_value() and
         voted_for_.value() == param.from_replica_id) or
        !voted_for_.has_value()) {
        result.vote_granted = true;
        voted_for_ = param.from_replica_id;
    }

    return status;
}

Status Replica::handle_append_entries(const AppendEntriesParam& param,
                                      AppendEntriesResult& result) {
    // TODO RETURN_IF_INVALID_PARAM(param)

    result = {
        .success = false,
        .term = current_term_,
    };
    if (param.term < current_term_) {
        return Status::OK();
    }

    const bool should_stop_heartbeat = (role_ == ReplicaRole::LEADER);
    if (param.term > current_term_ or role_ != ReplicaRole::FOLLOWER) {
        // 这里的判断条件是，只要role不是FOLLOWER，就会become，但是如果term和param的term是相等的
        // 然后prev_log_index不一样呢？ 这个时候不需要比较一下index吗？
        RETURN_IF_INVALID_STATUS(become_follower(param.term))
    }
    if (should_stop_heartbeat && heartbeat_timer_) {
        heartbeat_timer_->stop();
    }
    result.term = current_term_;

    if (param.prev_log_index > 0) {
        if (get_last_log_index() < param.prev_log_index) {
            return Status::OK();
        }

        const LogEntry& prev_entry = log_entries_[param.prev_log_index - 1];
        if (prev_entry.term != param.prev_log_term) {
            return Status::OK();
        }
    }

    if (param.entries.empty()) {
        if (election_timer_) {
            // 把自己的时间reset一下， 选举的
            election_timer_->reset_random(MILLISECONDS(150), MILLISECONDS(300));
        }
        result.success = true;
        // TODO 根据 leader_commit 推进 commit_index / last_applied
        return Status::OK();
    } else {
        // TODO 日志复制
        return Status::OK();
    }
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
    RETURN_IF_INVALID_CONDITION(
        later_term >= current_term_,
        "later_term should not be smaller than current_term");

    if (later_term > current_term_) {
        voted_for_.reset();
    }
    current_term_ = later_term;
    role_ = ReplicaRole::FOLLOWER;
    return Status::OK();
}

void Replica::execute_election() {
    if (role_ == ReplicaRole::LEADER) {
        return;
    }

    election_generation_++;
    voted_for_ = replica_id_;
    current_term_++;
    granted_vote_count_ = 1;
    role_ = ReplicaRole::CANDIDATE;

    // 先自己check一下可不可以当leader，毕竟万一raft的节点数只有1呢？
    if (members_.size() == 1) {
        role_ = ReplicaRole::LEADER;
        // TODO 这里立刻发一次空 AppendEntries

        return;
    }

    for (const PeerMember& member : members_) {
        if (member.replica_id == replica_id_) continue;
        if (role_ == ReplicaRole::FOLLOWER) break;

        // TODO
        // 这里貌似还是同步发送的，所以其实election_generation_的影响不太大，以后是异步发送的时候
        // 就需要用到了

        // TODO 以后要改成异步的
        Status status = send_member_request_vote(member, election_generation_);
        if (status.fail()) {
            WARN("[execute_election]: send_member_request_vote: fail: {}",
                 status.msg());
        }
    }

    return;
}

Status Replica::send_member_request_vote(const PeerMember& member,
                                         int32_t generation) {
    RequestVoteParam param{
        .from_replica_id = replica_id_,
        .to_replica_id = member.replica_id,
        .term = current_term_,
        .last_log_term = get_last_log_term(),
        .last_log_index = get_last_log_index(),
    };

    RequestVoteResult result;
    Status status = raft_sender_.send_request_vote(member, param, result);
    RETURN_IF_INVALID_STATUS(status)
    return handle_vote_response(member, generation, result);
}

Status Replica::handle_vote_response(const PeerMember& member,
                                     int32_t generation,
                                     const RequestVoteResult& result) {
    if (generation != election_generation_) {
        return Status::OK();
    }

    if (result.term > current_term_) {
        become_follower(result.term);
        return Status::OK();
    }

    if (!result.vote_granted) {
        return Status::OK();
    }

    if (role_ != ReplicaRole::CANDIDATE) {
        return Status::OK();
    }

    ++granted_vote_count_;

    if (role_ == ReplicaRole::LEADER) {  // 之后leader不会再走了
        return Status::OK();
    }

    int limit_allow_leader_size = static_cast<int>(members_.size()) / 2 + 1;
    if (granted_vote_count_ >= limit_allow_leader_size) {
        role_ = ReplicaRole::LEADER;
        voted_for_ = replica_id_;
        // TODO 这里立刻发一次空 AppendEntries
        execute_heartbeat();
    }

    return Status::OK();
}

void Replica::execute_heartbeat() {
    if (role_ != ReplicaRole::LEADER) {
        return;
    }
    Status status = Status::OK();
    for (const auto& member : members_) {
        if (member.replica_id == replica_id_) {
            continue;
        }

        AppendEntriesParam param{
            .from_replica_id = replica_id_,
            .to_replica_id = member.replica_id,
            .term = current_term_,
            .prev_log_index = -1,  // 疑似不用
            .prev_log_term = -1,   // 疑似不用
            .entries = {},
            .leader_commit = 0,  // TODO
        };

        AppendEntriesResult result;
        status = raft_sender_.send_append_entries(member, param, result);
        if (status.fail()) {
            WARN("[broadcast_heartbeat] send fail: {}", status.msg());
            if (result.term > current_term_) {
                status = become_follower(result.term);
                if (status.fail()) {
                    // TODO
                }
                return;
            }
            continue;
        }
    }

    return;
}

Status Replica::send_members_append_entries() {
    if (role_ != ReplicaRole::LEADER) {
        return Status{StatusCode::ERROR, "this is not leader"};
    }

    const LogIndex leader_last_index = get_last_log_index();
    Status status = Status::OK();

    for (const PeerMember& member : members_) {
        if (member.replica_id == replica_id_) {
            continue;
        }

        if (!next_index_.count(member.replica_id)) {
            next_index_[member.replica_id] = leader_last_index + 1;
        }
        if (!match_index_.count(member.replica_id)) {
            match_index_[member.replica_id] = 0;
        }

        // 对单个 follower 做追平
        // 万一中间自己不是leader，就跳出来了直接
        while (role_ == ReplicaRole::LEADER) {
            LogIndex next_index = next_index_[member.replica_id];

            assert(next_index > 0);

            LogIndex prev_log_index = next_index - 1;
            Term prev_log_term = (prev_log_index == 0)
                                     ? 0
                                     : log_entries_[prev_log_index - 1].term;

            AppendEntriesParam param{
                .from_replica_id = replica_id_,
                .to_replica_id = member.replica_id,
                .term = current_term_,
                .prev_log_index = prev_log_index,
                .prev_log_term = prev_log_term,
                .leader_commit = commit_index_,
            };

            for (LogIndex idx = next_index; idx <= leader_last_index; ++idx) {
                param.entries.push_back(log_entries_[idx - 1]);
            }

            AppendEntriesResult result;
            status = raft_sender_.send_append_entries(member, param, result);

            if (status.fail()) {
                WARN("[send_members_append_entries] rpc fail, peer={}, msg={}",
                     member.node_id, status.msg());
                break;
            }

            // 发现更高 term，自己立即退位
            if (result.term > current_term_) {
                RETURN_IF_INVALID_STATUS(become_follower(result.term))
                return Status::OK();
            }

            // 复制成功
            if (result.success) {
                match_index_[member.replica_id] = leader_last_index;
                next_index_[member.replica_id] = leader_last_index + 1;
                break;
            }

            // 应该是 prev_log 对不上，往前回退试试
            if (next_index_[member.replica_id] > 1) {
                --next_index_[member.replica_id];
            } else {
                // 已经退到最前面了，再失败就先放弃这一轮，避免死转
                break;
            }
        }
    }

    return Status::OK();
}

Status Replica::try_update_commit_index() {
    for (LogIndex idx = commit_index_ + 1; idx <= get_last_log_index(); ++idx) {
        int success_cnt = 1;
        for (const auto& member : members_) {
            if (member.replica_id == replica_id_) {
                continue;
            }
            if (auto it = match_index_.find(member.replica_id);
                it != match_index_.end()) {
                if (it->second >= idx) success_cnt++;
            } else {
                WARN("...");
            }
        }

        int limit_cnt = static_cast<int>(members_.size()) / 2 + 1;
        if (success_cnt >= limit_cnt) {
            commit_index_ = idx;
        }
    }
    return Status::OK();
}

Status Replica::apply_log_entry(const LogEntry& entry) {
    if (!engine_) {
        return {StatusCode::ERROR, "engine is nullptr"};
    }

    switch (entry.op_type) {
        case WriteOpType::PUT:
            return engine_->put(entry.key, entry.value);
        case WriteOpType::DEL:
            return engine_->del(entry.key);
        case WriteOpType::NONE: {
            WARN("WriteOpType NONE");
            return Status::OK();
        }
        default:
            return {StatusCode::INVALID_ARGUMENT, "unknown log op type"};
    }
}

Status Replica::trace_commit_log_entries() {
    while (last_applied_ < commit_index_) {
        const LogIndex next_to_apply = last_applied_ + 1;
        const LogEntry& entry = log_entries_[next_to_apply - 1];

        Status status = apply_log_entry(entry);
        if (status.fail()) {
            return status;
        }
        last_applied_ = next_to_apply;
    }
    return Status::OK();
}

}  // namespace adviskv::storage
