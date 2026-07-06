#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "storage/model/model.h"

namespace adviskv::storage {

struct WriteProposal {
    WriteOpType op{WriteOpType::NONE};
    Key key;
    Value value;
};

struct NoopProposal {};

struct ConfigChangeProposal {
    WriteOpType op{WriteOpType::NONE};
    PeerMember member;  // 当是ADD_LEARNER的时候用这个字段，否则使用下面的replica_id字段
    ReplicaID target_replica_id;
};

struct ProposeParam {
    std::variant<WriteProposal, NoopProposal, ConfigChangeProposal> payload;

    static ProposeParam write(WriteOpType op, Key key, Value value) {
        return ProposeParam{WriteProposal{op, std::move(key), std::move(value)}};
    }

    static ProposeParam noop() { return ProposeParam{NoopProposal{}}; }

    static ProposeParam add_learner(PeerMember member) {
        return ProposeParam{ConfigChangeProposal{WriteOpType::ADD_LEARNER, std::move(member), ReplicaID{}}};
    }

    static ProposeParam promote_voter(ReplicaID replica_id) {
        return ProposeParam{ConfigChangeProposal{WriteOpType::PROMOTE_VOTER, PeerMember{}, replica_id}};
    }

    static ProposeParam remove_member(ReplicaID replica_id) {
        return ProposeParam{ConfigChangeProposal{WriteOpType::REMOVE_MEMBER, PeerMember{}, replica_id}};
    }
};

struct PutParam {
    const Key& key;
    const Value& value;

    Status validate() const { return Status::OK(); }
};

struct GetParam {
    const Key& key;
    Status validate() const { return Status::OK(); }
};

struct DelParam {
    const Key& key;
    Status validate() const { return Status::OK(); }
};

struct ReplicaRuntimeOptions {
    std::string data_dir;
    int32_t raft_rpc_timeout_ms{1000};

    bool operator==(const ReplicaRuntimeOptions& other) const {
        return data_dir == other.data_dir && raft_rpc_timeout_ms == other.raft_rpc_timeout_ms;
    }

    DEFINE_OPERATOR_NOT_EQUAL(ReplicaRuntimeOptions)
};

struct ReplicaInitParam {
    ReplicaID replica_id;
    EngineType engine_type;
    Endpoint local_endpoint;
    std::vector<PeerMember> members;
    ReplicaRuntimeOptions runtime;

    bool operator==(const ReplicaInitParam& other) const {
        if (!(replica_id == other.replica_id)) return false;
        if (!(engine_type == other.engine_type)) return false;
        if (!(local_endpoint == other.local_endpoint)) return false;
        if (!(members == other.members)) return false;
        if (!(runtime == other.runtime)) return false;
        return true;
    }

    DEFINE_OPERATOR_NOT_EQUAL(ReplicaInitParam)
};

}  // namespace adviskv::storage
