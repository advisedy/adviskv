



mkdir build/generated

./build/vcpkg_installed/arm64-osx/tools/protobuf/protoc \
  -I=proto \
  --cpp_out=./build/generated \
  --grpc_out=./build/generated \
  --plugin=protoc-gen-grpc=./build/vcpkg_installed/arm64-osx/tools/grpc/grpc_cpp_plugin \
  proto/common.proto \
  proto/storage.proto \
  proto/meta.proto \
  proto/sdm.proto


find ./src/sdm -name '*.h' -o -name '*.cpp' | xargs clang-format -i


  #################
  todo: 
  - TableMeta，DBMeta里面加上个时间戳
  - 有些DB里面还没有放zone的概念




meta: 负责元数据，以及提供一些create_table，create_db这样的接口。 sdk侧会调用create_table的函数，然后meta会先在这里创建之后，通知sdm会对于这个新创建的table进行node的分配等操作。其余的暂时先没列出来。

sdm: 负责副本，路由表的信息， sdk侧如果是执行get或者put函数，会先通过sdm拿到路由表，然后直接连接到对应的ip:port，然后往对应的storage侧写入或查询数据。

storage: 负责存储。 

首先，这边查询的时候是指定db和table，然后分片是按照table来的。这样可以做到支持某些table热点访问的时候可以支持
单独给某个table进行分片和扩副本的操作。

有一个zone的概念，create db的时候需要填写，不写就是默认空。 
然后sdm那边分配node给table的时候，node必须和只有和这个table所在的db是同一个zone。



- Node.spec.resource_pool ： NodeService
- Node.spec.dc ： NodeService
- Node.state.endpoint ： HeartbeatService
- Node.state.status ： HeartbeatService 或拆出 effective_status
- Node.state.last_heartbeat_ts ： HeartbeatService
- NodeDerived.leader_count ： RuntimeIndex /扫描任务
- NodeDerived.owned_replica_count ： RuntimeIndex /扫描任务`
- Replica.spec.assign_node_id ： ReplicaScheduler
- Replica.spec.role ：leader/route 决策逻辑 
- Replica.state.role ： HeartbeatService
- Replica.state.status ： HeartbeatService
- Replica.state.endpoint ： HeartbeatService
- ShardRoute ： RouteUpdateCheckTask
