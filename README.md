



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


  #################
  todo: 
  - TableMeta，DBMeta里面加上个时间戳

meta: 负责元数据，以及提供一些create_table，create_db这样的接口。 sdk侧会调用create_table的函数，然后meta会先在这里创建之后，通知sdm会对于这个新创建的table进行node的分配等操作。其余的暂时先没列出来。

sdm: 负责副本，路由表的信息， sdk侧如果是执行get或者put函数，会先通过sdm拿到路由表，然后直接连接到对应的ip:port，然后往对应的storage侧写入或查询数据。

storage: 负责存储。 

首先，这边查询的时候是指定db和table，然后分片是按照table来的。这样可以做到支持某些table热点访问的时候可以支持
单独给某个table进行分片和扩副本的操作。

