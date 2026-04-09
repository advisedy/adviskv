



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
  