cmake_minimum_required(VERSION 3.16)

project(remote-compactor)

set(CMAKE_CXX_STANDARD 17)

# RocksDB Configuration
set(ROCKSDB_INCLUDE_DIR /home/ubuntu/compaction-server/rocksdb/include)
set(ROCKSDB_LIBRARY /home/ubuntu/compaction-server/rocksdb/librocksdb.a)
include_directories(${ROCKSDB_INCLUDE_DIR})

# Include AWS SDK
find_package(AWSSDK REQUIRED COMPONENTS sqs)

add_executable(remote-compaction src/main.cpp)
target_link_libraries(remote-compaction PRIVATE ${ROCKSDB_LIBRARY} snappy lz4 zstd bz2 pthread)
target_link_libraries(remote-compaction PRIVATE aws-cpp-sdk-core aws-cpp-sdk-sqs)
