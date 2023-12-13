git clone --depth 1 https://github.com/GeoffreyXue/metaarrow arrow
cd arrow/cpp
mkdir build
cd build
cmake .. -DCMAKE_PREFIX_PATH=/usr/local/ -DCMAKE_INSTALL_PREFIX=/usr/local/ -DARROW_PARQUET=ON -DARROW_JSON=ON -DARROW_BUILD_STATIC=OFF -DARROW_BUILD_SHARED=ON -GNinja
ninja -j 8
ninja install
cd ../../..

git clone -b compactor --depth 1 https://github.com/GeoffreyXue/metarocks rocksdb
cd rocksdb
mkdir build
cd build
cmake .. -DCMAKE_PREFIX_PATH=/usr/local/ -DCMAKE_BUILD_TYPE=Debug -DROCKSDB_BUILD_SHARED=OFF -DWITH_TESTS=OFF -DWITH_ALL_TESTS=OFF -DWITH_TOOLS=OFF -DWITH_BENCHMARK_TOOLS=OFF -DWITH_CORE_TOOLS=OFF -DWITH_TRACE_TOOLS=OFF -GNinja
ninja -j 8
cd ../..

git clone --recurse-submodules --depth 1 https://github.com/aws/aws-sdk-cpp
cd aws-sdk-cpp/
mkdir sdk_build
cd sdk_build/
cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_PREFIX_PATH=/usr/local/ -DCMAKE_INSTALL_PREFIX=/usr/local/ -DBUILD_ONLY="sqs"
make -j 8
make install
cd ../..

mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=1 -GNinja
ninja
cd ..
