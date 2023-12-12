git clone -b compactor --depth 1 https://github.com/GeoffreyXue/metarocks/tree/compactor rocksdb
cd rocksdb
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Debug -GNinja
ninja -j 8
cd ../..

git clone --recurse-submodules https://github.com/aws/aws-sdk-cpp
cd aws-sdk-cpp/
mkdir sdk_build
cd sdk_build/
cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_PREFIX_PATH=/usr/local/ -DCMAKE_INSTALL_PREFIX=/usr/local/  -DBUILD_ONLY="sqs"
make -j 8
make install

cd ../..
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=1
make
