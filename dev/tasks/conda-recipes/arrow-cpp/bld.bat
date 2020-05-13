mkdir "%SRC_DIR%"\cpp\build
pushd "%SRC_DIR%"\cpp\build

cmake -G "Ninja" ^
      -DARROW_BOOST_USE_SHARED:BOOL=ON ^
      -DARROW_BUILD_STATIC:BOOL=OFF ^
      -DARROW_BUILD_TESTS:BOOL=OFF ^
      -DARROW_BUILD_UTILITIES:BOOL=OFF ^
      -DARROW_DATASET:BOOL=ON ^
      -DARROW_DEPENDENCY_SOURCE=SYSTEM ^
      -DARROW_FLIGHT:BOOL=ON ^
      -DARROW_GANDIVA:BOOL=ON ^
      -DARROW_HDFS:BOOL=ON ^
      -DARROW_MIMALLOC:BOOL=ON ^
      -DARROW_ORC:BOOL=ON ^
      -DARROW_PACKAGE_PREFIX="%LIBRARY_PREFIX%" ^
      -DARROW_PARQUET:BOOL=ON ^
      -DARROW_PYTHON:BOOL=ON ^
      -DARROW_S3:BOOL=ON ^
      -DARROW_SIMD_LEVEL=NONE ^
      -DARROW_WITH_BROTLI:BOOL=ON ^
      -DARROW_WITH_BZ2:BOOL=ON ^
      -DARROW_WITH_LZ4:BOOL=ON ^
      -DARROW_WITH_SNAPPY:BOOL=ON ^
      -DARROW_WITH_ZLIB:BOOL=ON ^
      -DARROW_WITH_ZSTD:BOOL=ON ^
      -DBoost_NO_BOOST_CMAKE=ON ^
      -DCMAKE_BUILD_TYPE=release ^
      -DCMAKE_INSTALL_PREFIX="%LIBRARY_PREFIX%" ^
      -DLLVM_TOOLS_BINARY_DIR="%LIBRARY_BIN%" ^
      -DPYTHON_EXECUTABLE="%PYTHON%" ^
      ..

cmake --build . --target install --config Release

popd
