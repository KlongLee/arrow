# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

ARG base
FROM ${base}

ARG arch_alias
ARG arch_short_alias

RUN yum install -y git flex curl autoconf zip wget

# Install CMake
# AWS SDK doesn't work with CMake=3.22 due to https://gitlab.kitware.com/cmake/cmake/-/issues/22524
ARG cmake=3.21.4
RUN wget -q https://github.com/Kitware/CMake/releases/download/v${cmake}/cmake-${cmake}-Linux-${arch_alias}.tar.gz -O - | \
    tar -xzf - --directory /usr/local --strip-components=1

# Install Ninja
ARG ninja=1.10.2
RUN mkdir /tmp/ninja && \
    wget -q https://github.com/ninja-build/ninja/archive/v${ninja}.tar.gz -O - | \
    tar -xzf - --directory /tmp/ninja --strip-components=1 && \
    cd /tmp/ninja && \
    ./configure.py --bootstrap && \
    mv ninja /usr/local/bin && \
    rm -rf /tmp/ninja

# Install ccache
ARG ccache=4.1
RUN mkdir /tmp/ccache && \
    wget -q https://github.com/ccache/ccache/archive/v${ccache}.tar.gz -O - | \
    tar -xzf - --directory /tmp/ccache --strip-components=1 && \
    cd /tmp/ccache && \
    mkdir build && \
    cd build && \
    cmake -GNinja -DCMAKE_BUILD_TYPE=Release -DZSTD_FROM_INTERNET=ON .. && \
    ninja install && \
    rm -rf /tmp/ccache

# Install vcpkg
ARG vcpkg
COPY ci/vcpkg/*.patch \
     ci/vcpkg/*linux*.cmake \
     arrow/ci/vcpkg/
COPY ci/scripts/install_vcpkg.sh arrow/ci/scripts/
RUN arrow/ci/scripts/install_vcpkg.sh /opt/vcpkg ${vcpkg}
ENV PATH="/opt/vcpkg:${PATH}"

ARG build_type=release
ENV CMAKE_BUILD_TYPE=${build_type} \
    VCPKG_FORCE_SYSTEM_BINARIES=1 \
    VCPKG_OVERLAY_TRIPLETS=/arrow/ci/vcpkg \
    VCPKG_DEFAULT_TRIPLET=${arch_short_alias}-linux-static-${build_type} \
    VCPKG_FEATURE_FLAGS=-manifests

# Need to install the boost-build prior installing the boost packages, otherwise
# vcpkg will raise an error.
# TODO(kszucs): factor out the package enumeration to a text file and reuse it
# from the windows image and potentially in a future macos wheel build
RUN vcpkg install --clean-after-build \
        boost-build:${arch_short_alias}-linux && \
    vcpkg install --clean-after-build \
        abseil \
        aws-sdk-cpp[config,cognito-identity,core,identity-management,s3,sts,transfer] \
        boost-filesystem \
        brotli \
        bzip2 \
        c-ares \
        curl \
        flatbuffers \
        gflags \
        glog \
        google-cloud-cpp[core,storage] \
        grpc \
        lz4 \
        openssl \
        orc \
        protobuf \
        rapidjson \
        re2 \
        snappy \
        thrift \
        utf8proc \
        zlib \
        zstd

ARG python=3.6
ENV PYTHON_VERSION=${python}
RUN PYTHON_ROOT=$(find /opt/python -name cp${PYTHON_VERSION/./}-*) && \
    echo "export PATH=$PYTHON_ROOT/bin:\$PATH" >> /etc/profile.d/python.sh

SHELL ["/bin/bash", "-i", "-c"]
ENTRYPOINT ["/bin/bash", "-i", "-c"]

COPY python/requirements-wheel-build.txt /arrow/python/
RUN pip install -r /arrow/python/requirements-wheel-build.txt
