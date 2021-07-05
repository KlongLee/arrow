#!/usr/bin/env bash

arrow_dir=$1
build_dir=$2

export ARROW_FLIGHT=OFF
export ARROW_JEMALLOC=OFF
export ARROW_SIMD_LEVEL=NONE
export BUILD_PREFIX=$build_dir
export CONFIG_PATH=/dev/null
export MACOSX_DEPLOYMENT_TARGET=11.0
export MB_PYTHON_VERSION=3.9
export PLAT=arm64
export PYARROW_BUILD_VERBOSE=1
export VCPKG_DEFAULT_TRIPLET=arm64-osx-static-release
export VCPKG_FEATURE_FLAGS=-manifests
export VCPKG_OVERLAY_TRIPLETS=/Users/ursa/kszucs/arrow/ci/vcpkg
export VCPKG_ROOT=/Users/ursa/kszucs/vcpkg

source $arrow_dir/../multibuild/travis_osx_steps.sh
before_install

pip install numpy delocate cython setuptools_scm wheel

rm -rf build
$arrow_dir/ci/scripts/python_wheel_macos_build.sh $arrow_dir $build_dir