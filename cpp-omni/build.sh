#!/bin/bash
#  Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

set -e

install_fmt() {
  echo "Start build fmt.so"
  local fmt_so_core="${OMNI_HOME}/lib/libfmt.so.10"
  local fmt_tag="10.1.1"
  local fmt_repo="https://gitee.com/mirrors/fmt.git"
  local open_source_dir="open_source"
  local workspace=$(readlink -f $(dirname "$BASH_SOURCE"))
  local fmt_source_dir="${workspace}/${open_source_dir}/fmt"
  local fmt_build_dir="${fmt_source_dir}/build"

  if [ ! -f "${fmt_so_core}" ]; then
    echo ">>>>> libfmt.so.10 not found in ${OMNI_HOME}/lib, start to clone fmt-${fmt_tag} source code and build..."
    rm -rf ${fmt_source_dir} && mkdir -p ${fmt_source_dir}
    git clone --branch ${fmt_tag} --depth=1 ${fmt_repo} ${fmt_source_dir}
    cd ${fmt_source_dir} && mkdir -p build && cd build
    # Cmake build with your specified params
    cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DFMT_TEST=OFF \
    -DFMT_DOC=OFF \
    -DFMT_INSTALL=ON \
    -DBUILD_SHARED_LIBS=ON
    make -j$(nproc)
    sudo make install
    # Copy all generated fmt so files to OMNI_HOME/lib directly
    sudo cp -f libfmt.so* ${OMNI_HOME}/lib/
    echo ">>>>> All fmt shared libraries copied to ${OMNI_HOME}/lib successfully."
    # Back to workspace
    cd ../../../../
  else
    echo ">>>>> libfmt.so.10 already exists in ${OMNI_HOME}/lib, skip fmt build process."
  fi
}

install_folly() {
  echo "Start build folly"
  local folly_tag="v2024.07.01.00"
  local folly_repo="https://gitee.com/mirrors/folly.git"
  local open_source_dir="open_source"
  local workspace=$(readlink -f $(dirname "$BASH_SOURCE"))
  local folly_source_dir="${workspace}/${open_source_dir}/folly"
  echo ">>>>> Start to clone folly-${folly_tag} source code and build..."
  rm -rf ${folly_source_dir} && mkdir -p ${folly_source_dir}
  git clone --branch ${folly_tag} --depth=1 ${folly_repo} ${folly_source_dir}
  cd ${folly_source_dir} && mkdir -p build && cd build
  cmake .. -DBUILD_TESTS=OFF -DFOLLY_HAVE_INT128_T=ON
  make -j$(nproc)
  sudo make install
  echo ">>>>> folly-${folly_tag} build and install completed successfully."
  cd ../../../../
}

if [ -z "$OMNI_HOME" ]; then
  echo "OMNI_HOME is empty"
  OMNI_HOME=/opt
fi

export OMNI_INCLUDE_PATH=$OMNI_HOME/lib/include
export OMNI_INCLUDE_PATH=$OMNI_INCLUDE_PATH:$OMNI_HOME/lib
export CPLUS_INCLUDE_PATH=$OMNI_INCLUDE_PATH:$CPLUS_INCLUDE_PATH
echo "OMNI_INCLUDE_PATH=$OMNI_INCLUDE_PATH"

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
echo $CURRENT_DIR
cd ${CURRENT_DIR}
if [ -d build ]; then
    rm -r build
fi
mkdir build
cd build

# options
if [ $# != 0 ] ; then
  options=""
  if [ $1 = 'debug' ]; then
    echo "-- Enable Debug"
    options="$options -DCMAKE_BUILD_TYPE=Debug -DDEBUG_RUNTIME=ON"
  elif [ $1 = 'trace' ]; then
    echo "-- Enable Trace"
    options="$options -DCMAKE_BUILD_TYPE=Debug -DTRACE_RUNTIME=ON"
  elif [ $1 = 'release' ];then
    echo "-- Enable Release"
    options="$options -DCMAKE_BUILD_TYPE=Release"
  elif [ $1 = 'test' ];then
    echo "-- Enable Test"
    options="$options -DCMAKE_BUILD_TYPE=Test"
  elif [ $1 = 'coverage' ]; then
    echo "-- Enable Coverage"
    options="$options -DCMAKE_BUILD_TYPE=Debug -DDEBUG_RUNTIME=ON -DCOVERAGE=ON"
  else
    echo "-- Enable Release"
    options="$options -DCMAKE_BUILD_TYPE=Release"
  fi
  cmake .. $options \
    -DBUILD_CPP_TESTS=ON \
    -DSTRINGVIEW_ENABLE=${STRINGVIEW_ENABLE:-OFF}
else
  echo "-- Enable Release"
  install_fmt
  install_folly
  cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_CPP_TESTS=OFF \
    -DSTRINGVIEW_ENABLE=${STRINGVIEW_ENABLE:-OFF}
fi

make -j$(nproc)

if [ $# != 0 ] ; then
  if [ $1 = 'coverage' ]; then
    ./test/tptest --gtest_output=xml:test_detail.xml
    lcov --d ../ --c --output-file test.info --rc lcov_branch_coverage=1
    lcov --remove test.info '*/opt/lib/include/*' '*test/*' '*build/src/*' '*/usr/include/*' '*/usr/lib/*' '*/usr/lib64/*' '*/usr/local/include/*' '*/usr/local/lib/*' '*/usr/local/lib64/*' -o final.info --rc lcov_branch_coverage=1
    genhtml final.info -o test_coverage --branch-coverage --rc lcov_branch_coverage=1
  fi
fi

set +eu
