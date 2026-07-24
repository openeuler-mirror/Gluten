#!/bin/bash

set -exu

BASEDIR=$(cd "$(dirname "$0")"; pwd)
GLUTEN_DIR="$BASEDIR/.."

BUILD_TYPE=Release
BUILD_TESTS=OFF
BUILD_EXAMPLES=OFF
BUILD_BENCHMARKS=OFF
ENABLE_JEMALLOC_STATS=OFF
ENABLE_QAT=OFF
ENABLE_IAA=OFF
ENABLE_HBM=OFF
ENABLE_GCS=OFF
ENABLE_S3=OFF
ENABLE_HDFS=ON
ENABLE_ABFS=OFF
RUN_SETUP_SCRIPT=OFF
VELOX_HOME=""
VELOX_BUILD_PATH=""
PROTOBUF_LIBRARY=""
PROTOBUF_INCLUDE_DIR=""
PROTOBUF_PROTOC_EXECUTABLE=""
THRIFT_INCLUDE_DIR=""
BUILD_ARROW=OFF
SPARK_VERSION=ALL

if [[ "$(uname)" == "Darwin" ]]; then
  physical_cpu_cores=$(sysctl -n hw.physicalcpu)
  ignore_cores=2
  if [ "$physical_cpu_cores" -gt "$ignore_cores" ]; then
    NUM_THREADS=${NUM_THREADS:-$(($physical_cpu_cores - $ignore_cores))}
  else
    NUM_THREADS=${NUM_THREADS:-$physical_cpu_cores}
  fi
else
  NUM_THREADS=${NUM_THREADS:-$(nproc --ignore=2)}
fi

for arg in "$@"; do
  case $arg in
    --build_type=*)
      BUILD_TYPE=("${arg#*=}")
      shift
      ;;
    --build_tests=*)
      BUILD_TESTS=("${arg#*=}")
      shift
      ;;
    --build_examples=*)
      BUILD_EXAMPLES=("${arg#*=}")
      shift
      ;;
    --build_benchmarks=*)
      BUILD_BENCHMARKS=("${arg#*=}")
      shift
      ;;
    --enable_jemalloc_stats=*)
      ENABLE_JEMALLOC_STATS=("${arg#*=}")
      shift
      ;;
    --enable_qat=*)
      ENABLE_QAT=("${arg#*=}")
      shift
      ;;
    --enable_iaa=*)
      ENABLE_IAA=("${arg#*=}")
      shift
      ;;
    --enable_hbm=*)
      ENABLE_HBM=("${arg#*=}")
      shift
      ;;
    --enable_gcs=*)
      ENABLE_GCS=("${arg#*=}")
      shift
      ;;
    --enable_s3=*)
      ENABLE_S3=("${arg#*=}")
      shift
      ;;
    --enable_hdfs=*)
      ENABLE_HDFS=("${arg#*=}")
      shift
      ;;
    --enable_abfs=*)
      ENABLE_ABFS=("${arg#*=}")
      shift
      ;;
    --run_setup_script=*)
      RUN_SETUP_SCRIPT=("${arg#*=}")
      shift
      ;;
    --velox_home=*)
      VELOX_HOME=("${arg#*=}")
      shift
      ;;
    --velox_build_path=*)
      VELOX_BUILD_PATH=("${arg#*=}")
      shift
      ;;
    --protobuf_library=*)
      PROTOBUF_LIBRARY=("${arg#*=}")
      shift
      ;;
    --protobuf_include_dir=*)
      PROTOBUF_INCLUDE_DIR=("${arg#*=}")
      shift
      ;;
    --protobuf_protoc_executable=*)
      PROTOBUF_PROTOC_EXECUTABLE=("${arg#*=}")
      shift
      ;;
    --thrift_include_dir=*)
      THRIFT_INCLUDE_DIR=("${arg#*=}")
      shift
      ;;
    --build_arrow=*)
      BUILD_ARROW=("${arg#*=}")
      shift
      ;;
    --num_threads=*)
      NUM_THREADS=("${arg#*=}")
      shift
      ;;
    --spark_version=*)
      SPARK_VERSION=("${arg#*=}")
      shift
      ;;
    *)
      OTHER_ARGUMENTS+=("$1")
      shift
      ;;
  esac
done

function build_for_spark {
  spark_version=$1
  cd "$GLUTEN_DIR"
  mvn clean package -Pbackends-velox -Pceleborn -Puniffle -Pspark-$spark_version -DskipTests -Dspotless.check.skip=true -Dscalastyle.skip=true -Dcheckstyle.skip=true
}

function check_supported {
  PLATFORM=$(mvn help:evaluate -Dexpression=platform -q -DforceStdout)
  ARCH=$(mvn help:evaluate -Dexpression=arch -q -DforceStdout)
  if [ "$PLATFORM" == "null object or invalid expression" ] || [ "$ARCH" == "null object or invalid expression" ]; then
    OS_NAME=$(mvn help:evaluate -Dexpression=os.name -q -DforceStdout)
    OS_ARCH=$(mvn help:evaluate -Dexpression=os.arch -q -DforceStdout)
    echo "$OS_NAME-$OS_ARCH is not supported by current Gluten build."
    exit 1
  fi
}

function build_arrow {
  cd "$GLUTEN_DIR/dev"
  ./build_arrow.sh
}

function resolve_velox_protobuf {
  local protobuf_build_path="$VELOX_BUILD_PATH/_deps/protobuf-build"
  local protobuf_src_path="$VELOX_BUILD_PATH/_deps/protobuf-src"

  if [[ -z "$PROTOBUF_LIBRARY" && -f "$protobuf_build_path/libprotobuf.a" ]]; then
    PROTOBUF_LIBRARY="$protobuf_build_path/libprotobuf.a"
  fi
  if [[ -z "$PROTOBUF_INCLUDE_DIR" && -d "$protobuf_src_path/src/google/protobuf" ]]; then
    PROTOBUF_INCLUDE_DIR="$protobuf_src_path/src"
  fi
  if [[ -z "$PROTOBUF_PROTOC_EXECUTABLE" && -x "$protobuf_build_path/protoc" ]]; then
    PROTOBUF_PROTOC_EXECUTABLE="$protobuf_build_path/protoc"
  fi
}

function resolve_velox_thrift {
  local arrow_build_path="$VELOX_BUILD_PATH/CMake/resolve_dependency_modules/arrow/arrow_ep/src/arrow_ep-build"
  local thrift_install_include="$arrow_build_path/thrift_ep-install/include"

  if [[ -z "$THRIFT_INCLUDE_DIR" && -f "$thrift_install_include/thrift/TApplicationException.h" ]]; then
    THRIFT_INCLUDE_DIR="$thrift_install_include"
  fi
}

function build_gluten_cpp {
  echo "Start to build Gluten CPP against installed Velox"
  resolve_velox_protobuf
  resolve_velox_thrift
  cd "$GLUTEN_DIR/cpp"
  rm -rf build
  mkdir build
  cd build

  CMAKE_ARGS=(
    -DBUILD_VELOX_BACKEND=ON
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE"
    -DVELOX_HOME="$VELOX_HOME"
    -DBUILD_TESTS="$BUILD_TESTS"
    -DBUILD_EXAMPLES="$BUILD_EXAMPLES"
    -DBUILD_BENCHMARKS="$BUILD_BENCHMARKS"
    -DENABLE_JEMALLOC_STATS="$ENABLE_JEMALLOC_STATS"
    -DENABLE_HBM="$ENABLE_HBM"
    -DENABLE_QAT="$ENABLE_QAT"
    -DENABLE_IAA="$ENABLE_IAA"
    -DENABLE_GCS="$ENABLE_GCS"
    -DENABLE_S3="$ENABLE_S3"
    -DENABLE_HDFS="$ENABLE_HDFS"
    -DENABLE_ABFS="$ENABLE_ABFS"
  )

  if [[ -n "$VELOX_BUILD_PATH" ]]; then
    CMAKE_ARGS+=(-DVELOX_BUILD_PATH="$VELOX_BUILD_PATH")
  fi
  if [[ -n "$PROTOBUF_LIBRARY" ]]; then
    CMAKE_ARGS+=(-DProtobuf_LIBRARY="$PROTOBUF_LIBRARY")
  fi
  if [[ -n "$PROTOBUF_INCLUDE_DIR" ]]; then
    CMAKE_ARGS+=(-DProtobuf_INCLUDE_DIR="$PROTOBUF_INCLUDE_DIR")
  fi
  if [[ -n "$PROTOBUF_PROTOC_EXECUTABLE" ]]; then
    CMAKE_ARGS+=(-DProtobuf_PROTOC_EXECUTABLE="$PROTOBUF_PROTOC_EXECUTABLE")
  fi
  if [[ -n "$THRIFT_INCLUDE_DIR" ]]; then
    CMAKE_ARGS+=(-DTHRIFT_INCLUDE_DIR="$THRIFT_INCLUDE_DIR")
  fi

  cmake "${CMAKE_ARGS[@]}" ..
  make -j "$NUM_THREADS"
}

if [[ -z "$VELOX_HOME" ]]; then
  echo "--velox_home is required for buildbundle-from-installed-velox.sh"
  exit 1
fi

if [[ -z "$VELOX_BUILD_PATH" ]]; then
  if [[ "$BUILD_TYPE" == "Debug" ]] || [[ "$BUILD_TYPE" == "debug" ]]; then
    VELOX_BUILD_PATH="$VELOX_HOME/_build/debug"
  else
    VELOX_BUILD_PATH="$VELOX_HOME/_build/release"
  fi
fi

if [[ ! -d "$VELOX_HOME" ]]; then
  echo "Velox home does not exist: $VELOX_HOME"
  exit 1
fi

if [[ ! -d "$VELOX_BUILD_PATH" ]]; then
  echo "Velox build path does not exist: $VELOX_BUILD_PATH"
  exit 1
fi

if [ "$SPARK_VERSION" = "3.2" ] || [ "$SPARK_VERSION" = "3.3" ] || [ "$SPARK_VERSION" = "3.4" ] || [ "$SPARK_VERSION" = "3.5" ] || [ "$SPARK_VERSION" = "ALL" ]; then
  echo "Building for Spark $SPARK_VERSION"
else
  echo "Invalid Spark version: $SPARK_VERSION"
  exit 1
fi

OS=$(uname -s)
source "$GLUTEN_DIR/dev/build_helper_functions.sh"
if [ -z "${GLUTEN_VCPKG_ENABLED:-}" ] && [ "$RUN_SETUP_SCRIPT" == "ON" ]; then
  echo "Start to install dependencies using existing Velox repo"
  pushd "$VELOX_HOME"
  if [ "$OS" == "Linux" ]; then
    setup_linux
  elif [ "$OS" == "Darwin" ]; then
    setup_macos
  else
    echo "Unsupported kernel: $OS"
    exit 1
  fi
  if [ "$ENABLE_S3" == "ON" ]; then
    if [ "$OS" == "Darwin" ]; then
      echo "S3 is not supported on MacOS."
      exit 1
    fi
    "${VELOX_HOME}/scripts/setup-adapters.sh" aws
  fi
  if [ "$ENABLE_HDFS" == "ON" ]; then
    if [ "$OS" == "Darwin" ]; then
      echo "HDFS is not supported on MacOS."
      exit 1
    fi
    install_libhdfs3
  fi
  if [ "$ENABLE_GCS" == "ON" ]; then
    "${VELOX_HOME}/scripts/setup-adapters.sh" gcs
  fi
  if [ "$ENABLE_ABFS" == "ON" ]; then
    export AZURE_SDK_DISABLE_AUTO_VCPKG=ON
    "${VELOX_HOME}/scripts/setup-adapters.sh" abfs
  fi
  popd
fi

cd "$GLUTEN_DIR"
check_supported

if [ "$BUILD_ARROW" == "ON" ]; then
  build_arrow
fi
build_gluten_cpp

if [ "$SPARK_VERSION" = "ALL" ]; then
  for spark_version in 3.2 3.3 3.4 3.5
  do
    build_for_spark "$spark_version"
  done
else
  build_for_spark "$SPARK_VERSION"
fi
