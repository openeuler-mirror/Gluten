include_directories(SYSTEM "/usr/local/include")

set (PROJ_TARGET spark_columnar_plugin)

file(GLOB_RECURSE SOURCE_FILES "*.cpp" "*.cc")
if (NOT STRINGVIEW_ENABLE)
    list(FILTER SOURCE_FILES EXCLUDE REGEX ".*/compute/OmniRowToColumnarConverter\\.cpp$")
endif ()
# UDF/UDAF examples are separate shared libs (see udf/examples/CMakeLists.txt).
list(FILTER SOURCE_FILES EXCLUDE REGEX ".*udf/examples/.*")

#Find required protobuf package
find_package(Protobuf REQUIRED)
if(PROTOBUF_FOUND)
    message(STATUS "protobuf library found")
else()
    message(FATAL_ERROR "protobuf library is needed but cant be found")
endif()

include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})
set(PROTOC_BIN ${Protobuf_PROTOC_EXECUTABLE})
set(PROTOBUF_INCLUDE "${Protobuf_INCLUDE_DIRS}" CACHE PATH "Protobuf include path")

find_package(fmt REQUIRED)

# Find Apache Arrow (core library only)
find_package(Arrow QUIET)
if(Arrow_FOUND)
    message(STATUS "Arrow library found via CMake config")
    include_directories(${ARROW_INCLUDE_DIR})
    set(ARROW_INCLUDE_DIR ${ARROW_INCLUDE_DIR} CACHE PATH "Arrow include directory")
    set(ARROW_LINK_LIBRARIES arrow)
else()
    # Fallback: try pkg_check_modules
    find_package(PkgConfig QUIET)
    if(PKG_CONFIG_FOUND)
        pkg_check_modules(ARROW_PC arrow)
        if(ARROW_PC_FOUND)
            message(STATUS "Arrow library found via pkg-config")
            include_directories(${ARROW_PC_INCLUDE_DIRS})
            link_directories(${ARROW_PC_LIBRARY_DIRS})
            set(ARROW_INCLUDE_DIR ${ARROW_PC_INCLUDE_DIRS} CACHE PATH "Arrow include directory")
            set(ARROW_LINK_LIBRARIES arrow)
        endif()
    endif()
    if(NOT ARROW_PC_FOUND)
        # Manual search fallback
        find_path(ARROW_INCLUDE_DIR arrow/api.h
            PATHS /usr/local/include /usr/include
            HINTS ${ARROW_HOME}/include ENV ARROW_HOME
        )
        find_library(ARROW_LIBRARY arrow
            PATHS /usr/local/lib /usr/lib
            HINTS ${ARROW_HOME}/lib ENV ARROW_HOME
        )
        if(ARROW_INCLUDE_DIR AND ARROW_LIBRARY)
            message(STATUS "Arrow library found via manual search")
            include_directories(${ARROW_INCLUDE_DIR})
            set(ARROW_INCLUDE_DIR ${ARROW_INCLUDE_DIR} CACHE PATH "Arrow include directory")
            set(ARROW_LINK_LIBRARIES ${ARROW_LIBRARY})
        else()
            message(FATAL_ERROR "Apache Arrow core library not found. "
                "Please install Arrow or set ARROW_HOME to the Arrow installation prefix. "
                "See https://arrow.apache.org/install/ for installation instructions.")
        endif()
    endif()
endif()

# generate Substrait proto
# Set up Substrait Proto
set(SUBSTRAIT_PROTO_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/")
message(STATUS "Set Substrait Proto output directory in ${SUBSTRAIT_PROTO_OUTPUT_DIR}")
# List Substrait Proto compiled files
get_filename_component(OMNI_ADAPTOR_HOME ${CMAKE_SOURCE_DIR} DIRECTORY)
set(SUBSTRAIT_PROTO_SRC_DIR
        ${GLUTEN_HOME}/gluten-substrait/src/main/resources/substrait/proto)
message(STATUS "Set Substrait Proto Directory in ${SUBSTRAIT_PROTO_SRC_DIR}")
file(GLOB SUBSTRAIT_PROTO_FILES ${SUBSTRAIT_PROTO_SRC_DIR}/substrait/*.proto
        ${SUBSTRAIT_PROTO_SRC_DIR}/substrait/extensions/*.proto)
FOREACH(PROTO ${SUBSTRAIT_PROTO_FILES})
    file(RELATIVE_PATH REL_PROTO ${SUBSTRAIT_PROTO_SRC_DIR} ${PROTO})
    string(REGEX REPLACE "\\.proto" "" PROTO_NAME ${REL_PROTO})
    LIST(APPEND SUBSTRAIT_PROTO_SRCS "${SUBSTRAIT_PROTO_OUTPUT_DIR}/${PROTO_NAME}.pb.cc")
    LIST(APPEND SUBSTRAIT_PROTO_HDRS "${SUBSTRAIT_PROTO_OUTPUT_DIR}/${PROTO_NAME}.pb.h")
ENDFOREACH()
set(SUBSTRAIT_PROTO_OUTPUT_FILES ${SUBSTRAIT_PROTO_HDRS} ${SUBSTRAIT_PROTO_SRCS})
set_source_files_properties(${SUBSTRAIT_PROTO_OUTPUT_FILES} PROPERTIES GENERATED TRUE)
get_filename_component(SUBSTRAIT_PROTO_DIR ${SUBSTRAIT_PROTO_SRC_DIR}/ DIRECTORY)

set(PROTO_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/")
set(GLUTEN_PROTO_SRC_DIR ${GLUTEN_HOME}/gluten-core/src/main/resources/org/apache/gluten/proto)
message(STATUS "Set Gluten Proto Directory in ${GLUTEN_PROTO_SRC_DIR}")
file(GLOB GLUTEN_PROTO_FILES ${GLUTEN_PROTO_SRC_DIR}/*.proto)
foreach(PROTO ${GLUTEN_PROTO_FILES})
    file(RELATIVE_PATH REL_PROTO ${GLUTEN_PROTO_SRC_DIR} ${PROTO})
    string(REGEX REPLACE "\\.proto" "" PROTO_NAME ${REL_PROTO})
    list(APPEND GLUTEN_PROTO_SRCS "${PROTO_OUTPUT_DIR}/${PROTO_NAME}.pb.cc")
    list(APPEND GLUTEN_PROTO_HDRS "${PROTO_OUTPUT_DIR}/${PROTO_NAME}.pb.h")
endforeach()
set(GLUTEN_PROTO_OUTPUT_FILES ${GLUTEN_PROTO_HDRS} ${GLUTEN_PROTO_SRCS})
set_source_files_properties(${GLUTEN_PROTO_OUTPUT_FILES} PROPERTIES GENERATED TRUE)
get_filename_component(GLUTEN_PROTO_DIR ${GLUTEN_PROTO_SRC_DIR}/ DIRECTORY)

add_custom_command(OUTPUT ${SUBSTRAIT_PROTO_OUTPUT_FILES}
        COMMAND ${PROTOC_BIN}
        --proto_path=${SUBSTRAIT_PROTO_SRC_DIR}
        --cpp_out=${SUBSTRAIT_PROTO_OUTPUT_DIR}
        ${SUBSTRAIT_PROTO_FILES}
        DEPENDS ${SUBSTRAIT_PROTO_FILES}
        COMMENT "Running Substrait PROTO compiler"
        VERBATIM)
add_custom_command(OUTPUT ${GLUTEN_PROTO_OUTPUT_FILES}
        COMMAND ${PROTOC_BIN}
        --proto_path ${GLUTEN_PROTO_SRC_DIR}/
        --cpp_out ${PROTO_OUTPUT_DIR}
        ${GLUTEN_PROTO_FILES}
        DEPENDS ${GLUTEN_PROTO_FILES}
        COMMENT "Running Gluten PROTO compiler"
        VERBATIM)
# OmniMemoryManager calls
# gluten::MemoryManager::{create,registerFactory,release}, defined in
# gluten/cpp/core/memory/MemoryManager.cc. The Omni backend never linked gluten
# core (unlike cpp/velox, which does target_link_libraries(velox PUBLIC gluten)),
# so those symbols were undefined at dlopen time and the plugin failed to load.
# MemoryManager.cc is self-contained (MemoryManager.h + header-only
# utils/Registry.h), so compile it straight into libspark.
set(GLUTEN_CORE_SRCS ${GLUTEN_HOME}/cpp/core/memory/MemoryManager.cc)
add_library (${PROJ_TARGET} SHARED ${SOURCE_FILES} ${GLUTEN_CORE_SRCS} ${PROTO_SRCS} ${PROTO_HDRS} ${SUBSTRAIT_PROTO_HDRS} ${SUBSTRAIT_PROTO_SRCS} ${GLUTEN_PROTO_HDRS} ${GLUTEN_PROTO_SRCS})

#JNI
target_include_directories(${PROJ_TARGET} PUBLIC $ENV{JAVA_HOME}/include)
target_include_directories(${PROJ_TARGET} PUBLIC $ENV{JAVA_HOME}/include/linux)
target_include_directories(${PROJ_TARGET} PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

option(BUILD_CPP_UDAF_EXAMPLES "Build sample libmyudaf/libmyextraudaf under cpp-omni" OFF)

# The new gluten/cpp-omni/src/memory/OmniMemoryManager.cc
# needs ${GLUTEN_HOME}/cpp/core and ${OMNI_HOME}/lib/include on its compile include
# path so it can find gluten-core memory headers (#include "memory/MemoryManager.h"
# etc.) and the omni per-task pool header (#include "memory/omni_task_memory_pool.h").
#
# CRITICAL: we DO NOT add cpp/core to the *target-wide* include path — doing so
# shadows OmniOperator's /opt/Adaptor/lib/include/compute/ResultIterator.h with
# gluten's cpp/core/compute/ResultIterator.h, breaking every other cpp-omni TU
# that references the unqualified ResultIterator (omniruntime::ResultIterator).
#
# Therefore: scope the include path to *just this one source file* via
# set_source_files_properties + INCLUDE_DIRECTORIES. A proper cross-target link
# contract (e.g. a gluten_core library that cpp-omni links against) would let
# this per-file scoping be retired.
get_filename_component(_OMNI_CENTRAL_ROOT ${GLUTEN_HOME} DIRECTORY)
set(_OMNI_CORE_INCLUDE ${_OMNI_CENTRAL_ROOT}/omni/core/src)
get_filename_component(_OMNI_CONTAINER_CORE_INCLUDE
    ${CMAKE_SOURCE_DIR}/../../OmniOperator/core/src ABSOLUTE)
if(EXISTS ${_OMNI_CONTAINER_CORE_INCLUDE})
    set(_OMNI_CORE_INCLUDE ${_OMNI_CONTAINER_CORE_INCLUDE})
endif()
set(_OMNI_SPLITTER_OPERATOR_INCLUDE ${_OMNI_CENTRAL_ROOT}/omni/core/src/operator)
get_filename_component(_OMNI_CONTAINER_SPLITTER_OPERATOR_INCLUDE
    ${CMAKE_SOURCE_DIR}/../../OmniOperator/core/src/operator ABSOLUTE)
if(EXISTS ${_OMNI_CONTAINER_SPLITTER_OPERATOR_INCLUDE})
    set(_OMNI_SPLITTER_OPERATOR_INCLUDE ${_OMNI_CONTAINER_SPLITTER_OPERATOR_INCLUDE})
endif()
set(_OMNI_PER_TASK_POOL_INCLUDES
    ${GLUTEN_HOME}/cpp/core
    ${_OMNI_CORE_INCLUDE}
    ${_OMNI_SPLITTER_OPERATOR_INCLUDE}
)
# Runtime.cpp / SparkJniWrapper.cpp need omni-core (for
# omni_task_memory_pool.h / RuntimeMemoryHelper.h) but NOT gluten/cpp/core
# (would transitively shadow compute/ResultIterator.h via OmniOperator's
# compute/task.h chain). Per-task memory is reached via the Velox-style const
# pool_ on each buffer, not thread-local state.
set(_OMNI_CORE_ONLY_INCLUDES
    ${_OMNI_CORE_INCLUDE}
)
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/memory/OmniMemoryManager.cc)
    set_source_files_properties(
        memory/OmniMemoryManager.cc
        PROPERTIES INCLUDE_DIRECTORIES "${_OMNI_PER_TASK_POOL_INCLUDES}"
    )
endif()
set_source_files_properties(
    compute/OmniBackend.cpp
    jni/SparkJniWrapperGlobalMemory.cpp
    PROPERTIES INCLUDE_DIRECTORIES "${_OMNI_PER_TASK_POOL_INCLUDES}"
)
set_source_files_properties(
    jni/SparkJniWrapper.cpp
    # Keep the search path narrow: Omni core has a different shuffle.pb.h and
    # adding its root here shadows Gluten's generated shuffle types.
    PROPERTIES INCLUDE_DIRECTORIES "${_OMNI_CORE_INCLUDE}/operator/join"
)

# SparkJniWrapperMemory.cpp holds the JNI entry points
# that touch gluten/cpp/core types (jniCastOrThrow<gluten::MemoryManager>,
# gluten::MemoryManager::create, SparkAllocationListener). It is split off from
# SparkJniWrapper.cpp specifically so the cpp/core include path does NOT shadow
# OmniOperator's compute/Runtime.h / compute/ResultIterator.h in SparkJniWrapper.cpp.
# Same per-file scoping rationale as OmniMemoryManager.cc above.
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/jni/SparkJniWrapperMemory.cpp)
    set_source_files_properties(
        jni/SparkJniWrapperMemory.cpp
        PROPERTIES INCLUDE_DIRECTORIES "${_OMNI_PER_TASK_POOL_INCLUDES}"
    )
endif()
# Register OmniSparkAllocationListener TU for cpp/core include scope
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/jni/OmniSparkAllocationListener.cpp)
    # Needs gluten/cpp/core for memory/AllocationListener.h (per-file scope avoids
    # the compute/ResultIterator.h shadowing described above).
    set_source_files_properties(
        jni/OmniSparkAllocationListener.cpp
        PROPERTIES INCLUDE_DIRECTORIES "${_OMNI_PER_TASK_POOL_INCLUDES}"
    )
endif()
# Register OmniBlockAllocationListener TU for cpp/core include scope
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/jni/OmniBlockAllocationListener.cpp)
    # Needs gluten/cpp/core for memory/AllocationListener.h (per-file scope avoids
    # the compute/ResultIterator.h shadowing described above).
    set_source_files_properties(
        jni/OmniBlockAllocationListener.cpp
        PROPERTIES INCLUDE_DIRECTORIES "${_OMNI_PER_TASK_POOL_INCLUDES}"
    )
endif()
# The compiled-in gluten-core MemoryManager.cc needs
# gluten/cpp/core on its include path (#include "utils/Registry.h"). Per-file
# scope only — avoids the cpp/core target-wide compute/ResultIterator.h shadow trap.
set_source_files_properties(
    ${GLUTEN_HOME}/cpp/core/memory/MemoryManager.cc
    PROPERTIES INCLUDE_DIRECTORIES "${GLUTEN_HOME}/cpp/core"
)
# compute/Runtime.cpp needs omni-core (for memory/omni_task_memory_pool.h and
# RuntimeMemoryHelper.h) but not gluten/cpp/core: the task-pool lookup is a free
# function declared in memory/RuntimeMemoryHelper.h and defined in
# OmniMemoryManager.cc.
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/compute/Runtime.cpp)
    set_source_files_properties(
        compute/Runtime.cpp
        PROPERTIES INCLUDE_DIRECTORIES "${_OMNI_CORE_ONLY_INCLUDES}"
    )
endif()
# compute/WholeStageResultIterator.cpp includes omni-core headers for the
# OmniTaskMemoryPool accessor while avoiding the compute/ResultIterator.h
# shadowing from gluten/cpp/core, hence the per-file include scope.
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/compute/WholeStageResultIterator.cpp)
    set_source_files_properties(
        compute/WholeStageResultIterator.cpp
        PROPERTIES INCLUDE_DIRECTORIES "${_OMNI_CORE_ONLY_INCLUDES}"
    )
endif()
# Splitter's row-mode safe boundary uses only the standalone reclaim_context.h.
# Scope its directory to this TU so source-tree vector headers do not shadow the
# installed Omni ABI headers used by cpp-omni.
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/shuffle/splitter.cpp)
    set_source_files_properties(
        shuffle/splitter.cpp
        PROPERTIES INCLUDE_DIRECTORIES "${_OMNI_SPLITTER_OPERATOR_INCLUDE}"
    )
endif()
# The OmniMemoryManager downcast that GetTaskMemoryPool() needs lives inside
# OmniMemoryManager.cc as a free function (omniruntime::ToOmniTaskPoolOrNull);
# see memory/RuntimeMemoryHelper.h. OmniMemoryManager.cc already has
# _OMNI_PER_TASK_POOL_INCLUDES applied above.

add_subdirectory(udf/examples)

target_link_libraries (${PROJ_TARGET} PUBLIC
        crypto
        sasl2
        protobuf
        z
        # omni libboostkit-omniop-reader needs zlib-ng (zng_* symbols).
        # In a standalone Omni run libz-ng arrives via java-binding.so; gluten loads
        # only libspark, so libspark must pull libz-ng itself or reader.so fails dlopen.
        z-ng
        snappy
        lz4
        zstd
        re2
        fmt::fmt
        ${ARROW_LINK_LIBRARIES}
        boostkit-omniop-vector-2.2.0-aarch64
        boostkit-omniop-operator-2.2.0-aarch64
        boostkit-omniop-codegen-2.2.0-aarch64
        boostkit-omniop-reader-2.2.0-aarch64
        )

set_target_properties(${PROJ_TARGET} PROPERTIES
                      LIBRARY_OUTPUT_DIRECTORY ${root_directory}/releases
)

install(TARGETS ${PROJ_TARGET} DESTINATION lib)
