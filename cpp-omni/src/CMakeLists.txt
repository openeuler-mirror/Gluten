include_directories(SYSTEM "/user/local/include")

set (PROJ_TARGET spark_columnar_plugin)

file(GLOB_RECURSE SOURCE_FILES "*.cpp" "*.cc")
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
        COMMENT "Running Substrait PROTO compiler"
        VERBATIM)
add_custom_command(OUTPUT ${GLUTEN_PROTO_OUTPUT_FILES}
        COMMAND ${PROTOC_BIN}
        --proto_path ${GLUTEN_PROTO_SRC_DIR}/
        --cpp_out ${PROTO_OUTPUT_DIR}
        ${GLUTEN_PROTO_FILES}
        COMMENT "Running Gluten PROTO compiler"
        VERBATIM)
add_library (${PROJ_TARGET} SHARED ${SOURCE_FILES} ${PROTO_SRCS} ${PROTO_HDRS} ${SUBSTRAIT_PROTO_HDRS} ${SUBSTRAIT_PROTO_SRCS} ${GLUTEN_PROTO_HDRS} ${GLUTEN_PROTO_SRCS})

#JNI
target_include_directories(${PROJ_TARGET} PUBLIC $ENV{JAVA_HOME}/include)
target_include_directories(${PROJ_TARGET} PUBLIC $ENV{JAVA_HOME}/include/linux)
target_include_directories(${PROJ_TARGET} PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

option(BUILD_CPP_UDAF_EXAMPLES "Build sample libmyudaf/libmyextraudaf under cpp-omni" OFF)
add_subdirectory(udf/examples)

target_link_libraries (${PROJ_TARGET} PUBLIC
        crypto
        sasl2
        protobuf
        z
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
