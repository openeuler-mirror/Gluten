include_directories(SYSTEM "/user/local/include")

set (PROJ_TARGET spark_columnar_plugin)

find_package(fmt REQUIRED)
file(GLOB_RECURSE SOURCE_FILES "*.cpp" "*.cc")

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

# generate vecdata proto
protobuf_generate_cpp(PROTO_SRCS_VB PROTO_HDRS_VB proto/vec_data.proto)

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
add_custom_command(OUTPUT ${SUBSTRAIT_PROTO_OUTPUT_FILES}
        COMMAND ${PROTOC_BIN}
        --proto_path=${SUBSTRAIT_PROTO_SRC_DIR}
        --cpp_out=${SUBSTRAIT_PROTO_OUTPUT_DIR}
        ${SUBSTRAIT_PROTO_FILES}
        COMMENT "Running Substrait PROTO compiler"
        VERBATIM)
add_library (${PROJ_TARGET} SHARED ${SOURCE_FILES} ${PROTO_SRCS} ${PROTO_HDRS} ${PROTO_SRCS_VB} ${PROTO_HDRS_VB} ${SUBSTRAIT_PROTO_HDRS} ${SUBSTRAIT_PROTO_SRCS})

#JNI
target_include_directories(${PROJ_TARGET} PUBLIC $ENV{JAVA_HOME}/include)
target_include_directories(${PROJ_TARGET} PUBLIC $ENV{JAVA_HOME}/include/linux)
target_include_directories(${PROJ_TARGET} PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries (${PROJ_TARGET} PUBLIC
        crypto
        sasl2
        protobuf
        z
        snappy
        lz4
        zstd
        fmt::fmt
        boostkit-omniop-vector-1.8.0-aarch64
        boostkit-omniop-operator-1.8.0-aarch64
        boostkit-omniop-codegen-1.8.0-aarch64
        )

set_target_properties(${PROJ_TARGET} PROPERTIES
                      LIBRARY_OUTPUT_DIRECTORY ${root_directory}/releases
)

install(TARGETS ${PROJ_TARGET} DESTINATION lib)
