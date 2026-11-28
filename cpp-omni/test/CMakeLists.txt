aux_source_directory(${CMAKE_CURRENT_LIST_DIR} TEST_ROOT_SRCS)

# Find Apache Arrow include path for test sources (including shuffle/utils/benchmark subdirectories).
# src/CMakeLists.txt already finds Arrow, but its include_directories() is
# directory-scoped and does not propagate to this sibling test/ directory.

# First check if Arrow was already found by src/CMakeLists.txt
if(ARROW_INCLUDE_DIR)
    message(STATUS "Reusing Arrow include dir from src build: ${ARROW_INCLUDE_DIR}")
    set(ARROW_TEST_INCLUDE_DIR ${ARROW_INCLUDE_DIR})
elseif(ARROW_PC_INCLUDE_DIRS)
    message(STATUS "Reusing Arrow include dir from pkg-config: ${ARROW_PC_INCLUDE_DIRS}")
    set(ARROW_TEST_INCLUDE_DIR ${ARROW_PC_INCLUDE_DIRS})
else()
    # Need to find Arrow independently
    find_package(Arrow QUIET)
    if(Arrow_FOUND)
        message(STATUS "Arrow found for tests via CMake config: ${ARROW_INCLUDE_DIR}")
        set(ARROW_TEST_INCLUDE_DIR ${ARROW_INCLUDE_DIR})
    else()
        find_package(PkgConfig QUIET)
        if(PKG_CONFIG_FOUND)
            pkg_check_modules(ARROW_PC QUIET arrow)
        endif()
        if(ARROW_PC_FOUND)
            message(STATUS "Arrow found for tests via pkg-config: ${ARROW_PC_INCLUDE_DIRS}")
            set(ARROW_TEST_INCLUDE_DIR ${ARROW_PC_INCLUDE_DIRS})
        else()
            find_path(ARROW_TEST_INCLUDE_DIR arrow/api.h
                PATHS /usr/local/include /usr/include
                HINTS ${ARROW_HOME}/include ENV ARROW_HOME)
            if(ARROW_TEST_INCLUDE_DIR)
                message(STATUS "Arrow found for tests via manual search: ${ARROW_TEST_INCLUDE_DIR}")
            else()
                message(WARNING "Apache Arrow headers not found for tests. "
                    "Set ARROW_HOME or install Arrow to fix this.")
            endif()
        endif()
    endif()
endif()

# Propagate Arrow include path to all test subdirectories (shuffle, utils, benchmark)
# so that static library targets like shuffletest can find arrow/version.h etc.
if(ARROW_TEST_INCLUDE_DIR)
    include_directories(${ARROW_TEST_INCLUDE_DIR})
endif()

add_subdirectory(shuffle)
add_subdirectory(utils)
add_subdirectory(benchmark)
add_subdirectory(memory)        # Phase 2 Plan 2.1 — OmniSparkAllocationListener tests

# configure
set(TP_TEST_TARGET tptest)
set(MY_LINK
        shuffletest
        utilstest
        benchmark_test
        memorytest              # Phase 2 Plan 2.1
        )

# find gtest package
find_package(GTest REQUIRED)

# `memorytest` contains an Invocation API test. Because it is linked into
# tptest through --whole-archive, link libjvm on the final executable as well;
# relying on a static library's transitive link interface leaves
# JNI_CreateJavaVM unresolved on OpenEuler/JDK8.
find_library(TP_JVM_LIB jvm PATHS
        $ENV{JAVA_HOME}/lib/server
        $ENV{JAVA_HOME}/jre/lib/${CMAKE_SYSTEM_PROCESSOR}/server
        $ENV{JAVA_HOME}/lib/${CMAKE_SYSTEM_PROCESSOR}/server
        NO_DEFAULT_PATH)

# compile a executable file
add_executable(${TP_TEST_TARGET} ${ROOT_SRCS} ${TEST_ROOT_SRCS})

# dependent libraries
target_link_libraries(${TP_TEST_TARGET}
        ${GTEST_BOTH_LIBRARIES}
        ${SOURCE_LINK}
        -Wl,--whole-archive
        ${MY_LINK}
        -Wl,--no-whole-archive
        gtest
        pthread
        stdc++
        dl
        arrow
        boostkit-omniop-vector-2.2.0-aarch64
        boundscheck
        LLVM-15
        ${TP_JVM_LIB}
        spark_columnar_plugin)

target_compile_options(${TP_TEST_TARGET} PUBLIC -g -O2 -fPIC)

# dependent include (Arrow is already propagated via directory-level include_directories above)
target_include_directories(${TP_TEST_TARGET} PRIVATE ${GTEST_INCLUDE_DIRS})

# discover tests
gtest_discover_tests(${TP_TEST_TARGET})
