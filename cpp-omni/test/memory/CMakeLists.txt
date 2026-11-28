aux_source_directory(${CMAKE_CURRENT_LIST_DIR} MEMORY_TESTS_LIST)
set(MEMORY_TEST_TARGET memorytest)
add_library(${MEMORY_TEST_TARGET} STATIC ${MEMORY_TESTS_LIST})
target_compile_options(${MEMORY_TEST_TARGET} PUBLIC )
target_link_libraries(${MEMORY_TEST_TARGET} utilstest)
target_include_directories(${MEMORY_TEST_TARGET} PUBLIC ${CMAKE_BINARY_DIR}/src)
target_include_directories(${MEMORY_TEST_TARGET} PUBLIC $ENV{JAVA_HOME}/include)
target_include_directories(${MEMORY_TEST_TARGET} PUBLIC $ENV{JAVA_HOME}/include/linux)
# Tests need gluten/cpp/core for memory/AllocationListener.h
# (mock listener subclass + OmniSparkAllocationListener.h -> AllocationListener.h)
target_include_directories(${MEMORY_TEST_TARGET} PUBLIC ${GLUTEN_HOME}/cpp/core)

# JniGlobalRef test needs JNI Invocation API (JNI_CreateJavaVM)
# libjvm.so ships at $JAVA_HOME/lib/server/libjvm.so.
# If not found, tests will fall back to GTEST_SKIP() at runtime (see jni_global_ref_test.cc).
find_library(JVM_LIB jvm PATHS
    $ENV{JAVA_HOME}/lib/server
    $ENV{JAVA_HOME}/jre/lib/${CMAKE_SYSTEM_PROCESSOR}/server
    $ENV{JAVA_HOME}/lib/${CMAKE_SYSTEM_PROCESSOR}/server
    NO_DEFAULT_PATH)
if(JVM_LIB)
    target_link_libraries(${MEMORY_TEST_TARGET} ${JVM_LIB})
else()
    message(WARNING "libjvm.so not found at $ENV{JAVA_HOME}/lib/server — JniGlobalRef tests will GTEST_SKIP if JNI_CreateJavaVM is unavailable")
endif()
