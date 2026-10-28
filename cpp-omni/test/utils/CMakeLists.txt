aux_source_directory(${CMAKE_CURRENT_LIST_DIR} UTILS_TESTS_LIST)
set(UTILS_TEST_TARGET utilstest)
add_library(${UTILS_TEST_TARGET} ${UTILS_TESTS_LIST})
target_include_directories(${UTILS_TEST_TARGET} PUBLIC ${CMAKE_BINARY_DIR}/src)
target_include_directories(${UTILS_TEST_TARGET} PUBLIC $ENV{JAVA_HOME}/include)
target_include_directories(${UTILS_TEST_TARGET} PUBLIC $ENV{JAVA_HOME}/include/linux)

# test_utils.cpp gets memory/omni_task_memory_pool.h from the installed Omni
# include tree (CPLUS_INCLUDE_PATH), like the plugin sources.
