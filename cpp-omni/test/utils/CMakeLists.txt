aux_source_directory(${CMAKE_CURRENT_LIST_DIR} UTILS_TESTS_LIST)
set(UTILS_TEST_TARGET utilstest)
add_library(${UTILS_TEST_TARGET} ${UTILS_TESTS_LIST})
target_include_directories(${UTILS_TEST_TARGET} PUBLIC ${CMAKE_BINARY_DIR}/src)
target_include_directories(${UTILS_TEST_TARGET} PUBLIC $ENV{JAVA_HOME}/include)
target_include_directories(${UTILS_TEST_TARGET} PUBLIC $ENV{JAVA_HOME}/include/linux)

# test_utils.cpp constructs the same task-bound pool used by JNI splitter
# creation. Scope Omni core to this source so Gluten's similarly named compute
# headers cannot shadow the installed Omni interfaces for the whole target.
get_filename_component(_OMNI_TEST_CORE_INCLUDE
    ${CMAKE_SOURCE_DIR}/../../OmniOperator/core/src ABSOLUTE)
set_source_files_properties(
    test_utils.cpp
    PROPERTIES INCLUDE_DIRECTORIES "${_OMNI_TEST_CORE_INCLUDE}"
)
