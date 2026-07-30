# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(_omni_boostkit_libs
        boostkit-omniop-codegen-2.2.0-aarch64
        boostkit-omniop-vector-2.2.0-aarch64
        boostkit-omniop-operator-2.2.0-aarch64)

add_library(myudf SHARED "MyUDF.cpp")
target_link_libraries(myudf ${_omni_boostkit_libs})

if(BUILD_CPP_UDAF_EXAMPLES)
    if(DEFINED ENV{OMNI_OPERATOR_HOME})
        set(OMNI_OPERATOR_HOME "$ENV{OMNI_OPERATOR_HOME}")
    else()
        get_filename_component(OMNI_OPERATOR_HOME "${GLUTEN_HOME}/../OmniOperator" ABSOLUTE)
    endif()
    set(OMNI_OPERATOR_INCLUDE "${OMNI_OPERATOR_HOME}/core/src")

    if(NOT EXISTS "${OMNI_OPERATOR_INCLUDE}/operator/aggregation/aggregator/typed_aggregator.h")
        message(FATAL_ERROR
                "UDAF examples require OmniOperator headers. Set OMNI_OPERATOR_HOME to the OmniOperator repo root "
                "(directory containing core/src/operator/aggregation/aggregator/typed_aggregator.h). "
                "Current OMNI_OPERATOR_HOME=${OMNI_OPERATOR_HOME}")
    endif()

    add_library(myudaf SHARED "MyUDAF.cpp")
    target_link_libraries(myudaf PUBLIC ${_omni_boostkit_libs})
    target_include_directories(myudaf SYSTEM PRIVATE "${OMNI_OPERATOR_INCLUDE}")
    target_compile_options(myudaf PRIVATE -Wno-error -fPIC -fpermissive)

    add_library(myextraudaf SHARED "MyExtraUDAF.cpp")
    target_link_libraries(myextraudaf PUBLIC ${_omni_boostkit_libs})
    target_include_directories(myextraudaf SYSTEM PRIVATE "${OMNI_OPERATOR_INCLUDE}")
    target_compile_options(myextraudaf PRIVATE -Wno-error -fPIC -fpermissive)

    if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64|ARM64")
        target_compile_options(myudaf PRIVATE -march=armv8-a+crc)
        target_compile_options(myextraudaf PRIVATE -march=armv8-a+crc)
    endif()

    if(DEFINED ENV{OMNI_HOME})
        target_include_directories(myudaf SYSTEM PRIVATE "$ENV{OMNI_HOME}/lib/include")
        target_include_directories(myextraudaf SYSTEM PRIVATE "$ENV{OMNI_HOME}/lib/include")
    endif()

    set_target_properties(myudaf myextraudaf PROPERTIES
            LIBRARY_OUTPUT_DIRECTORY ${root_directory}/releases)

    install(TARGETS myudaf myextraudaf DESTINATION lib)
    message(STATUS "myudaf/myextraudaf: OmniOperator headers from ${OMNI_OPERATOR_INCLUDE}")
endif()
