#
#   Copyright (C) 2020 Nippon Telegraph and Telephone Corporation.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

macro(register_test testfile testfilepath)
  add_executable(${testfile} ${CMAKE_CURRENT_LIST_DIR}/${testfilepath})
  target_link_libraries(${testfile} gtest_main gtest ${PROJECT_NAME})
  target_include_directories(${testfile} PRIVATE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/third_party/concurrentqueue>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/third_party/spdlog/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/third_party/msgpack/include>
  )

  # Add compiler definitions and linker options for Apple Silicon compatibility
  if(APPLE AND CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
    target_compile_definitions(${testfile} PRIVATE GTEST_HAS_STD_WSTRING=0)
    target_link_options(${testfile} PRIVATE -Wl,-undefined,dynamic_lookup)
  endif()

  add_test(
    NAME ${testfile}
    COMMAND ${CMAKE_CURRENT_BINARY_DIR}/${testfile})
endmacro()

file(GLOB_RECURSE tests RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*_test.cpp")
foreach(test ${tests})
    message(STATUS "Test executable is found: ${test}")
    get_filename_component(testname ${test} NAME_WE)
    register_test(${testname} ${test})
endforeach(test)
