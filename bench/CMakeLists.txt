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

# ycsb
file(GLOB_RECURSE SOURCES "ycsb/*.cpp")
add_executable(ycsb ${SOURCES})
target_compile_features(ycsb
  PUBLIC
    cxx_std_17
    cxx_return_type_deduction
    cxx_rvalue_references)
target_link_libraries(ycsb ${PROJECT_NAME})
target_include_directories(ycsb PRIVATE
  $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/third_party/cxxopts/include>
  $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/third_party/rapidjson/include>
  $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/third_party/spdlog/include>
  $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/third_party/concurrentqueue>
  $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/third_party/msgpack/include>
  $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src>
)

# microbenchmarks
file(GLOB DIRS "microbench/*")
foreach(DIRNAME ${DIRS})
  file(GLOB SOURCES "${DIRNAME}/*.cpp")
  get_filename_component(EXNAME ${DIRNAME} NAME_WE)
  add_executable(${EXNAME} ${SOURCES})
  message(STATUS "Microbenchmark executable is found: ${EXNAME}")
  target_link_libraries(${EXNAME} ${PROJECT_NAME} pthread)
  target_compile_features(${EXNAME}
    PUBLIC
      cxx_std_17
      cxx_return_type_deduction
      cxx_rvalue_references)
  target_include_directories(${EXNAME} PRIVATE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/third_party/cxxopts/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/third_party/rapidjson/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/third_party/spdlog/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/third_party/concurrentqueue>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/third_party/msgpack/include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src>
  )

endforeach()

