#
# Copyright (C) 2020 Nippon Telegraph and Telephone Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

cmake_minimum_required(VERSION 3.10)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")
project(
  lineairdb
  VERSION 0.1.0
  DESCRIPTION
    "Transactional key-value storage with linearizability and linear-scale performance."
  LANGUAGES CXX)

# Build Options
option(BUILD_TESTS "Build testing executables" ON)
option(BUILD_BENCHMARKS "Build benchmarking executables" ON)
option(BUILD_SANITIZER "Build with clang's address sanitizer" ON)

# Build Parameters
if (DEFINED PREFETCH_LOCALITY)
  add_compile_definitions(PREFETCH_LOCALITY=${PREFETCH_LOCALITY})
endif()

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  set(IS_TOPLEVEL_PROJECT TRUE)
endif()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
                      "${PROJECT_SOURCE_DIR}/cmake/modules")
set(CMAKE_INCLUDE_CURRENT_DIR_IN_INTERFACE ON)
add_compile_definitions(_LIBCPP_NO_EXPERIMENTAL_DEPRECATION_WARNING_FILESYSTEM)

# Compiler Options
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_C_FLAGS "--std=c99")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -fPIC")
if (NOT APPLE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mcx16")
endif()
if(NOT IS_TOPLEVEL_PROJECT)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ftls-model=local-dynamic")
endif()

set(CMAKE_CXX_FLAGS_DEBUG
    "${CMAKE_CXX_FLAGS} -g -O0 --save-temps -fno-omit-frame-pointer")
if(CMAKE_CXX_COMPILER_ID MATCHES "^(Apple)?Clang$")
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fcolor-diagnostics")
  if(IS_TOPLEVEL_PROJECT AND BUILD_SANITIZER)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address")
  endif()
endif()
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_DEBUG} -Ofast")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS} -Ofast -ffast-math -DNDEBUG")
if(APPLE AND CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
  # Apple Sillicon (m1) does not support `march=native`. use `mcpu` instead.
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS} -mcpu=apple-m1")
else()
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS} -march=native")
endif()

# Build as library
file(GLOB_RECURSE SOURCES "src/*.cpp")
add_library(${PROJECT_NAME} ${SOURCES})
add_library(${PROJECT_NAME}::${PROJECT_NAME} ALIAS ${PROJECT_NAME})

find_package(Boost QUIET)
if(Boost_FOUND)
  include_directories(PRIVATE
    ${Boost_INCLUDE_DIRS}
  )
else()
  add_compile_definitions(MSGPACK_NO_BOOST=ON)
endif()

find_package(jemalloc QUIET)
if(JEMALLOC_FOUND)
  if(NOT APPLE) # WANTFIX: it seems that googletest's TYPED_TEST, that are built
                # with static linking of jemalloc, always result in SIGSEGV on
                # OSX.
    message(STATUS "JEMALLOC ${JEMALLOC_LIBRARIES}")
    target_link_libraries(${PROJECT_NAME} ${JEMALLOC_LIBRARIES})
  endif()
else()
  message("warning: jemalloc is not found in this environment.")
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
  target_link_libraries(${PROJECT_NAME} pthread)
else()
  if(APPLE) # Use libc++
    target_link_libraries(${PROJECT_NAME} c++ stdc++fs atomic pthread)
  else() # Use libstdc++
    target_link_libraries(${PROJECT_NAME} stdc++ stdc++fs atomic pthread numa)
  endif()
endif()

target_include_directories(
  ${PROJECT_NAME} PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                         $<INSTALL_INTERFACE:include>)
target_include_directories(
  ${PROJECT_NAME} PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>)

# Third-party libraries
target_include_directories(
  ${PROJECT_NAME} SYSTEM
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/third_party/rapidjson/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/third_party/concurrentqueue>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/third_party/spdlog/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/third_party/msgpack/include>)

# Export as cmake library
file(GLOB_RECURSE PUBLIC_HEADERS include/${PROJECT_NAME}/*.h)
message(STATUS "${PUBLIC_HEADERS}")
set_target_properties(${PROJECT_NAME} PROPERTIES PUBLIC_HEADER
                                                 "${PUBLIC_HEADERS}")
install(
  TARGETS ${PROJECT_NAME}
  EXPORT ${PROJECT_NAME}-config
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  INCLUDES
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
  PUBLIC_HEADER DESTINATION include/${PROJECT_NAME})
install(
  EXPORT ${PROJECT_NAME}-config
  NAMESPACE ${PROJECT_NAME}::
  DESTINATION lib/cmake/${PROJECT_NAME}
  EXPORT_LINK_INTERFACE_LIBRARIES)

# Tests
if(IS_TOPLEVEL_PROJECT AND BUILD_TESTS)
  enable_testing()
  if(NOT TARGET gtest)
    add_subdirectory(third_party/googletest)
  endif()
  add_subdirectory(tests)
endif()

# Benchmarks
if(IS_TOPLEVEL_PROJECT AND BUILD_BENCHMARKS)
  enable_testing()
  add_subdirectory(bench)
endif()

# Documents
find_package(Doxygen)
if(DOXYGEN_FOUND)
  if(CMAKE_BUILD_TYPE MATCHES Debug)
    set(doxyfile ${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.debug)
  else()
    set(doxyfile ${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile)
  endif()
  add_custom_target(
    "doc"
    ${DOXYGEN_EXECUTABLE} ${doxyfile}
    DEPENDS ${doxyfile}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
endif()
