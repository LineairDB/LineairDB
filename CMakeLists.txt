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

cmake_minimum_required(VERSION 3.10)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")
project(lineairdb
  VERSION 0.1.0
  DESCRIPTION "Transactional key-value storage with linearizability and linear-scale performance."
  LANGUAGES CXX)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_SOURCE_DIR}/cmake/modules")

### Options
option(BUILD_TESTS "Build testing executables" ON)
option(BUILD_BENCHMARKS "Build benchmarking executables" ON)
option(BUILD_SANITIZER "Build with clang's address sanitizer" ON)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

### Compiler Options
set(CMAKE_C_FLAGS "--std=c99")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -mcx16 -fPIC -march=native")

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS} -g -O0 --save-temps -fno-omit-frame-pointer")
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fcolor-diagnostics")
  if (BUILD_SANITIZER)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address")
  endif()
endif()
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_DEBUG} -Ofast")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS} -march=native -Ofast -ffast-math")


### Build as library
file(GLOB_RECURSE SOURCES "src/*.cpp")
add_library(${PROJECT_NAME} ${SOURCES})

find_package(Boost REQUIRED)

set(JEMALLOC_USE_STATIC ON)
find_package(jemalloc)
if(JEMALLOC_FOUND)
  if(NOT APPLE) # WANTFIX: it seems that googletest's TYPED_TEST,
                # that are built with static linking of jemalloc,
                # always result in SIGSEGV on OSX.
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
    target_link_libraries(${PROJECT_NAME} stdc++ stdc++fs atomic pthread)
  endif()
endif()

target_include_directories(${PROJECT_NAME} PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)
target_include_directories(${PROJECT_NAME} PRIVATE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
)

### Third-party libraries
target_include_directories(${PROJECT_NAME} PRIVATE
  ${Boost_INCLUDE_DIRS}
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/third_party/rapidjson/include>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/third_party/libcuckoo>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/third_party/concurrentqueue>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/third_party/spdlog/include>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/third_party/msgpack/include>
)

install(
  TARGETS ${PROJECT_NAME}
  EXPORT ${PROJECT_NAME}-config
  DESTINATION lib
  )
install(
  EXPORT ${PROJECT_NAME}-config
  NAMESPACE ${PROJECT_NAME}::
  DESTINATION lib/cmake/${PROJECT_NAME}
)
install(
  DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/${PROJECT_NAME}
  DESTINATION include
)

### Tests
if (BUILD_TESTS)
  enable_testing()
  add_subdirectory(third_party/googletest)
  add_subdirectory(tests)
endif()

### Benchmarks
if (BUILD_BENCHMARKS)
  enable_testing()
  add_subdirectory(bench)
endif()

### Documents
find_package(Doxygen)
if (DOXYGEN_FOUND)
  if (CMAKE_BUILD_TYPE MATCHES Debug)
    set(doxyfile ${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.debug)
  else()
    set(doxyfile ${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile)
  endif()
  add_custom_target("doc" ${DOXYGEN_EXECUTABLE} ${doxyfile} 
    DEPENDS ${doxyfile}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
endif()
