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

project(lineairdb-example)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if (CMAKE_CXX_COMPILER_ID MATCHES "^(Apple)?Clang$")
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fcolor-diagnostics -fsanitize=address")
endif()

find_package(lineairdb REQUIRED)
if (NOT lineairdb_FOUND)
    message(FATAL_ERROR
        "LineairDB is not installed. Use 'make; make install' in the project root directory to install the header & libraries.")
endif()

add_executable(${PROJECT_NAME} example.cpp)

target_link_libraries(${PROJECT_NAME} lineairdb::lineairdb) 
