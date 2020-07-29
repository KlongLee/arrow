# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

if(ARROW_UTF8PROC_USE_SHARED)
  set(UTF8PROC_LIB_NAMES)
  if(CMAKE_IMPORT_LIBRARY_SUFFIX)
    list(APPEND UTF8PROC_LIB_NAMES
                "${CMAKE_IMPORT_LIBRARY_PREFIX}utf8proc${CMAKE_IMPORT_LIBRARY_SUFFIX}")
  endif()
  list(APPEND UTF8PROC_LIB_NAMES
              "${CMAKE_SHARED_LIBRARY_PREFIX}utf8proc${CMAKE_SHARED_LIBRARY_SUFFIX}")
else()
  if(MSVC)
    set(UTF8PROC_COMPILER_DEFINITIONS UTF8PROC_STATIC)
  endif()
  if(MSVC AND NOT DEFINED UTF8PROC_MSVC_STATIC_LIB_SUFFIX)
    set(UTF8PROC_MSVC_STATIC_LIB_SUFFIX "_static")
  endif()
  set(UTF8PROC_STATIC_LIB_SUFFIX
      "${UTF8PROC_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(UTF8PROC_STATIC_LIB_NAME ${CMAKE_STATIC_LIBRARY_PREFIX}utf8proc${UTF8PROC_STATIC_LIB_SUFFIX})
  set(UTF8PROC_LIB_NAMES "${UTF8PROC_STATIC_LIB_NAME}" "lib${UTF8PROC_STATIC_LIB_NAME}")
endif()

if(utf8proc_ROOT)
  find_library(
    UTF8PROC_LIB
    NAMES ${UTF8PROC_LIB_NAMES}
    PATHS ${utf8proc_ROOT}
    PATH_SUFFIXES ${LIB_PATH_SUFFIXES}
    NO_DEFAULT_PATH)
  find_path(UTF8PROC_INCLUDE_DIR
            NAMES utf8proc.h
            PATHS ${utf8proc_ROOT}
            NO_DEFAULT_PATH
            PATH_SUFFIXES ${INCLUDE_PATH_SUFFIXES})
else()
  find_library(
    UTF8PROC_LIB
    NAMES ${UTF8PROC_LIB_NAMES}
    PATH_SUFFIXES ${LIB_PATH_SUFFIXES})
  find_path(UTF8PROC_INCLUDE_DIR NAMES utf8proc.h PATH_SUFFIXES ${INCLUDE_PATH_SUFFIXES})
endif()

find_package_handle_standard_args(utf8proc REQUIRED_VARS UTF8PROC_LIB UTF8PROC_INCLUDE_DIR)

# CMake 3.2 does uppercase the FOUND variable
if(UTF8PROC_FOUND OR utf8proc_FOUND)
  set(utf8proc_FOUND TRUE)
  add_library(utf8proc::utf8proc UNKNOWN IMPORTED)
  set_target_properties(utf8proc::utf8proc
                        PROPERTIES IMPORTED_LOCATION "${UTF8PROC_LIB}"
                                   INTERFACE_COMPILE_DEFINITIONS "${UTF8PROC_COMPILER_DEFINITIONS}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${UTF8PROC_INCLUDE_DIR}")
endif()

