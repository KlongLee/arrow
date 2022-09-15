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

if(OpenSSLAlt_FOUND)
  return()
endif()

if(APPLE AND NOT OPENSSL_ROOT_DIR)
  find_program(BREW brew)
  if(BREW)
    execute_process(COMMAND ${BREW} --prefix "openssl@1.1"
                    OUTPUT_VARIABLE OPENSSL11_BREW_PREFIX
                    OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(OPENSSL11_BREW_PREFIX)
      set(OPENSSL_ROOT_DIR ${OPENSSL11_BREW_PREFIX})
    else()
      execute_process(COMMAND ${BREW} --prefix "openssl"
                      OUTPUT_VARIABLE OPENSSL_BREW_PREFIX
                      OUTPUT_STRIP_TRAILING_WHITESPACE)
      if(OPENSSL_BREW_PREFIX)
        set(OPENSSL_ROOT_DIR ${OPENSSL_BREW_PREFIX})
      endif()
    endif()
  endif()
endif()

set(find_package_args)
if(OpenSSLAlt_FIND_VERSION)
  list(APPEND find_package_args ${OpenSSLAlt_FIND_VERSION})
endif()
if(OpenSSLAlt_FIND_QUIETLY)
  list(APPEND find_package_args QUIET)
endif()
if(ARROW_OPENSSL_USE_SHARED)
  set(OPENSSL_USE_STATIC_LIBS OFF)
else()
  set(OPENSSL_USE_STATIC_LIBS ON)
endif()
find_package(OpenSSL ${find_package_args})

set(OpenSSLAlt_FOUND ${OPENSSL_FOUND})
