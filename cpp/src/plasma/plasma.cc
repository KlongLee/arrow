// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "plasma/plasma.h"

#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "plasma/common.h"
#include "plasma/protocol.h"

namespace plasma {

extern "C" {
void dlfree(void* mem);
}

ObjectTableEntry::ObjectTableEntry() : pointer(nullptr), ref_count(0) {}

ObjectTableEntry::~ObjectTableEntry() {
  dlfree(pointer);
  pointer = nullptr;
}

int warn_if_sigpipe(int status, int client_sock) {
  if (status >= 0) {
    return 0;
  }
  if (errno == EPIPE || errno == EBADF || errno == ECONNRESET) {
    ARROW_LOG(WARNING) << "Received SIGPIPE, BAD FILE DESCRIPTOR, or ECONNRESET when "
                          "sending a message to client on fd "
                       << client_sock
                       << ". The client on the other end may "
                          "have hung up.";
    return errno;
  }
  ARROW_LOG(FATAL) << "Failed to write message to client on fd " << client_sock << ".";
  return -1;  // This is never reached.
}

std::shared_ptr<std::string> create_object_info_buffer(
      ObjectInfoT* object_info) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreateObjectInfo(fbb, object_info);
  fbb.Finish(message);
  std::string notification;
  notification.resize(sizeof(int64_t) + fbb.GetSize());
  *(reinterpret_cast<int64_t*>(&notification[0])) = fbb.GetSize();
  memcpy(&notification[0] + sizeof(int64_t), fbb.GetBufferPointer(), fbb.GetSize());
  return std::make_shared<std::string>(std::move(notification));
}

ObjectTableEntry* get_object_table_entry(PlasmaStoreInfo* store_info,
                                         const ObjectID& object_id) {
  auto it = store_info->objects.find(object_id);
  if (it == store_info->objects.end()) {
    return NULL;
  }
  return it->second.get();
}

}  // namespace plasma
