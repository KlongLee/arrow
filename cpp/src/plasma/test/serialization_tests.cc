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

#include <sys/types.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include "plasma/common.h"
#include "plasma/io.h"
#include "plasma/plasma.h"
#include "plasma/protocol.h"
#include "plasma/test-util.h"

namespace fb = plasma::flatbuf;

namespace plasma {

/**
 * Create a temporary file. Needs to be closed by the caller.
 *
 * @return File descriptor of the file.
 */
int create_temp_file(void) {
  static char temp[] = "/tmp/tempfileXXXXXX";
  char file_name[32];
  strncpy(file_name, temp, 32);
  return mkstemp(file_name);
}

/**
 * Seek to the beginning of a file and read a message from it.
 *
 * @param fd File descriptor of the file.
 * @param message_type Message type that we expect in the file.
 *
 * @return Pointer to the content of the message. Needs to be freed by the
 * caller.
 */
std::vector<uint8_t> read_message_from_file(int fd, MessageType message_type) {
  /* Go to the beginning of the file. */
  lseek(fd, 0, SEEK_SET);
  MessageType type;
  std::vector<uint8_t> data;
  ARROW_CHECK_OK(ReadMessage(fd, &type, &data));
  ARROW_CHECK(type == message_type);
  return data;
}

PlasmaObject random_plasma_object(void) {
  unsigned int seed = static_cast<unsigned int>(time(NULL));
  int random = rand_r(&seed);
  PlasmaObject object;
  memset(&object, 0, sizeof(object));
  object.store_fd = random + 7;
  object.data_offset = random + 1;
  object.metadata_offset = random + 2;
  object.data_size = random + 3;
  object.metadata_size = random + 4;
  object.device_num = 0;
  return object;
}

TEST(PlasmaSerialization, CreateRequest) {
  int fd = create_temp_file();
  ObjectID object_id1 = random_object_id();
  int64_t data_size1 = 42;
  int64_t metadata_size1 = 11;
  int device_num1 = 0;
  ARROW_CHECK_OK(
      SendCreateRequest(fd, object_id1, data_size1, metadata_size1, device_num1));
  std::vector<uint8_t> data =
      read_message_from_file(fd, MessageType::PlasmaCreateRequest);
  ObjectID object_id2;
  int64_t data_size2;
  int64_t metadata_size2;
  int device_num2;
  ARROW_CHECK_OK(ReadCreateRequest(data.data(), data.size(), &object_id2, &data_size2,
                                   &metadata_size2, &device_num2));
  ASSERT_EQ(data_size1, data_size2);
  ASSERT_EQ(metadata_size1, metadata_size2);
  ASSERT_EQ(object_id1, object_id2);
  ASSERT_EQ(device_num1, device_num2);
  close(fd);
}

TEST(PlasmaSerialization, CreateReply) {
  int fd = create_temp_file();
  ObjectID object_id1 = random_object_id();
  PlasmaObject object1 = random_plasma_object();
  int64_t mmap_size1 = 1000000;
  ARROW_CHECK_OK(SendCreateReply(fd, object_id1, &object1, PlasmaError::OK, mmap_size1));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType::PlasmaCreateReply);
  ObjectID object_id2;
  PlasmaObject object2;
  memset(&object2, 0, sizeof(object2));
  int store_fd;
  int64_t mmap_size2;
  ARROW_CHECK_OK(ReadCreateReply(data.data(), data.size(), &object_id2, &object2,
                                 &store_fd, &mmap_size2));
  ASSERT_EQ(object_id1, object_id2);
  ASSERT_EQ(object1.store_fd, store_fd);
  ASSERT_EQ(mmap_size1, mmap_size2);
  ASSERT_EQ(memcmp(&object1, &object2, sizeof(object1)), 0);
  close(fd);
}

TEST(PlasmaSerialization, SealRequest) {
  int fd = create_temp_file();
  ObjectID object_id1 = random_object_id();
  unsigned char digest1[kDigestSize];
  memset(&digest1[0], 7, kDigestSize);
  bool notify1 = true;
  ARROW_CHECK_OK(SendSealRequest(fd, object_id1, &digest1[0], notify1));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType::PlasmaSealRequest);
  ObjectID object_id2;
  unsigned char digest2[kDigestSize];
  bool notify2;
  ARROW_CHECK_OK(ReadSealRequest(data.data(), data.size(), &object_id2, &digest2[0],
                                 &notify2));
  ASSERT_EQ(object_id1, object_id2);
  ASSERT_EQ(memcmp(&digest1[0], &digest2[0], kDigestSize), 0);
  ASSERT_EQ(notify1, notify2);
  close(fd);
}

TEST(PlasmaSerialization, SealReply) {
  int fd = create_temp_file();
  ObjectID object_id1 = random_object_id();
  ARROW_CHECK_OK(SendSealReply(fd, object_id1, PlasmaError::ObjectExists));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType::PlasmaSealReply);
  ObjectID object_id2;
  Status s = ReadSealReply(data.data(), data.size(), &object_id2);
  ASSERT_EQ(object_id1, object_id2);
  ASSERT_TRUE(s.IsPlasmaObjectExists());
  close(fd);
}

TEST(PlasmaSerialization, GetRequest) {
  int fd = create_temp_file();
  ObjectID object_ids[2];
  object_ids[0] = random_object_id();
  object_ids[1] = random_object_id();
  int64_t timeout_ms = 1234;
  ARROW_CHECK_OK(SendGetRequest(fd, object_ids, 2, timeout_ms));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType::PlasmaGetRequest);
  std::vector<ObjectID> object_ids_return;
  int64_t timeout_ms_return;
  ARROW_CHECK_OK(
      ReadGetRequest(data.data(), data.size(), object_ids_return, &timeout_ms_return));
  ASSERT_EQ(object_ids[0], object_ids_return[0]);
  ASSERT_EQ(object_ids[1], object_ids_return[1]);
  ASSERT_EQ(timeout_ms, timeout_ms_return);
  close(fd);
}

TEST(PlasmaSerialization, GetReply) {
  int fd = create_temp_file();
  ObjectID object_ids[2];
  object_ids[0] = random_object_id();
  object_ids[1] = random_object_id();
  std::unordered_map<ObjectID, PlasmaObject> plasma_objects;
  plasma_objects[object_ids[0]] = random_plasma_object();
  plasma_objects[object_ids[1]] = random_plasma_object();
  std::vector<int> store_fds = {1, 2, 3};
  std::vector<int64_t> mmap_sizes = {100, 200, 300};
  ARROW_CHECK_OK(SendGetReply(fd, object_ids, plasma_objects, 2, store_fds, mmap_sizes));

  std::vector<uint8_t> data = read_message_from_file(fd, MessageType::PlasmaGetReply);
  ObjectID object_ids_return[2];
  PlasmaObject plasma_objects_return[2];
  std::vector<int> store_fds_return;
  std::vector<int64_t> mmap_sizes_return;
  memset(&plasma_objects_return, 0, sizeof(plasma_objects_return));
  ARROW_CHECK_OK(ReadGetReply(data.data(), data.size(), object_ids_return,
                              &plasma_objects_return[0], 2, store_fds_return,
                              mmap_sizes_return));

  ASSERT_EQ(object_ids[0], object_ids_return[0]);
  ASSERT_EQ(object_ids[1], object_ids_return[1]);
  ASSERT_EQ(memcmp(&plasma_objects[object_ids[0]], &plasma_objects_return[0],
                   sizeof(PlasmaObject)),
            0);
  ASSERT_EQ(memcmp(&plasma_objects[object_ids[1]], &plasma_objects_return[1],
                   sizeof(PlasmaObject)),
            0);
  ASSERT_TRUE(store_fds == store_fds_return);
  ASSERT_TRUE(mmap_sizes == mmap_sizes_return);
  close(fd);
}

TEST(PlasmaSerialization, ReleaseRequest) {
  int fd = create_temp_file();
  ObjectID object_id1 = random_object_id();
  ARROW_CHECK_OK(SendReleaseRequest(fd, object_id1));
  std::vector<uint8_t> data =
      read_message_from_file(fd, MessageType::PlasmaReleaseRequest);
  ObjectID object_id2;
  ARROW_CHECK_OK(ReadReleaseRequest(data.data(), data.size(), &object_id2));
  ASSERT_EQ(object_id1, object_id2);
  close(fd);
}

TEST(PlasmaSerialization, ReleaseReply) {
  int fd = create_temp_file();
  ObjectID object_id1 = random_object_id();
  ARROW_CHECK_OK(SendReleaseReply(fd, object_id1, PlasmaError::ObjectExists));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType::PlasmaReleaseReply);
  ObjectID object_id2;
  Status s = ReadReleaseReply(data.data(), data.size(), &object_id2);
  ASSERT_EQ(object_id1, object_id2);
  ASSERT_TRUE(s.IsPlasmaObjectExists());
  close(fd);
}

TEST(PlasmaSerialization, DeleteRequest) {
  int fd = create_temp_file();
  ObjectID object_id1 = random_object_id();
  ARROW_CHECK_OK(SendDeleteRequest(fd, std::vector<ObjectID>{object_id1}));
  std::vector<uint8_t> data =
      read_message_from_file(fd, MessageType::PlasmaDeleteRequest);
  std::vector<ObjectID> object_vec;
  ARROW_CHECK_OK(ReadDeleteRequest(data.data(), data.size(), &object_vec));
  ASSERT_EQ(object_vec.size(), 1);
  ASSERT_EQ(object_id1, object_vec[0]);
  close(fd);
}

TEST(PlasmaSerialization, DeleteReply) {
  int fd = create_temp_file();
  ObjectID object_id1 = random_object_id();
  PlasmaError error1 = PlasmaError::ObjectExists;
  ARROW_CHECK_OK(SendDeleteReply(fd, std::vector<ObjectID>{object_id1},
                                 std::vector<PlasmaError>{error1}));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType::PlasmaDeleteReply);
  std::vector<ObjectID> object_vec;
  std::vector<PlasmaError> error_vec;
  Status s = ReadDeleteReply(data.data(), data.size(), &object_vec, &error_vec);
  ASSERT_EQ(object_vec.size(), 1);
  ASSERT_EQ(object_id1, object_vec[0]);
  ASSERT_EQ(error_vec.size(), 1);
  ASSERT_TRUE(error_vec[0] == PlasmaError::ObjectExists);
  ASSERT_TRUE(s.ok());
  close(fd);
}

TEST(PlasmaSerialization, EvictRequest) {
  int fd = create_temp_file();
  int64_t num_bytes = 111;
  ARROW_CHECK_OK(SendEvictRequest(fd, num_bytes));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType::PlasmaEvictRequest);
  int64_t num_bytes_received;
  ARROW_CHECK_OK(ReadEvictRequest(data.data(), data.size(), &num_bytes_received));
  ASSERT_EQ(num_bytes, num_bytes_received);
  close(fd);
}

TEST(PlasmaSerialization, EvictReply) {
  int fd = create_temp_file();
  int64_t num_bytes = 111;
  ARROW_CHECK_OK(SendEvictReply(fd, num_bytes));
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType::PlasmaEvictReply);
  int64_t num_bytes_received;
  ARROW_CHECK_OK(ReadEvictReply(data.data(), data.size(), num_bytes_received));
  ASSERT_EQ(num_bytes, num_bytes_received);
  close(fd);
}

TEST(PlasmaSerialization, DataRequest) {
  int fd = create_temp_file();
  ObjectID object_id1 = random_object_id();
  const char* address1 = "address1";
  int port1 = 12345;
  ARROW_CHECK_OK(SendDataRequest(fd, object_id1, address1, port1));
  /* Reading message back. */
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType::PlasmaDataRequest);
  ObjectID object_id2;
  char* address2;
  int port2;
  ARROW_CHECK_OK(
      ReadDataRequest(data.data(), data.size(), &object_id2, &address2, &port2));
  ASSERT_EQ(object_id1, object_id2);
  ASSERT_EQ(strcmp(address1, address2), 0);
  ASSERT_EQ(port1, port2);
  free(address2);
  close(fd);
}

TEST(PlasmaSerialization, DataReply) {
  int fd = create_temp_file();
  ObjectID object_id1 = random_object_id();
  int64_t object_size1 = 146;
  int64_t metadata_size1 = 198;
  ARROW_CHECK_OK(SendDataReply(fd, object_id1, object_size1, metadata_size1));
  /* Reading message back. */
  std::vector<uint8_t> data = read_message_from_file(fd, MessageType::PlasmaDataReply);
  ObjectID object_id2;
  int64_t object_size2;
  int64_t metadata_size2;
  ARROW_CHECK_OK(ReadDataReply(data.data(), data.size(), &object_id2, &object_size2,
                               &metadata_size2));
  ASSERT_EQ(object_id1, object_id2);
  ASSERT_EQ(object_size1, object_size2);
  ASSERT_EQ(metadata_size1, metadata_size2);
}

}  // namespace plasma
