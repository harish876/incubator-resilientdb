/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <mutex>
#include <memory>
#include <string>

#include "platform/networkstrate/rdma/zrpc.h"

namespace resdb {

class RdmaAsyncReplicaClient {
 public:
  RdmaAsyncReplicaClient(const std::string& ip, int port,
                         uint32_t max_outstanding = 8,
                         uint32_t buffer_len = 65536,
                         uint32_t max_payload = 65536,
                         bool use_basic_ring = false);
  virtual ~RdmaAsyncReplicaClient();

  virtual int SendMessage(const std::string& data, bool use_async = false);
  void Progress();
  uint32_t Outstanding() const;

 private:
  std::unique_ptr<zrpc::Client> client_;
  std::string ip_;
  int port_;
  uint32_t max_outstanding_;
  uint32_t max_payload_;
  mutable std::mutex send_mutex_;
};

}  // namespace resdb
