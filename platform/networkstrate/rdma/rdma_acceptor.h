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

#include <functional>
#include <memory>
#include <thread>

#include "platform/networkstrate/rdma/zrpc.h"

namespace resdb {

class RdmaAcceptor {
 public:
  typedef std::function<void(uint32_t client_id, const char* buffer,
                             size_t len)>
      CallBack;

  RdmaAcceptor(int port, uint32_t max_clients, CallBack call_back_func,
               uint32_t buffer_len = 65536, uint32_t max_payload = 65536);
  virtual ~RdmaAcceptor();

  void StartAccept();
  void Stop();

 private:
  int port_;
  uint32_t max_clients_;
  uint32_t buffer_len_;
  uint32_t max_payload_;
  CallBack call_back_func_;
  std::unique_ptr<zrpc::MultiServer> server_;
  std::thread server_thread_;
};

}  // namespace resdb
