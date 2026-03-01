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

#include <cstdint>
#include <functional>
#include <string>

namespace zrpc {

class Client {
 public:
  Client(const std::string& server_ip, int port,
         uint32_t max_outstanding = 8);
  ~Client();

  void Send(const std::string& message);
  uint64_t SendAsync(const std::string& message);
  uint32_t ProgressOnce();
  void Progress();
  uint32_t Outstanding() const;
  uint32_t MaxOutstanding() const;

 private:
  class Impl;
  Impl* impl_;
};

class MultiServer {
 public:
  MultiServer(int port, uint32_t max_outstanding, uint32_t max_clients);
  ~MultiServer();

  void Run(const std::function<void(uint32_t, const std::string&)>& handler);
  void Stop();

 private:
  class Impl;
  Impl* impl_;
};

std::string GetHostIpV4();

}  // namespace zrpc
