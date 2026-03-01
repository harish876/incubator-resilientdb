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

#include "platform/networkstrate/rdma/rdma_acceptor.h"

namespace resdb {

RdmaAcceptor::RdmaAcceptor(int port, uint32_t max_clients,
                           CallBack call_back_func)
    : port_(port),
      max_clients_(max_clients),
      call_back_func_(call_back_func),
      server_(new zrpc::MultiServer(port, 8, max_clients)) {}

RdmaAcceptor::~RdmaAcceptor() {
  if (server_thread_.joinable()) {
    server_->Stop();
    server_thread_.join();
  }
}

void RdmaAcceptor::StartAccept() {
  server_thread_ = std::thread([this]() {
    server_->Run([this](uint32_t client_id, const std::string& message) {
      if (call_back_func_) {
        call_back_func_(client_id, message.data(), message.size());
      }
    });
  });
}

}  // namespace resdb
