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

#include "platform/networkstrate/rdma/rdma_async_replica_client.h"
#include "platform/networkstrate/rdma/rdma_acceptor.h"
#include "platform/networkstrate/rdma/zrpc.h"

#include <chrono>
#include <future>
#include <iostream>
#include <string>
#include <thread>

int main() {
  std::string ip = zrpc::GetHostIpV4();
  if (ip.empty()) {
    ip = "127.0.0.1";
  }

  std::promise<bool> received;
  std::future<bool> received_future = received.get_future();

  std::cout << "Creating RdmaAcceptor on port 9997..." << std::endl;
  resdb::RdmaAcceptor acceptor(9997, 1, [&](uint32_t client_id, const char* buff,
                                            size_t data_len) {
    std::cout << "Received: " << std::string(buff, data_len) << std::endl;
    received.set_value(true);
  });

  acceptor.StartAccept();

  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  std::cout << "Creating RdmaAsyncReplicaClient to " << ip << ":9997..."
            << std::endl;
  resdb::RdmaAsyncReplicaClient client(ip, 9997);
  if (client.SendMessage("test") != 0) {
    std::cerr << "SendMessage failed" << std::endl;
    return 1;
  }

  received_future.get();
  std::cout << "Success!" << std::endl;
  return 0;
}
