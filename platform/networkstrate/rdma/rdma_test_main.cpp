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

#include <atomic>
#include <chrono>
#include <memory>
#include <cstdlib>
#include <functional>
#include <future>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>
#include <sys/wait.h>

static std::string GetHostIp() {
  std::string ip = zrpc::GetHostIpV4();
  return ip.empty() ? "127.0.0.1" : ip;
}

static bool TestSendMessage() {
  std::cout << "[TEST] SendMessage..." << std::endl;
  std::promise<std::string> received;
  std::future<std::string> received_future = received.get_future();

  resdb::RdmaAcceptor acceptor(9997, 1, [&](uint32_t client_id, const char* buff,
                                            size_t data_len) {
    received.set_value(std::string(buff, data_len));
  });
  acceptor.StartAccept();
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  resdb::RdmaAsyncReplicaClient client(GetHostIp(), 9997);
  if (client.SendMessage("test") != 0) {
    std::cerr << "[FAIL] SendMessage failed" << std::endl;
    return false;
  }

  std::string msg = received_future.get();
  if (msg != "test") {
    std::cerr << "[FAIL] Expected 'test', got '" << msg << "'" << std::endl;
    return false;
  }
  std::cout << "[PASS] SendMessage" << std::endl;
  return true;
}

static bool TestMultiSendMessage() {
  std::cout << "[TEST] MultiSendMessage..." << std::endl;
  std::promise<int> count_promise;
  std::future<int> count_future = count_promise.get_future();
  std::atomic<int> received_count{0};

  resdb::RdmaAcceptor acceptor(9996, 1, [&](uint32_t client_id, const char* buff,
                                            size_t data_len) {
    if (std::string(buff, data_len) == "test" &&
        received_count.fetch_add(1) + 1 == 100) {
      count_promise.set_value(100);
    }
  });
  acceptor.StartAccept();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  resdb::RdmaAsyncReplicaClient client(GetHostIp(), 9996);
  for (int i = 0; i < 100; ++i) {
    if (client.SendMessage("test", true) != 0) {
      std::cerr << "[FAIL] SendMessage failed at " << i << std::endl;
      return false;
    }
  }
  client.Progress();

  if (count_future.get() != 100) {
    std::cerr << "[FAIL] Expected 100 messages" << std::endl;
    return false;
  }
  std::cout << "[PASS] MultiSendMessage" << std::endl;
  return true;
}

static bool TestMultiClient() {
  std::cout << "[TEST] MultiClient..." << std::endl;
  std::promise<int> count_promise;
  std::future<int> count_future = count_promise.get_future();
  std::atomic<int> received_count{0};

  resdb::RdmaAcceptor acceptor(9998, 3, [&](uint32_t client_id, const char* buff,
                                            size_t data_len) {
    if (received_count.fetch_add(1) + 1 == 3) {
      count_promise.set_value(3);
    }
  });
  acceptor.StartAccept();
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  for (int i = 0; i < 3; ++i) {
    resdb::RdmaAsyncReplicaClient client(GetHostIp(), 9998);
    client.SendMessage("test");
  }

  if (count_future.get() != 3) {
    std::cerr << "[FAIL] Expected 3 clients" << std::endl;
    return false;
  }
  std::cout << "[PASS] MultiClient" << std::endl;
  return true;
}

static bool TestMultiClientVariableMessages() {
  std::cout << "[TEST] MultiClientVariableMessages..." << std::endl;
  const int kCounts[] = {10, 5, 15};
  const int kTotal = 10 + 5 + 15;
  const int kNumClients = 3;
  const auto kTimeout = std::chrono::seconds(15);
  std::promise<void> done_promise;
  std::future<void> done_future = done_promise.get_future();
  std::atomic<int> received_total{0};
  std::atomic<int> received_per_client[kNumClients];
  for (int i = 0; i < kNumClients; ++i) received_per_client[i].store(0);

  resdb::RdmaAcceptor acceptor(9995, 3, [&](uint32_t client_id, const char* buff,
                                            size_t data_len) {
    (void)client_id;
    if (data_len >= 1 && buff[0] >= '0' && buff[0] <= '2') {
      int idx = buff[0] - '0';
      received_per_client[idx].fetch_add(1);
      if (received_total.fetch_add(1) + 1 == kTotal) {
        done_promise.set_value();
      }
    }
  });
  acceptor.StartAccept();
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  std::vector<std::unique_ptr<resdb::RdmaAsyncReplicaClient>> clients;
  for (int i = 0; i < kNumClients; ++i) {
    clients.push_back(std::unique_ptr<resdb::RdmaAsyncReplicaClient>(
        new resdb::RdmaAsyncReplicaClient(GetHostIp(), 9995)));
  }

  std::vector<std::thread> threads;
  for (int i = 0; i < kNumClients; ++i) {
    threads.emplace_back([&, i]() {
      std::string msg(1, '0' + i);
      for (int j = 0; j < kCounts[i]; ++j) {
        if (clients[i]->SendMessage(msg, false) != 0) {
          std::cerr << "[FAIL] SendMessage failed at client " << i << " msg "
                    << j << std::endl;
          return;
        }
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }

  if (done_future.wait_for(kTimeout) != std::future_status::ready) {
    std::cerr << "[FAIL] Timeout after 15s. Received: total="
              << received_total.load() << ", per_client=[" << received_per_client[0].load()
              << "," << received_per_client[1].load() << ","
              << received_per_client[2].load() << "]" << std::endl;
    return false;
  }
  done_future.get();
  for (int i = 0; i < kNumClients; ++i) {
    if (received_per_client[i].load() != kCounts[i]) {
      std::cerr << "[FAIL] Client " << i << ": expected " << kCounts[i]
                << ", got " << received_per_client[i].load() << std::endl;
      return false;
    }
  }
  std::cout << "[PASS] MultiClientVariableMessages" << std::endl;
  return true;
}

int main(int argc, char* argv[]) {
  const std::map<std::string, std::function<bool()>> tests = {
      {"SendMessage", TestSendMessage},
      {"MultiSend", TestMultiSendMessage},
      {"MultiClient", TestMultiClient},
      {"MultiClientMultiSend", TestMultiClientVariableMessages},
  };

  if (argc >= 2) {
    std::string name = argv[1];
    if (name == "--list" || name == "-l") {
      for (const auto& p : tests) {
        std::cout << p.first << std::endl;
      }
      return 0;
    }
    if (name == "all" || name == "--all" || name == "-a") {
      std::cout << "Running all RDMA tests (each in separate process)..."
                << std::endl;
      int failed = 0;
      for (const auto& p : tests) {
        pid_t pid = fork();
        if (pid < 0) {
          std::cerr << "[FAIL] fork failed" << std::endl;
          return 1;
        }
        if (pid == 0) {
          return p.second() ? 0 : 1;
        }
        int status = 0;
        waitpid(pid, &status, 0);
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
          ++failed;
        }
      }
      return failed ? 1 : 0;
    }
    auto it = tests.find(name);
    if (it != tests.end()) {
      std::cout << "Running RDMA tests..." << std::endl;
      return it->second() ? 0 : 1;
    }
    std::cerr << "Unknown test: " << name << "\nUse --list to see available tests"
              << std::endl;
    return 1;
  }

  std::cout << "Usage: " << (argc ? argv[0] : "rdma_test_main")
            << " <TestName|all>\nUse --list to see available tests" << std::endl;
  return 0;
}
