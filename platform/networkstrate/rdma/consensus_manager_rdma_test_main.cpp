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
#include "platform/networkstrate/rdma/rdma_replica_communicator.h"
#include "platform/networkstrate/rdma/zrpc.h"
#include "platform/proto/broadcast.pb.h"
#include "platform/proto/resdb.pb.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <future>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <unistd.h>
#include <sys/wait.h>
#include <vector>

static std::string GetHostIp() {
  std::string ip = zrpc::GetHostIpV4();
  return ip.empty() ? "127.0.0.1" : ip;
}

static bool TestRdmaReplicaCommunicatorBroadcast() {
  std::cout << "[TEST] RdmaReplicaCommunicatorBroadcast (1 replica)..."
            << std::endl;
  const int kPort = 19994;
  const int kRdmaPortOffset = 20000;
  std::string host_ip = GetHostIp();

  std::promise<std::string> received;
  std::future<std::string> received_future = received.get_future();

  resdb::RdmaAcceptor acceptor(
      kPort + kRdmaPortOffset, 1,
      [&](uint32_t /*client_id*/, const char* buff, size_t len) {
        received.set_value(std::string(buff, len));
      });
  acceptor.StartAccept();
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  resdb::ReplicaInfo replica;
  replica.set_id(1);
  replica.set_ip(host_ip);
  replica.set_port(kPort);

  {
    resdb::RdmaReplicaCommunicator communicator({replica}, nullptr,
                                                kRdmaPortOffset);

    resdb::Request request;
    request.set_type(resdb::Request::TYPE_CLIENT_REQUEST);
    request.set_sender_id(1);
    request.set_data("test_consensus_data");

    communicator.BroadCast(request);

    std::string msg = received_future.get();
    if (msg.empty()) {
      std::cerr << "[FAIL] Received empty message" << std::endl;
      return false;
    }

    BroadcastData broadcast_data;
    if (!broadcast_data.ParseFromString(msg)) {
      std::cerr << "[FAIL] Failed to parse BroadcastData" << std::endl;
      return false;
    }
    if (broadcast_data.data_size() != 1) {
      std::cerr << "[FAIL] Expected 1 data item, got "
                << broadcast_data.data_size() << std::endl;
      return false;
    }

    resdb::ResDBMessage resdb_msg;
    if (!resdb_msg.ParseFromString(broadcast_data.data(0))) {
      std::cerr << "[FAIL] Failed to parse ResDBMessage" << std::endl;
      return false;
    }

    resdb::Request received_request;
    if (!received_request.ParseFromString(resdb_msg.data())) {
      std::cerr << "[FAIL] Failed to parse Request" << std::endl;
      return false;
    }
    if (received_request.data() != "test_consensus_data") {
      std::cerr << "[FAIL] Expected 'test_consensus_data', got '"
                << received_request.data() << "'" << std::endl;
      return false;
    }
  }

  std::cout << "[PASS] RdmaReplicaCommunicatorBroadcast (1 replica)"
            << std::endl;
  return true;
}

static bool TestRdmaReplicaCommunicatorBroadcastMulti() {
  std::cout << "[TEST] RdmaReplicaCommunicatorBroadcast (4 replicas)..."
            << std::endl;
  const int kBasePort = 19994;
  const int kRdmaPortOffset = 20000;
  const int kNumReplicas = 4;
  std::string host_ip = GetHostIp();

  std::vector<std::string> received_messages;
  std::mutex mutex;
  std::promise<void> all_received;
  std::future<void> all_received_future = all_received.get_future();

  auto callback = [&](uint32_t /*client_id*/, const char* buff, size_t len) {
    std::lock_guard<std::mutex> lock(mutex);
    received_messages.push_back(std::string(buff, len));
    if (received_messages.size() == kNumReplicas) {
      all_received.set_value();
    }
  };

  std::vector<std::unique_ptr<resdb::RdmaAcceptor>> acceptors;
  for (int i = 0; i < kNumReplicas; ++i) {
    acceptors.push_back(std::make_unique<resdb::RdmaAcceptor>(
        kBasePort + i + kRdmaPortOffset, 1, callback));
    acceptors.back()->StartAccept();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  std::vector<resdb::ReplicaInfo> replicas;
  for (int i = 0; i < kNumReplicas; ++i) {
    resdb::ReplicaInfo replica;
    replica.set_id(i + 1);
    replica.set_ip(host_ip);
    replica.set_port(kBasePort + i);
    replicas.push_back(replica);
  }

  {
    resdb::RdmaReplicaCommunicator communicator(replicas, nullptr,
                                                 kRdmaPortOffset);

    resdb::Request request;
    request.set_type(resdb::Request::TYPE_CLIENT_REQUEST);
    request.set_sender_id(1);
    request.set_data("test_consensus_data");

    communicator.BroadCast(request);

    all_received_future.get();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  if (received_messages.size() != kNumReplicas) {
    std::cerr << "[FAIL] Expected " << kNumReplicas << " messages, got "
              << received_messages.size() << std::endl;
    return false;
  }

  for (int i = 0; i < kNumReplicas; ++i) {
    const std::string& msg = received_messages[i];
    if (msg.empty()) {
      std::cerr << "[FAIL] Replica " << (i + 1) << " received empty message"
                << std::endl;
      return false;
    }

    BroadcastData broadcast_data;
    if (!broadcast_data.ParseFromString(msg)) {
      std::cerr << "[FAIL] Replica " << (i + 1)
                << ": Failed to parse BroadcastData" << std::endl;
      return false;
    }
    if (broadcast_data.data_size() != 1) {
      std::cerr << "[FAIL] Replica " << (i + 1)
                << ": Expected 1 data item, got " << broadcast_data.data_size()
                << std::endl;
      return false;
    }

    resdb::ResDBMessage resdb_msg;
    if (!resdb_msg.ParseFromString(broadcast_data.data(0))) {
      std::cerr << "[FAIL] Replica " << (i + 1)
                << ": Failed to parse ResDBMessage" << std::endl;
      return false;
    }

    resdb::Request received_request;
    if (!received_request.ParseFromString(resdb_msg.data())) {
      std::cerr << "[FAIL] Replica " << (i + 1)
                << ": Failed to parse Request" << std::endl;
      return false;
    }
    if (received_request.data() != "test_consensus_data") {
      std::cerr << "[FAIL] Replica " << (i + 1)
                << ": Expected 'test_consensus_data', got '"
                << received_request.data() << "'" << std::endl;
      return false;
    }
  }

  std::cout << "[PASS] RdmaReplicaCommunicatorBroadcast (4 replicas)"
            << std::endl;
  return true;
}

static bool TestRdmaReplicaCommunicatorSendHeartBeat() {
  std::cout << "[TEST] RdmaReplicaCommunicatorSendHeartBeat (2 replicas + 1 "
               "client)..."
            << std::endl;
  const int kBasePort = 19998;
  const int kRdmaPortOffset = 20000;
  const int kNumReplicas = 2;
  const int kNumClientReplicas = 1;
  const int kTotalTargets = kNumReplicas + kNumClientReplicas;
  std::string host_ip = GetHostIp();

  std::vector<std::string> received_messages;
  std::mutex mutex;
  std::promise<void> all_received;
  std::future<void> all_received_future = all_received.get_future();

  auto callback = [&](uint32_t /*client_id*/, const char* buff, size_t len) {
    std::lock_guard<std::mutex> lock(mutex);
    received_messages.push_back(std::string(buff, len));
    if (received_messages.size() == kTotalTargets) {
      all_received.set_value();
    }
  };

  std::vector<std::unique_ptr<resdb::RdmaAcceptor>> acceptors;
  for (int i = 0; i < kTotalTargets; ++i) {
    acceptors.push_back(std::make_unique<resdb::RdmaAcceptor>(
        kBasePort + i + kRdmaPortOffset, 1, callback));
    acceptors.back()->StartAccept();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  std::vector<resdb::ReplicaInfo> replicas;
  for (int i = 0; i < kNumReplicas; ++i) {
    resdb::ReplicaInfo replica;
    replica.set_id(i + 1);
    replica.set_ip(host_ip);
    replica.set_port(kBasePort + i);
    replicas.push_back(replica);
  }

  resdb::ReplicaInfo client_replica;
  client_replica.set_id(100);
  client_replica.set_ip(host_ip);
  client_replica.set_port(kBasePort + kNumReplicas);

  {
    resdb::RdmaReplicaCommunicator communicator(replicas, nullptr,
                                                 kRdmaPortOffset);
    communicator.UpdateClientReplicas({client_replica});

    resdb::Request hb_request;
    hb_request.set_type(resdb::Request::TYPE_HEART_BEAT);
    hb_request.set_sender_id(1);
    hb_request.set_data("heartbeat_data");

    int sent = communicator.SendHeartBeat(hb_request);
    if (sent != kTotalTargets) {
      std::cerr << "[FAIL] SendHeartBeat returned " << sent << ", expected "
                << kTotalTargets << std::endl;
      return false;
    }

    all_received_future.get();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  if (received_messages.size() != kTotalTargets) {
    std::cerr << "[FAIL] Expected " << kTotalTargets << " messages, got "
              << received_messages.size() << std::endl;
    return false;
  }

  for (size_t i = 0; i < received_messages.size(); ++i) {
    const std::string& msg = received_messages[i];
    if (msg.empty()) {
      std::cerr << "[FAIL] Target " << (i + 1) << " received empty message"
                << std::endl;
      return false;
    }

    BroadcastData broadcast_data;
    if (!broadcast_data.ParseFromString(msg)) {
      std::cerr << "[FAIL] Target " << (i + 1)
                << ": Failed to parse BroadcastData" << std::endl;
      return false;
    }
    if (broadcast_data.data_size() != 1) {
      std::cerr << "[FAIL] Target " << (i + 1)
                << ": Expected 1 data item, got " << broadcast_data.data_size()
                << std::endl;
      return false;
    }

    resdb::ResDBMessage resdb_msg;
    if (!resdb_msg.ParseFromString(broadcast_data.data(0))) {
      std::cerr << "[FAIL] Target " << (i + 1)
                << ": Failed to parse ResDBMessage" << std::endl;
      return false;
    }

    resdb::Request received_request;
    if (!received_request.ParseFromString(resdb_msg.data())) {
      std::cerr << "[FAIL] Target " << (i + 1)
                << ": Failed to parse Request" << std::endl;
      return false;
    }
    if (received_request.type() != resdb::Request::TYPE_HEART_BEAT) {
      std::cerr << "[FAIL] Target " << (i + 1)
                << ": Expected TYPE_HEART_BEAT, got " << received_request.type()
                << std::endl;
      return false;
    }
    if (received_request.data() != "heartbeat_data") {
      std::cerr << "[FAIL] Target " << (i + 1)
                << ": Expected 'heartbeat_data', got '"
                << received_request.data() << "'" << std::endl;
      return false;
    }
  }

  std::cout << "[PASS] RdmaReplicaCommunicatorSendHeartBeat (2 replicas + 1 "
               "client)"
            << std::endl;
  return true;
}

static bool TestRdmaReplicaCommunicatorSendHeartBeatSelf() {
  std::cout << "[TEST] RdmaReplicaCommunicatorSendHeartBeatSelf..."
            << std::endl;
  const int kPort = 20008;
  const int kRdmaPortOffset = 0;
  std::string host_ip = GetHostIp();

  std::promise<std::string> received;
  std::future<std::string> received_future = received.get_future();

  resdb::RdmaAcceptor acceptor(
      kPort + kRdmaPortOffset, 1,
      [&](uint32_t /*client_id*/, const char* buff, size_t len) {
        received.set_value(std::string(buff, len));
      });
  acceptor.StartAccept();
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  resdb::ReplicaInfo self_replica;
  self_replica.set_id(1);
  self_replica.set_ip(host_ip);
  self_replica.set_port(kPort);

  {
    std::vector<resdb::ReplicaInfo> replicas = {self_replica};
    resdb::RdmaReplicaCommunicator communicator(replicas, nullptr,
                                                kRdmaPortOffset);

    resdb::Request hb_request;
    hb_request.set_type(resdb::Request::TYPE_HEART_BEAT);
    hb_request.set_sender_id(1);
    hb_request.set_data("self_heartbeat_data");

    int sent = communicator.SendHeartBeat(hb_request);
    if (sent != 0) {
      std::cerr << "[FAIL] SendHeartBeat returned " << sent
                << ", expected 0 when only self target exists" << std::endl;
      return false;
    }

    if (received_future.wait_for(std::chrono::milliseconds(300)) !=
        std::future_status::timeout) {
      std::cerr << "[FAIL] Unexpected self-heartbeat delivery" << std::endl;
      return false;
    }
  }

  std::cout << "[PASS] RdmaReplicaCommunicatorSendHeartBeatSelf (self skipped)"
            << std::endl;
  return true;
}

// Simple test: 2 replicas, each has server + client, everyone talks to everyone.
// No mocks - real RdmaAcceptor and RdmaReplicaCommunicator.
static bool TestRdmaReplicasEveryoneTalks() {
  std::cout << "[TEST] RdmaReplicasEveryoneTalks (2 replicas, each server+client)"
            << std::endl;
  const int kBasePort = 20002;
  const int kRdmaPortOffset = 20000;
  const int kNumReplicas = 2;
  std::string host_ip = GetHostIp();

  std::vector<std::string> replica_received(2);
  std::promise<void> both_received;
  std::future<void> both_future = both_received.get_future();
  std::atomic<int> receive_count{0};

  std::vector<std::unique_ptr<resdb::RdmaAcceptor>> acceptors;
  for (int i = 0; i < kNumReplicas; ++i) {
    int replica_id = i;
    acceptors.push_back(std::make_unique<resdb::RdmaAcceptor>(
        kBasePort + replica_id + kRdmaPortOffset, 1,
        [&, replica_id](uint32_t /*client_id*/, const char* buff, size_t len) {
          replica_received[replica_id] = std::string(buff, len);
          if (++receive_count == kNumReplicas) {
            both_received.set_value();
          }
        }));
    acceptors.back()->StartAccept();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  std::vector<resdb::ReplicaInfo> all_replicas;
  for (int i = 0; i < kNumReplicas; ++i) {
    resdb::ReplicaInfo r;
    r.set_id(i + 1);
    r.set_ip(host_ip);
    r.set_port(kBasePort + i);
    all_replicas.push_back(r);
  }

  {
    std::vector<resdb::ReplicaInfo> targets_for_0 = {all_replicas[1]};
    std::vector<resdb::ReplicaInfo> targets_for_1 = {all_replicas[0]};
    resdb::RdmaReplicaCommunicator comm0(targets_for_0, nullptr,
                                         kRdmaPortOffset);
    resdb::RdmaReplicaCommunicator comm1(targets_for_1, nullptr,
                                         kRdmaPortOffset);

    resdb::Request req0;
    req0.set_type(resdb::Request::TYPE_CLIENT_REQUEST);
    req0.set_sender_id(1);
    req0.set_data("from_replica_0");

    resdb::Request req1;
    req1.set_type(resdb::Request::TYPE_CLIENT_REQUEST);
    req1.set_sender_id(2);
    req1.set_data("from_replica_1");

    comm0.SendMessage(req0, all_replicas[1]);
    comm1.SendMessage(req1, all_replicas[0]);

    both_future.get();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  for (int i = 0; i < kNumReplicas; ++i) {
    const std::string& msg = replica_received[i];
    if (msg.empty()) {
      std::cerr << "[FAIL] Replica " << i << " received nothing" << std::endl;
      return false;
    }
    BroadcastData broadcast_data;
    if (!broadcast_data.ParseFromString(msg)) {
      std::cerr << "[FAIL] Replica " << i << ": parse BroadcastData failed"
                << std::endl;
      return false;
    }
    resdb::ResDBMessage resdb_msg;
    if (!resdb_msg.ParseFromString(broadcast_data.data(0))) {
      std::cerr << "[FAIL] Replica " << i << ": parse ResDBMessage failed"
                << std::endl;
      return false;
    }
    resdb::Request recv;
    if (!recv.ParseFromString(resdb_msg.data())) {
      std::cerr << "[FAIL] Replica " << i << ": parse Request failed"
                << std::endl;
      return false;
    }
    std::string expected = (i == 0) ? "from_replica_1" : "from_replica_0";
    if (recv.data() != expected) {
      std::cerr << "[FAIL] Replica " << i << ": expected '" << expected
                << "', got '" << recv.data() << "'" << std::endl;
      return false;
    }
  }

  std::cout << "[PASS] RdmaReplicasEveryoneTalks" << std::endl;
  return true;
}

int main(int argc, char* argv[]) {
  const std::map<std::string, std::function<bool()>> tests = {
      {"RdmaReplicaCommunicatorBroadcast",
       TestRdmaReplicaCommunicatorBroadcast},
      {"RdmaReplicaCommunicatorBroadcastMulti",
       TestRdmaReplicaCommunicatorBroadcastMulti},
      {"RdmaReplicaCommunicatorSendHeartBeat",
       TestRdmaReplicaCommunicatorSendHeartBeat},
      {"RdmaReplicaCommunicatorSendHeartBeatSelf",
       TestRdmaReplicaCommunicatorSendHeartBeatSelf},
      {"RdmaReplicasEveryoneTalks", TestRdmaReplicasEveryoneTalks},
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
      std::cout << "Running all consensus manager RDMA tests (each in separate "
                   "process)..."
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
      std::cout << "Running consensus manager RDMA tests..." << std::endl;
      return it->second() ? 0 : 1;
    }
    std::cerr << "Unknown test: " << name
              << "\nUse --list to see available tests" << std::endl;
    return 1;
  }

  std::cout << "Usage: " << (argc ? argv[0] : "consensus_manager_rdma_test_main")
            << " <TestName|all>\nUse --list to see available tests" << std::endl;
  return 0;
}
