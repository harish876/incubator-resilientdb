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

#include <glog/logging.h>

#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "executor/common/transaction_manager.h"
#include "interface/rdbc/transaction_constructor.h"
#include "platform/config/resdb_config.h"
#include "platform/config/resdb_config_utils.h"
#include "platform/consensus/ordering/pbft/consensus_manager_pbft.h"
#include "platform/networkstrate/service_network.h"
#include "proto/kv/kv.pb.h"

namespace resdb {

class LightweightExecutor : public TransactionManager {
 public:
  LightweightExecutor() : TransactionManager(false, true) {}

  std::unique_ptr<std::string> ExecuteData(const std::string& request) override {
    KVResponse response;
    response.set_value("ok");
    auto data = std::make_unique<std::string>();
    response.SerializeToString(data.get());
    return data;
  }
};

std::unique_ptr<ServiceNetwork> CreateServer(const ResDBConfig& config) {
  auto consensus = std::make_unique<ConsensusManagerPBFT>(
      config, std::make_unique<LightweightExecutor>());
  return std::make_unique<ServiceNetwork>(config, std::move(consensus));
}

// One-shot local benchmark helper:
// starts 4 replicas, sends one SET request, verifies response is "ok".
int RunSelfTest(bool use_rdma, const std::string& ip, int base_port) {
  std::vector<ReplicaInfo> replicas = {
      GenerateReplicaInfo(1, ip, base_port + 1),
      GenerateReplicaInfo(2, ip, base_port + 2),
      GenerateReplicaInfo(3, ip, base_port + 3),
      GenerateReplicaInfo(4, ip, base_port + 4),
  };

  ResConfigData data;
  data.set_enable_viewchange(false);
  data.set_enable_resview(false);
  data.set_enable_faulty_switch(false);
  data.set_enable_rdma(use_rdma);
  data.set_rdma_port_offset(0);

  std::vector<std::unique_ptr<ServiceNetwork>> servers;
  std::vector<std::thread> server_threads;
  servers.reserve(replicas.size());
  server_threads.reserve(replicas.size());

  for (const auto& self : replicas) {
    ResDBConfig cfg(replicas, self, data);
    cfg.SetHeartBeatEnabled(false);
    cfg.SetSignatureVerifierEnabled(false);
    cfg.SetTestMode(true);
    cfg.RunningPerformance(false);
    servers.push_back(CreateServer(cfg));
  }

  for (size_t i = 0; i < servers.size(); ++i) {
    if (i > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    ServiceNetwork* s = servers[i].get();
    server_threads.emplace_back([s]() { s->Run(); });
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // RDMA: pre-warm connections so all replicas can reach each other before
  // consensus starts. Reduces connection setup races in single-process mode.
  if (use_rdma) {
    for (auto& server : servers) {
      server->PreWarmRdmaConnections();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }

  ResDBConfig client_cfg(replicas, ReplicaInfo(), data);
  client_cfg.SetClientTimeoutMs(5000000);
  TransactionConstructor client(client_cfg);

  KVRequest req;
  req.set_cmd(KVRequest::SET);
  req.set_key("light_key");
  req.set_value("light_value");

  KVResponse resp;
  int ret = client.SendRequest(req, &resp);
  bool ok = (ret == 0 && resp.value() == "ok");

  for (auto& server : servers) {
    server->Stop();
  }
  for (auto& th : server_threads) {
    if (th.joinable()) {
      th.join();
    }
  }

  if (!ok) {
    LOG(ERROR) << "Self-test failed ret=" << ret
               << " response_value=" << resp.value();
    return 1;
  }
  LOG(ERROR) << "Self-test passed mode=" << (use_rdma ? "rdma" : "tcp")
             << " response_value=" << resp.value();
  return 0;
}

}  // namespace resdb

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  FLAGS_minloglevel = google::GLOG_ERROR;

  // One-command mode:
  //   <bin> --selftest [tcp|rdma] [ip] [base_port]
  if (argc >= 2 && std::string(argv[1]) == "--selftest") {
    const std::string mode = (argc >= 3) ? argv[2] : "tcp";
    const bool use_rdma = (mode == "rdma");
    const std::string ip = (argc >= 4) ? argv[3] : "127.0.0.1";
    const int base_port = (argc >= 5) ? atoi(argv[4]) : 23000;
    return resdb::RunSelfTest(use_rdma, ip, base_port);
  }

  if (argc < 4) {
    printf(
        "Usage:\n"
        "  %s <server.config> <node_private_key.key.pri> <cert_file.cert>\n"
        "  %s --selftest [tcp|rdma] [ip] [base_port]\n",
        argv[0], argv[0]);
    return 1;
  }

  std::unique_ptr<resdb::ResDBConfig> config =
      resdb::GenerateResDBConfig(argv[1], argv[2], argv[3]);

  // Keep the benchmark focused on consensus transport behavior.
  config->SetHeartBeatEnabled(false);
  config->SetSignatureVerifierEnabled(false);

  auto consensus = std::make_unique<resdb::ConsensusManagerPBFT>(
      *config, std::make_unique<resdb::LightweightExecutor>());
  auto server =
      std::make_unique<resdb::ServiceNetwork>(*config, std::move(consensus));
  server->Run();
  return 0;
}
