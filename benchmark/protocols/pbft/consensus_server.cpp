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
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <algorithm>

#include "cxxopts.hpp"
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
    KVRequest kv_req;
    if (!kv_req.ParseFromString(request)) {
      return nullptr;
    }
    KVResponse response;
    std::lock_guard<std::mutex> lock(storage_mutex_);
    if (kv_req.cmd() == KVRequest::SET) {
      storage_[kv_req.key()] = kv_req.value();
      response.set_value("ok");
    } else if (kv_req.cmd() == KVRequest::GET) {
      auto it = storage_.find(kv_req.key());
      response.set_value(it != storage_.end() ? it->second : "");
    } else {
      response.set_value("ok");
    }
    auto data = std::make_unique<std::string>();
    response.SerializeToString(data.get());
    return data;
  }

 private:
  std::unordered_map<std::string, std::string> storage_;
  std::mutex storage_mutex_;
};

std::unique_ptr<ServiceNetwork> CreateServer(const ResDBConfig& config) {
  auto consensus = std::make_unique<ConsensusManagerPBFT>(
      config, std::make_unique<LightweightExecutor>());
  return std::make_unique<ServiceNetwork>(config, std::move(consensus));
}

// Simple struct to hold latency statistics.
struct LatencyStats {
  double avg_us = 0;
  double p50_us = 0;
  double p95_us = 0;
  double p99_us = 0;
  double min_us = 0;
  double max_us = 0;
};

static double PercentileFromSorted(const std::vector<double>& v, double pct) {
  if (v.empty()) return 0.0;
  size_t idx = static_cast<size_t>(pct * (v.size() - 1));
  return v[idx];
}

static LatencyStats ComputeLatencyStats(std::vector<double> lat_us) {
  LatencyStats s;
  if (lat_us.empty()) return s;

  std::sort(lat_us.begin(), lat_us.end());

  s.min_us = lat_us.front();
  s.max_us = lat_us.back();

  double sum = 0.0;
  for (double x : lat_us) sum += x;
  s.avg_us = sum / lat_us.size();

  s.p50_us = PercentileFromSorted(lat_us, 0.50);
  s.p95_us = PercentileFromSorted(lat_us, 0.95);
  s.p99_us = PercentileFromSorted(lat_us, 0.99);

  return s;
}

// Adding num_requests as a parameter to allow flexibility in testing with different numbers of requests.
// Keeping its default to 2, to maintain backward compatibility 
int RunSelfTest(bool use_rdma, const std::string& ip, int base_port,
                bool use_basic_ring = false, int num_requests = 2) {
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
  data.set_use_basic_ring_rdma(use_basic_ring);

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

  // RDMA: establish control-plane connections before data-plane traffic.
  // Reduces connection setup races in single-process selftest.
  if (use_rdma) {
    for (auto& server : servers) {
      server->EstablishRdmaControlPlane();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }

  ResDBConfig client_cfg(replicas, ReplicaInfo(), data);
  //client_cfg.SetClientTimeoutMs(5000000);
  client_cfg.SetClientTimeoutMs(5000);
  TransactionConstructor client(client_cfg);

  //---------------------------------------------------------------------
  // KVRequest set_req;
  // set_req.set_cmd(KVRequest::SET);
  // set_req.set_key("light_key");
  // set_req.set_value("light_value");

  // KVResponse set_resp;
  // int ret = client.SendRequest(set_req, &set_resp);
  // bool set_ok = (ret == 0 && set_resp.value() == "ok");

  // KVRequest get_req;
  // get_req.set_cmd(KVRequest::GET);
  // get_req.set_key("light_key");

  // KVResponse get_resp;
  // ret = client.SendRequest(get_req, &get_resp);
  // bool get_ok = (ret == 0 && get_resp.value() == "light_value");

  // bool ok = set_ok && get_ok;
  //---------------------------------------------------------------------
  
  // Warmup SET so later GETs have a valid key.
  KVRequest set_req;
  set_req.set_cmd(KVRequest::SET);
  set_req.set_key("light_key");
  set_req.set_value("light_value");

  KVResponse set_resp;
  int ret = client.SendRequest(set_req, &set_resp);
  bool set_ok = (ret == 0 && set_resp.value() == "ok");

  std::vector<double> lat_us;
  lat_us.reserve(num_requests);

  bool get_ok = true;
  KVResponse last_get_resp;

  auto overall_start = std::chrono::steady_clock::now();

  for (int i = 0; i < num_requests; ++i) {
    KVRequest get_req;
    get_req.set_cmd(KVRequest::GET);
    get_req.set_key("light_key");

    KVResponse get_resp;
    auto req_start = std::chrono::steady_clock::now();
    // DEBUG print: Remove Later to avoid cluttering output
    LOG(ERROR) << "Selftest sending request i=" << i;
    ret = client.SendRequest(get_req, &get_resp);
    LOG(ERROR) << "Selftest completed request i=" << i
           << " ret=" << ret
           << " value=" << get_resp.value();
    auto req_end = std::chrono::steady_clock::now();

    double us = std::chrono::duration<double, std::micro>(req_end - req_start).count();
    lat_us.push_back(us);

    if (!(ret == 0 && get_resp.value() == "light_value")) {
      get_ok = false;
    }
    last_get_resp = get_resp;
  }

  auto overall_end = std::chrono::steady_clock::now();
  double total_time_s =
      std::chrono::duration<double>(overall_end - overall_start).count();

  bool ok = set_ok && get_ok;
  //------------------------------------------------------------------------------

  LatencyStats stats = ComputeLatencyStats(lat_us);
  double throughput_rps =
      total_time_s > 0 ? static_cast<double>(num_requests) / total_time_s : 0.0;

  for (auto& server : servers) {
    server->Stop();
  }
  for (auto& th : server_threads) {
    if (th.joinable()) {
      th.join();
    }
  }

  // if (!ok) {
  //   LOG(ERROR) << "Self-test failed set_ok=" << set_ok << " get_ok=" << get_ok
  //              << " set_resp=" << set_resp.value()
  //              << " get_resp=" << get_resp.value();
  //   return 1;
  // }
  // std::string mode_str = use_rdma ? (use_basic_ring ? "basic_ring" : "rdma") : "tcp";
  // LOG(ERROR) << "Self-test passed mode=" << mode_str
  //            << " (SET+GET verified, get_value=" << get_resp.value() << ")";
  // return 0;

  std::string mode_str =
      use_rdma ? (use_basic_ring ? "basic_ring" : "rdma") : "tcp";

  if (!ok) {
    LOG(ERROR) << "Self-test failed set_ok=" << set_ok
               << " get_ok=" << get_ok
               << " set_resp=" << set_resp.value()
               << " last_get_resp=" << last_get_resp.value();
    LOG(ERROR) << "RESULT"
               << " mode=" << mode_str
               << " num_requests=" << num_requests
               << " total_time_s=" << total_time_s
               << " throughput_rps=" << throughput_rps
               << " avg_us=" << stats.avg_us
               << " p50_us=" << stats.p50_us
               << " p95_us=" << stats.p95_us
               << " p99_us=" << stats.p99_us
               << " min_us=" << stats.min_us
               << " max_us=" << stats.max_us
               << " status=FAIL";
    return 1;
  }

  LOG(ERROR) << "Self-test passed mode=" << mode_str
             << " (GET verified, last_value=" << last_get_resp.value() << ")";

  LOG(ERROR) << "RESULT"
             << " mode=" << mode_str
             << " num_requests=" << num_requests
             << " total_time_s=" << total_time_s
             << " throughput_rps=" << throughput_rps
             << " avg_us=" << stats.avg_us
             << " p50_us=" << stats.p50_us
             << " p95_us=" << stats.p95_us
             << " p99_us=" << stats.p99_us
             << " min_us=" << stats.min_us
             << " max_us=" << stats.max_us
             << " status=OK";

  return 0;
}

}  // namespace resdb

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  FLAGS_minloglevel = google::GLOG_ERROR;

  // Backward-compatible one-command mode:
  //   <bin> --selftest [tcp|rdma|basic_ring] [ip] [base_port] [num_requests] [choice]
  //----------------------------------------------------------------------------
  // if (argc >= 3 && std::string(argv[1]) == "--selftest" &&
  //     std::string(argv[2]).rfind("-", 0) != 0) {
  //   const std::string mode = (argc >= 3) ? argv[2] : "tcp";
  //   const std::string choice = (argc >= 6) ? argv[5] : "shared";
  //   const bool use_rdma = (mode == "rdma" || mode == "basic_ring");
  //   const bool use_basic_ring =
  //       (mode == "basic_ring" || choice == "per_client");
  //   const std::string ip = (argc >= 4) ? argv[3] : "127.0.0.1";
  //   const int base_port = (argc >= 5) ? atoi(argv[4]) : 23000;
  //   return resdb::RunSelfTest(use_rdma, ip, base_port, use_basic_ring);
  // }
  //----------------------------------------------------------------------------

  if (argc >= 3 && std::string(argv[1]) == "--selftest" &&
      std::string(argv[2]).rfind("-", 0) != 0) {
    const std::string mode = (argc >= 3) ? argv[2] : "tcp";
    const std::string ip = (argc >= 4) ? argv[3] : "127.0.0.1";
    const int base_port = (argc >= 5) ? atoi(argv[4]) : 23000;
    const int num_requests = (argc >= 6) ? atoi(argv[5]) : 2;
    const std::string choice = (argc >= 7) ? argv[6] : "shared";

    const bool use_rdma = (mode == "rdma" || mode == "basic_ring");
    const bool use_basic_ring =
        (mode == "basic_ring" || choice == "per_client");

    return resdb::RunSelfTest(use_rdma, ip, base_port, use_basic_ring,
                              num_requests);
  }

  cxxopts::Options options(argv[0], "PBFT consensus server");
  options.positional_help("<server.config> <private_key> <cert_file>");
  options.add_options()("selftest", "Run in self-test mode",
                        cxxopts::value<bool>()->default_value("false"))(
      "mode", "Self-test mode: tcp|rdma|basic_ring",
      cxxopts::value<std::string>()->default_value("tcp"))(
      "choice", "RDMA ring mode: shared|per_client (default: shared)",
      cxxopts::value<std::string>()->default_value("shared"))(
      "ip", "Self-test bind IP",
      cxxopts::value<std::string>()->default_value("127.0.0.1"))(
      "num_requests", "Number of measured GET requests in self-test",
      cxxopts::value<int>()->default_value("2"))(
      "base_port", "Self-test base port",
      cxxopts::value<int>()->default_value("23000"))(
      "config", "Server config file", cxxopts::value<std::string>())(
      "private_key", "Private key file", cxxopts::value<std::string>())(
      "cert", "Certificate file", cxxopts::value<std::string>())(
      "h,help", "Show help");
  options.parse_positional({"config", "private_key", "cert"});

  std::string config_file;
  std::string private_key_file;
  std::string cert_file;
  try {
    auto parsed = options.parse(argc, argv);

    if (parsed.count("help")) {
      std::cout << options.help() << std::endl;
      return 0;
    }

    if (parsed["selftest"].as<bool>()) {
      const std::string mode = parsed["mode"].as<std::string>();
      const std::string choice = parsed["choice"].as<std::string>();
      int num_requests = parsed["num_requests"].as<int>();
      if (mode != "tcp" && mode != "rdma" && mode != "basic_ring") {
        LOG(ERROR) << "Invalid mode: " << mode
                   << ". Expected tcp or rdma.";
        return 1;
      }
      if (choice != "shared" && choice != "per_client" &&
          choice != "multi_client") {
        LOG(ERROR) << "Invalid choice: " << choice
                   << ". Expected shared or per_client.";
        return 1;
      }
      const bool use_rdma = (mode == "rdma" || mode == "basic_ring");
      const bool use_basic_ring =
          (mode == "basic_ring" || choice == "per_client");
      std::string ip = parsed["ip"].as<std::string>();
      int base_port = parsed["base_port"].as<int>();
      // Compatibility: allow
      //   --selftest --mode rdma --choice per_client <ip> <base_port>
      // by reusing positional slots when explicit --ip/--base_port are omitted.
      if (ip == "127.0.0.1" && parsed.count("config")) {
        ip = parsed["config"].as<std::string>();
      }
      if (base_port == 23000 && parsed.count("private_key")) {
        base_port = atoi(parsed["private_key"].as<std::string>().c_str());
      }
      if (num_requests == 2 && parsed.count("cert")) {
        num_requests = atoi(parsed["cert"].as<std::string>().c_str());
      }
      return resdb::RunSelfTest(use_rdma, ip, base_port, use_basic_ring, num_requests);
    }

    if (!parsed.count("config") || !parsed.count("private_key") ||
        !parsed.count("cert")) {
      std::cout << options.help() << std::endl;
      return 1;
    }

    config_file = parsed["config"].as<std::string>();
    private_key_file = parsed["private_key"].as<std::string>();
    cert_file = parsed["cert"].as<std::string>();
  } catch (const cxxopts::OptionException& e) {
    LOG(ERROR) << "CLI parse error: " << e.what();
    std::cout << options.help() << std::endl;
    return 1;
  }

  std::unique_ptr<resdb::ResDBConfig> config =
      resdb::GenerateResDBConfig(const_cast<char*>(config_file.c_str()),
                                 const_cast<char*>(private_key_file.c_str()),
                                 const_cast<char*>(cert_file.c_str()));

  config->SetHeartBeatEnabled(false);
  config->SetSignatureVerifierEnabled(false);

  auto consensus = std::make_unique<resdb::ConsensusManagerPBFT>(
      *config, std::make_unique<resdb::LightweightExecutor>());
  auto server =
      std::make_unique<resdb::ServiceNetwork>(*config, std::move(consensus));
  server->Run();
  return 0;
}
