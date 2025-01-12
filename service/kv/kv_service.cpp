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

#include <cstdlib>
#include <optional>
#include <ostream>

#include "chain/storage/memory_db.h"
#include "executor/kv/kv_executor.h"
#include "platform/config/resdb_config_utils.h"
#include "platform/statistic/stats.h"
#include "service/utils/server_factory.h"
// #ifdef ENABLE_LEVELDB
#include "chain/storage/leveldb.h"
#include "chain/storage/lmdb.h"
// #endif

using namespace resdb;
using namespace resdb::storage;

void ShowUsage() {
  printf("<config> <private_key> <cert_file> [logging_dir]\n");
}

std::unique_ptr<Storage> NewStorage(const std::string& db_path,
                                    const ResConfigData& config_data) {
  LOG(ERROR) << "use leveldb storage.";
  return NewResLevelDB(db_path, std::nullopt);
  // sending config_data as arg2 throws
  // type error. TODO investigate.

  // LOG(ERROR) << "use lmdb storage.";
  // return NewResLmdb(db_path);

  // LOG(ERROR) << "use memory storage.";
  // return NewMemoryDB();
}

void signal_handler(int signum) {
  std::cout << "Received signal: " << signum << ", exiting cleanly..."
            << std::endl;
  exit(0);
}

int main(int argc, char** argv) {
  if (argc < 4) {
    ShowUsage();
    exit(0);
  }
  // signal(SIGINT, signal_handler);
  google::InitGoogleLogging(argv[0]);
  // FLAGS_minloglevel = google::GLOG_INFO;
  //  INFO level doesnt work

  char* config_file = argv[1];
  char* private_key_file = argv[2];
  char* cert_file = argv[3];

  if (argc == 5) {
    std::string grafana_port = argv[4];
    std::string grafana_address = "0.0.0.0:" + grafana_port;

    auto monitor_port = Stats::GetGlobalStats(5);
    monitor_port->SetPrometheus(grafana_address);
    LOG(ERROR) << "monitoring prot:" << grafana_address;
  }

  std::unique_ptr<ResDBConfig> config =
      GenerateResDBConfig(config_file, private_key_file, cert_file);
  ResConfigData config_data = config->GetConfigData();

  // std::string db_path =
  //     "./" + std::to_string(config->GetSelfInfo().port()) + "_db.mdb";
  // LOG(ERROR) << "db path:" << db_path;

  std::string db_path = std::to_string(config->GetSelfInfo().port()) + "_db";
  LOG(ERROR) << "db path:" << db_path;

  auto server = GenerateResDBServer(
      config_file, private_key_file, cert_file,
      std::make_unique<KVExecutor>(NewStorage(db_path, config_data)), nullptr);
  server->Run();
}
