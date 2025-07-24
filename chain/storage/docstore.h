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

#include <nlohmann/json.hpp>
#include <optional>
#include <string>

#include "chain/storage/proto/leveldb_config.pb.h"
#include "chain/storage/storage.h"
#include "common/lru/lru_cache.h"
#include "docstore/document.h"
#include "platform/statistic/stats.h"

namespace resdb {
namespace storage {

std::unique_ptr<Storage> NewResDocstoreDB(
    const std::string& path, std::optional<LevelDBInfo> config = std::nullopt);
std::unique_ptr<Storage> NewResDocstoreDB(
    std::optional<LevelDBInfo> config = std::nullopt);

class ResDocstoreDB : public Storage {
 public:
  ResDocstoreDB(std::optional<LevelDBInfo> config_data = std::nullopt);

  virtual ~ResDocstoreDB();
  int SetValue(const std::string& key, const std::string& value) override;
  std::string GetValue(const std::string& key) override;
  std::string GetAllValues(void) override;
  std::string GetRange(const std::string& min_key,
                       const std::string& max_key) override;

  int SetValueWithVersion(const std::string& key, const std::string& value,
                          int version) override;
  std::pair<std::string, int> GetValueWithVersion(const std::string& key,
                                                  int version) override;

  // Return a map of <key, <value, version>>
  std::map<std::string, std::pair<std::string, int>> GetAllItems() override;
  std::map<std::string, std::pair<std::string, int>> GetKeyRange(
      const std::string& min_key, const std::string& max_key) override;

  // Return a list of <value, version>
  std::vector<std::pair<std::string, int>> GetHistory(const std::string& key,
                                                      int min_version,
                                                      int max_version) override;

  std::vector<std::pair<std::string, int>> GetTopHistory(
      const std::string& key, int top_number) override;

  bool UpdateMetrics();

  bool Flush() override;

 private:
  void CreateDB(const std::string& path);

 private:
  std::unique_ptr<docstore::DocumentStore> store_;

 protected:
  Stats* global_stats_ = nullptr;
  // TODO: move to docstore storage engine
  std::optional<nlohmann::json> parse_document_from_request(
      const std::string& serialized_json) {
    try {
      // Attempt to parse the JSON and return the parsed document
      nlohmann::json raw_value = nlohmann::json::parse(serialized_json);
      if (!raw_value.contains("value")) {
        return std::nullopt;
      }
      return raw_value["value"];
    } catch (const nlohmann::json::parse_error& e) {
      // If parsing fails, return an empty optional
      return std::nullopt;
    }
  }

  void handle_create_collection(const std::string&, leveldb::Status&);
  void handle_insert(const std::string&, leveldb::Status&);
  void handle_default_kv_store(const std::string& key, const std::string& value,
                               leveldb::Status&);
  void validate_collection_payload(const std::string& serialized_document,
                                   leveldb::Status& s);
  void validate_insert_payload(const std::string& serialized_document,
                               leveldb::Status& s);
};

}  // namespace storage
}  // namespace resdb
