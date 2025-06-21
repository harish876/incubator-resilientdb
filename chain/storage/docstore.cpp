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

#include "docstore.h"

#include <glog/logging.h>
#include <unistd.h>

#include <memory>
#include <nlohmann/json.hpp>

namespace resdb {
namespace storage {

std::unique_ptr<Storage> NewResDocstoreDB(const std::string& path,
                                          std::optional<LevelDBInfo> config) {
  if (config == std::nullopt) {
    config = LevelDBInfo();
  }
  (*config).set_path(path);
  leveldb::Status s;
  return std::make_unique<ResDocstoreDB>(config);
}

std::unique_ptr<Storage> NewResDocstoreDB(std::optional<LevelDBInfo> config) {
  return std::make_unique<ResDocstoreDB>(config);
}

ResDocstoreDB::ResDocstoreDB(std::optional<LevelDBInfo> config) {
  std::string path = "/tmp/nexres-leveldb";
  if (config.has_value()) {
    if (!(*config).path().empty()) {
      LOG(ERROR) << "Custom path for ResLevelDB provided in config: "
                 << (*config).path();
      path = (*config).path();
    }
  }
  global_stats_ = Stats::GetGlobalStats();
  CreateDB(path);
}

void ResDocstoreDB::CreateDB(const std::string& path) {
  LOG(ERROR) << "ResLevelDB Create DB: path:" << path << "\n";
  leveldb::Status s;
  store_ = std::make_unique<docstore::DocumentStore>(path, s);
  assert(s.ok());
  LOG(ERROR) << "Successfully opened LevelDB";
}

ResDocstoreDB::~ResDocstoreDB() {}

/**
 * SetValue is going to be a TopLevel Function to handle any POST queries to our
 * collection store This could be CreateCollection This could be Insert
 * We assume here that key is the action and value is a serialized JSON
 * representing the object.
 */
int ResDocstoreDB::SetValue(const std::string& key,
                            const std::string& serialized_document) {
  leveldb::Status s;
  if (key == "CREATE_COLLECTION") {
    handle_create_collection(serialized_document, s);
  } else if (key == "INSERT") {
    handle_insert(serialized_document, s);
  } else {
    LOG(ERROR) << "Normal kv store case not handled" << key << s.ToString()
               << " \n";
    return -1;
    // handle_default_kv_store(key, serialized_document, s);
  }
  if (s.ok()) {
    return 0;
  } else {
    LOG(ERROR) << "Error at SetValue" << key << s.ToString() << " \n";
    return -1;
  }
}

void ResDocstoreDB::handle_create_collection(
    const std::string& serialized_document, leveldb::Status& s) {
  validate_collection_payload(serialized_document, s);
  if (!s.ok()) {
    return;
  }

  auto document = parse_document_from_request(serialized_document);
  leveldb::Options options;
  options = options.FromJSON(document->at("options"), s);
  if (!s.ok()) {
    return;
  }

  s = store_->CreateCollection(document->at("collection_name"), options,
                               document->at("schema"));
  if (!s.ok()) {
    return;
  }
  LOG(ERROR) << "Successfully created collection "
             << document->at("collection_name") << "\n";
}

void ResDocstoreDB::handle_insert(const std::string& serialized_document,
                                  leveldb::Status& s) {
  validate_insert_payload(serialized_document, s);
  if (!s.ok()) {
    return;
  }

  auto document = parse_document_from_request(serialized_document);
  s = store_->Insert(document->at("collection_name"), document->at("value"));

  if (!s.ok()) {
    LOG(ERROR) << "Invalid Document Schema " << s.ToString() << " "
               << document->dump(1, ' ') << "\n";
    return;
  }
  LOG(ERROR) << "Successfully inserted into collection "
             << document->at("collection_name")
             << document->at("value").dump(1, ' ') << "\n";
  return;
}

void ResDocstoreDB::handle_default_kv_store(
    const std::string& key, const std::string& serialized_document,
    leveldb::Status& s) {
  return;
}

std::string ResDocstoreDB::GetValue(const std::string& key) {
  LOG(ERROR) << "GetValue" << key << "\n";
  nlohmann::json request;
  auto result = nlohmann::json::parse(key, nullptr, true);
  const std::string& collection_name = result.at("collection_name");
  const std::string& filter_key = result.at("filter_key");

  nlohmann::json metadata;
  leveldb::Status s =
      store_->CheckCollectionInRegistry(collection_name, metadata);

  if (!s.ok()) {
    return "Collection not found";
  }

  if (!metadata.contains("options")) {
    LOG(ERROR) << "Metadata missing 'options' field";
    return "[]";
  }

  if (!metadata["options"].contains("secondary_key") ||
      !metadata["options"].contains("primary_key")) {
    LOG(ERROR)
        << "'secondary_key' or 'primary_key' field not found in metadata";
    return "[]";
  }

  const std::string& secondary_key = metadata.at("options").at("secondary_key");
  const std::string& primary_key = metadata.at("options").at("primary_key");

  std::string filter_value;
  if (result.at("filter_value").is_string()) {
    filter_value = result.at("filter_value").get<std::string>();
  } else if (result.at("filter_value").is_number()) {
    filter_value = std::to_string(result.at("filter_value").get<int64_t>());
  } else {
    LOG(ERROR) << "filter_value must be either string or number";
    return "[]";
  }

  if (secondary_key == filter_key) {
    std::vector<leveldb::SecondayKeyReturnVal> acc;
    leveldb::Status s =
        store_->GetSec(collection_name, filter_value, &acc, 1000);
    if (!s.ok()) {
      LOG(ERROR) << "Error at GetSec" << s.ToString() << " " << filter_value
                 << "\n";
      return "[]";
    }
    std::string values = "[";
    bool first_iteration = true;
    for (const auto& val : acc) {
      if (!first_iteration) values.append(",");
      first_iteration = false;
      values.append(val.value);
    }
    values.append("]");
    return values;

    return "[]";
  } else if (primary_key == filter_key) {
    std::string value;
    leveldb::Status s = store_->Get(collection_name, filter_value, value);
    if (!s.ok()) {
      LOG(ERROR) << "Error at GetSec" << s.ToString() << " " << filter_value
                 << "\n";
      return "[]";
    }
    return value;
  } else {
    return "Not implemented";
  }
}

std::string ResDocstoreDB::GetAllValues(void) {
  // Get All values from the database
  std::vector<std::string> values;
  throw std::logic_error("Function not implemented");
}

std::map<std::string, std::pair<std::string, int>>
ResDocstoreDB::GetAllItems() {
  throw std::logic_error("Function not implemented");
}

// TODO: Figure out a way to call this directly from the API layer.
std::string ResDocstoreDB::GetRange(const std::string& key,
                                    const std::string& dummy_key) {
  LOG(ERROR) << "GetRange" << key << "\n";
  nlohmann::json request;
  auto result = nlohmann::json::parse(key, nullptr, true);
  const std::string& collection_name = result.at("collection_name");
  const std::string& filter_key = result.at("filter_key");

  nlohmann::json metadata;
  leveldb::Status s =
      store_->CheckCollectionInRegistry(collection_name, metadata);

  if (!s.ok()) {
    return "Collection not found";
  }

  if (!metadata.contains("options")) {
    LOG(ERROR) << "Metadata missing 'options' field";
    return "[]";
  }

  if (!metadata["options"].contains("secondary_key") ||
      !metadata["options"].contains("primary_key")) {
    LOG(ERROR)
        << "'secondary_key' or 'primary_key' field not found in metadata";
    return "[]";
  }

  const std::string& secondary_key = metadata.at("options").at("secondary_key");
  const std::string& primary_key = metadata.at("options").at("primary_key");

  std::string filter_value_from;
  if (result.at("filter_value_from").is_string()) {
    filter_value_from = result.at("filter_value_from").get<std::string>();
  } else if (result.at("filter_value_from").is_number()) {
    filter_value_from =
        std::to_string(result.at("filter_value_from").get<int64_t>());
  } else {
    LOG(ERROR) << "filter_value_from must be either string or number";
    return "[]";
  }

  std::string filter_value_to;
  if (result.at("filter_value_to").is_string()) {
    filter_value_to = result.at("filter_value_to").get<std::string>();
  } else if (result.at("filter_value_to").is_number()) {
    filter_value_to =
        std::to_string(result.at("filter_value_to").get<int64_t>());
  } else {
    LOG(ERROR) << "filter_value_to must be either string or number";
    return "[]";
  }

  if (secondary_key == filter_key) {
    std::vector<leveldb::SecondayKeyReturnVal> acc;
    leveldb::Status s = store_->RangeGetSec(collection_name, filter_value_from,
                                            filter_value_to, &acc, 1000);
    if (!s.ok()) {
      LOG(ERROR) << "Error at RangeGetSec" << s.ToString() << " "
                 << filter_value_from << " " << filter_value_to << "\n";
      return "[]";
    }
    std::string values = "[";
    bool first_iteration = true;
    for (const auto& val : acc) {
      if (!first_iteration) values.append(",");
      first_iteration = false;
      values.append(val.value);
    }
    values.append("]");
    return values;

    return "[]";
  } else if (primary_key == filter_key) {
    vector<nlohmann::json> acc;
    leveldb::Status s = store_->GetRange(collection_name, filter_value_from,
                                         filter_value_to, acc);
    if (!s.ok()) {
      LOG(ERROR) << "Error at RangeGetSec" << s.ToString() << " "
                 << filter_value_from << " " << filter_value_to << "\n";
      return "[]";
    }

    std::string values = "[";
    bool first_iteration = true;
    for (const auto& doc : acc) {
      if (!first_iteration) values.append(",");
      first_iteration = false;
      values.append(doc.dump(1, ' '));
    }
    values.append("]");
    return values;
  } else {
    return "Not implemented";
  }
}

bool ResDocstoreDB::UpdateMetrics() { return false; }

bool ResDocstoreDB::Flush() { return false; }

int ResDocstoreDB::SetValueWithVersion(const std::string& key,
                                       const std::string& value, int version) {
  throw std::logic_error("Function not implemented");
}

std::pair<std::string, int> ResDocstoreDB::GetValueWithVersion(
    const std::string& key, int version) {
  throw std::logic_error("Function not implemented");
}

std::map<std::string, std::pair<std::string, int>> ResDocstoreDB::GetKeyRange(
    const std::string& min_key, const std::string& max_key) {
  throw std::logic_error("Function not implemented");
}

// Return a list of <value, version>
std::vector<std::pair<std::string, int>> ResDocstoreDB::GetHistory(
    const std::string& key, int min_version, int max_version) {
  throw std::logic_error("Function not implemented");
}

// Return a list of <value, version>
std::vector<std::pair<std::string, int>> ResDocstoreDB::GetTopHistory(
    const std::string& key, int top_number) {
  throw std::logic_error("Function not implemented");
}

void ResDocstoreDB::validate_collection_payload(
    const std::string& serialized_document, leveldb::Status& s) {
  auto document = parse_document_from_request(serialized_document);
  if (!document) {
    s = leveldb::Status::InvalidArgument("Failed to parse JSON document");
    return;
  }

  if (!document->contains("collection_name")) {
    s = leveldb::Status::InvalidArgument(
        "Invalid JSON object at handle_create_collection. CreateCollection"
        "Does not contain collection_name string");
    return;
  }

  if (!document->contains("schema")) {
    s = leveldb::Status::InvalidArgument(
        "Invalid JSON object at handle_create_collection. CreateCollection"
        "Does not contain schema object");
    return;
  }

  if (!document->contains("options")) {
    s = leveldb::Status::InvalidArgument(
        "Invalid JSON object at handle_create_collection. CreateCollection"
        "Does not contain options object");
    return;
  }
}

void ResDocstoreDB::validate_insert_payload(
    const std::string& serialized_document, leveldb::Status& s) {
  auto document = parse_document_from_request(serialized_document);
  if (!document) {
    s = leveldb::Status::InvalidArgument(
        "Invalid JSON object at handle_insert");
    return;
  }
  if (!document->contains("collection_name")) {
    s = leveldb::Status::InvalidArgument(
        "Invalid JSON object at handle_insert. Insert"
        "Does not contain collection_name string");
    return;
  }
  if (!document->contains("value")) {
    s = leveldb::Status::InvalidArgument(
        "Invalid JSON object at handle_insert. Insert"
        "Does not contain value object");
    return;
  }
}

}  // namespace storage
}  // namespace resdb
