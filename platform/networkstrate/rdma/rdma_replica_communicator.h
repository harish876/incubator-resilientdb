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

#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include "common/crypto/signature_verifier.h"
#include "platform/networkstrate/rdma/rdma_async_replica_client.h"
#include "platform/networkstrate/replica_communicator_interface.h"
#include "platform/proto/replica_info.pb.h"

namespace resdb {

// RDMA implementation of IReplicaCommunicator. Does not inherit from
// ReplicaCommunicator, avoiding Stats/thread startup from the TCP implementation.
class RdmaReplicaCommunicator : public IReplicaCommunicator {
 public:
  RdmaReplicaCommunicator(const std::vector<ReplicaInfo>& replicas,
                          SignatureVerifier* verifier = nullptr,
                          int rdma_port_offset = 20000);
  ~RdmaReplicaCommunicator() override;

  int SendHeartBeat(const Request& hb_info) override;
  int SendMessage(const google::protobuf::Message& message) override;
  int SendMessage(const google::protobuf::Message& message,
                  const ReplicaInfo& replica_info) override;
  void BroadCast(const google::protobuf::Message& message) override;
  void SendMessage(const google::protobuf::Message& message,
                   int64_t node_id) override;
  int SendBatchMessage(
      const std::vector<std::unique_ptr<Request>>& messages,
      const ReplicaInfo& replica_info) override;

  void UpdateClientReplicas(const std::vector<ReplicaInfo>& replicas) override;
  std::vector<ReplicaInfo> GetClientReplicas() override;

  // Establish control-plane connections to all replicas except self. Call
  // before data-plane traffic to avoid connection setup races in selftest.
  void EstablishControlPlaneConnections(int64_t self_id);

 private:
  RdmaAsyncReplicaClient* GetOrCreateClient(const std::string& ip, int port);
  int SendToReplica(const google::protobuf::Message& message,
                    const ReplicaInfo& replica_info);

  int rdma_port_offset_;
  std::vector<ReplicaInfo> replicas_;
  std::vector<ReplicaInfo> client_replicas_;
  SignatureVerifier* verifier_;
  std::map<std::pair<std::string, int>, std::unique_ptr<RdmaAsyncReplicaClient>>
      rdma_clients_;
  std::mutex clients_mutex_;
};

}  // namespace resdb
