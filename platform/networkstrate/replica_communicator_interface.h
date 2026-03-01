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

#include "platform/proto/replica_info.pb.h"
#include "platform/proto/resdb.pb.h"

namespace resdb {

// Interface for replica-to-replica communication.
// Implemented by ReplicaCommunicator (TCP) and RdmaReplicaCommunicator (RDMA).
// RdmaReplicaCommunicator implements this directly without inheriting from
// ReplicaCommunicator, avoiding Stats/thread startup.
class IReplicaCommunicator {
 public:
  virtual ~IReplicaCommunicator() = default;

  virtual int SendHeartBeat(const Request& hb_info) = 0;
  virtual int SendMessage(const google::protobuf::Message& message) = 0;
  virtual int SendMessage(const google::protobuf::Message& message,
                         const ReplicaInfo& replica_info) = 0;
  virtual void BroadCast(const google::protobuf::Message& message) = 0;
  virtual void SendMessage(const google::protobuf::Message& message,
                           int64_t node_id) = 0;
  virtual int SendBatchMessage(
      const std::vector<std::unique_ptr<Request>>& messages,
      const ReplicaInfo& replica_info) = 0;

  virtual void UpdateClientReplicas(const std::vector<ReplicaInfo>& replicas) = 0;
  virtual std::vector<ReplicaInfo> GetClientReplicas() = 0;
};

}  // namespace resdb
