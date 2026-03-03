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

#include "platform/networkstrate/rdma/rdma_replica_communicator.h"

#include <glog/logging.h>

#include "interface/rdbc/net_channel.h"
#include "platform/proto/broadcast.pb.h"

namespace resdb {

RdmaReplicaCommunicator::RdmaReplicaCommunicator(
    const std::vector<ReplicaInfo>& replicas, SignatureVerifier* verifier,
    int rdma_port_offset, bool use_basic_ring)
    : rdma_port_offset_(rdma_port_offset),
      use_basic_ring_(use_basic_ring),
      replicas_(replicas),
      verifier_(verifier) {}

RdmaReplicaCommunicator::~RdmaReplicaCommunicator() {}

RdmaAsyncReplicaClient* RdmaReplicaCommunicator::GetOrCreateClient(
    const std::string& ip, int port) {
  int rdma_port = port + rdma_port_offset_;
  auto key = std::make_pair(ip, rdma_port);
  std::lock_guard<std::mutex> lock(clients_mutex_);
  if (rdma_clients_.find(key) == rdma_clients_.end()) {
    LOG(ERROR) << "[RDMA] create client ip=" << ip
               << " base_port=" << port
               << " rdma_port=" << rdma_port
               << " offset=" << rdma_port_offset_;
    rdma_clients_[key] = std::make_unique<RdmaAsyncReplicaClient>(
        ip, rdma_port, 8, 65536, 65536, use_basic_ring_);
  }
  return rdma_clients_[key].get();
}

int RdmaReplicaCommunicator::SendToReplica(
    const google::protobuf::Message& message,
    const ReplicaInfo& replica_info) {
  if (!replica_info.ip().size() || !replica_info.port()) {
    LOG(ERROR) << "[RDMA] invalid target ip/port. id=" << replica_info.id()
               << " ip='" << replica_info.ip()
               << "' port=" << replica_info.port();
    return -1;
  }
  const int rdma_port = replica_info.port() + rdma_port_offset_;
  std::string raw =
      NetChannel::GetRawMessageString(message, verifier_);
  BroadcastData broadcast_data;
  broadcast_data.add_data()->assign(raw);
  std::string data;
  if (!broadcast_data.SerializeToString(&data)) {
    LOG(ERROR) << "[RDMA] BroadcastData serialize failed for id="
               << replica_info.id();
    return -1;
  }
  LOG(ERROR) << "[RDMA] send target id=" << replica_info.id()
             << " ip=" << replica_info.ip()
             << " base_port=" << replica_info.port()
             << " rdma_port=" << rdma_port
             << " payload_bytes=" << data.size();
  auto* client = GetOrCreateClient(replica_info.ip(), replica_info.port());
  int ret = client->SendMessage(data, false);
  if (ret != 0) {
    LOG(ERROR) << "[RDMA] send failed target id=" << replica_info.id()
               << " ip=" << replica_info.ip()
               << " rdma_port=" << rdma_port
               << " ret=" << ret;
  }
  return ret;
}

void RdmaReplicaCommunicator::UpdateClientReplicas(
    const std::vector<ReplicaInfo>& replicas) {
  client_replicas_ = replicas;
}

void RdmaReplicaCommunicator::EstablishControlPlaneConnections(int64_t self_id) {
  for (const auto& replica : replicas_) {
    if (!replica.ip().empty() && replica.port()) {
      GetOrCreateClient(replica.ip(), replica.port());
    }
  }
}

std::vector<ReplicaInfo> RdmaReplicaCommunicator::GetClientReplicas() {
  return client_replicas_;
}

int RdmaReplicaCommunicator::SendHeartBeat(const Request& hb_info) {
  int ret = 0;
  std::vector<ReplicaInfo> targets = replicas_;
  for (const auto& client : client_replicas_) {
    targets.push_back(client);
  }
  LOG(ERROR) << "[RDMA] heartbeat sender=" << hb_info.sender_id()
             << " targets=" << targets.size();

  for (const auto& replica : targets) {
    if (SendToReplica(hb_info, replica) == 0) {
      ret++;
    }
  }
  return ret;
}

int RdmaReplicaCommunicator::SendMessage(
    const google::protobuf::Message& message) {
  int ret = 0;
  int64_t sender_id = 0;
  if (const auto* request = dynamic_cast<const Request*>(&message)) {
    sender_id = request->sender_id();
  }

  for (const auto& replica : replicas_) {
    if (SendToReplica(message, replica) == 0) {
      ret++;
    }
  }
  return ret;
}

int RdmaReplicaCommunicator::SendMessage(
    const google::protobuf::Message& message,
    const ReplicaInfo& replica_info) {
  return SendToReplica(message, replica_info);
}

void RdmaReplicaCommunicator::BroadCast(
    const google::protobuf::Message& message) {
  int ret = SendMessage(message);
  if (ret < 0) {
    LOG(ERROR) << "RDMA broadcast failed";
  }
}

void RdmaReplicaCommunicator::SendMessage(
    const google::protobuf::Message& message, int64_t node_id) {
  ReplicaInfo target;
  for (const auto& r : replicas_) {
    if (r.id() == node_id) {
      target = r;
      break;
    }
  }
  if (target.ip().empty()) {
    for (const auto& r : GetClientReplicas()) {
      if (r.id() == node_id) {
        target = r;
        break;
      }
    }
  }
  if (target.ip().empty()) {
    return;
  }
  int ret = SendToReplica(message, target);
  if (ret < 0) {
    LOG(ERROR) << "RDMA SendMessage to node " << node_id << " failed";
  }
}

int RdmaReplicaCommunicator::SendBatchMessage(
    const std::vector<std::unique_ptr<Request>>& messages,
    const ReplicaInfo& replica_info) {
  int ret = 0;
  for (const auto& msg : messages) {
    if (SendToReplica(*msg, replica_info) == 0) {
      ret++;
    }
  }
  return ret;
}

}  // namespace resdb
