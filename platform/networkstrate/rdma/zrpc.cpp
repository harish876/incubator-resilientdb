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

#include "platform/networkstrate/rdma/zrpc.h"

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <infiniband/verbs.h>
#include <netinet/in.h>
#include <rdma/rdma_cma.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "ClientRDMA.hpp"
#include "ServerRDMA.hpp"
#include "VerbsEP.hpp"
#include "com/basic_ring.hpp"
#include "com/magic_ring.hpp"
#include "com/protocols.hpp"
#include "com/ring.hpp"
#include "com/utils.hpp"

namespace zrpc {
namespace {

constexpr uint32_t kRecvBatch = 16;
constexpr uint32_t kMaxSendWr = 128;
constexpr uint32_t kMaxRecvWr = 128;
constexpr uint32_t kExperimentCode = 6;
constexpr uint32_t kDefaultOutstanding = 8;
constexpr uint32_t kMailboxSize = sizeof(uint64_t) * 2;
constexpr uint32_t kLocalMemSize = sizeof(uint64_t) * 32;

std::string GetHostIpV4Impl() {
  ifaddrs* ifa = nullptr;
  if (getifaddrs(&ifa) != 0) return "";
  std::string ip;
  for (auto* p = ifa; p; p = p->ifa_next) {
    if (!p->ifa_addr || p->ifa_addr->sa_family != AF_INET) continue;
    if (std::strcmp(p->ifa_name, "lo") == 0) continue;
    char buf[INET_ADDRSTRLEN] = {};
    auto* sin = reinterpret_cast<sockaddr_in*>(p->ifa_addr);
    if (inet_ntop(AF_INET, &sin->sin_addr, buf, sizeof(buf))) {
      ip = buf;
      break;
    }
  }
  freeifaddrs(ifa);
  return ip;
}

struct Endpoint {
  VerbsEP* ep = nullptr;
  connect_info peer_info{};
};

uint32_t log2_pow2(uint32_t value) {
  uint32_t out = 0;
  while ((1u << out) < value) {
    ++out;
  }
  return out;
}

void PreparePayloadRegion(Region& send_region, const char* payload,
                          uint32_t payload_len) {
  if (payload_len > 0) {
    std::memcpy(send_region.addr, payload, payload_len);
  }
  send_region.length = payload_len;
}

void RpcSendBlocking(SharedCircularConnectionNotify& sender,
                     Region& send_region, const char* payload,
                     uint32_t payload_len) {
  PreparePayloadRegion(send_region, payload, payload_len);
  uint64_t send_id = sender.SendAsync(send_region);
  sender.WaitSend(send_id);
}

Endpoint AcceptEndpointWithRequest(ServerRDMA& server,
                                   struct rdma_cm_id* id, void* buf,
                                   const connect_info& local_info,
                                   struct ibv_qp_init_attr attr, uint32_t cid);
Endpoint ConnectEndpoint(struct rdma_cm_id* id, const connect_info& local_info,
                         struct ibv_qp_init_attr attr);

class ClientImpl {
 public:
  ClientImpl(const std::string& server_ip, int port,
             uint32_t max_outstanding = kDefaultOutstanding,
             uint32_t buffer_len = 65536, uint32_t max_payload = 65536)
      : ip_(server_ip),
        port_(port),
        max_outstanding_(max_outstanding),
        buffer_len_(buffer_len),
        max_payload_(max_payload) {
    ring_mem_ = static_cast<char*>(GetMagicBuffer(buffer_len_));
    if (!ring_mem_) {
      std::cerr << "Failed to allocate magic buffer" << std::endl;
      std::exit(1);
    }

    id_ = ClientRDMA::sendConnectRequest(const_cast<char*>(ip_.c_str()), port_);
    if (!id_) {
      std::cerr << "Failed to resolve address" << std::endl;
      std::exit(1);
    }
    if (!id_->pd) {
      id_->pd = ibv_alloc_pd(id_->verbs);
    }

    ring_mr_ = ibv_reg_mr(id_->pd, ring_mem_, buffer_len_ * 2,
                          IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                              IBV_ACCESS_REMOTE_READ);
    if (!ring_mr_) {
      std::cerr << "Failed to register ring MR" << std::endl;
      std::exit(1);
    }

    local_buffer_ = std::unique_ptr<MagicRingBuffer>(
        new MagicRingBuffer(ring_mr_, log2_pow2(buffer_len_), true));
    connect_info info{};
    info.code = kExperimentCode;
    info.ctx = local_buffer_->GetContext();

    struct ibv_qp_init_attr attr =
        prepare_qp(id_->pd, kMaxSendWr, kMaxRecvWr, false);
    endpoint_ = ConnectEndpoint(id_, info, attr);

    if (endpoint_.peer_info.ctx.length == 0) {
      std::cerr << "Missing server buffer info" << std::endl;
      std::exit(1);
    }

    remote_buffer_ = std::unique_ptr<MagicRemoteBuffer>(
        new MagicRemoteBuffer(endpoint_.peer_info.ctx));

    local_mem_ = static_cast<char*>(aligned_alloc(4096, kLocalMemSize));
    if (!local_mem_) {
      std::cerr << "Failed to allocate local memory" << std::endl;
      std::exit(1);
    }
    local_mr_ = ibv_reg_mr(id_->pd, local_mem_, kLocalMemSize,
                           IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                               IBV_ACCESS_REMOTE_READ);
    if (!local_mr_) {
      std::cerr << "Failed to register local MR" << std::endl;
      std::exit(1);
    }

    uint64_t rem_head = endpoint_.peer_info.addr_magic;
    uint32_t rem_head_rkey = endpoint_.peer_info.rkey_magic;
    uint64_t rem_win = rem_head + sizeof(uint64_t);
    uint32_t rem_win_rkey = endpoint_.peer_info.rkey_magic;

    if (endpoint_.peer_info.dm_rkey != 0) {
      uint32_t offset = 0;
      if (endpoint_.peer_info.addr_magic2 & 1) {
        rem_head = 0;
        rem_head_rkey = endpoint_.peer_info.dm_rkey;
        offset += sizeof(uint64_t);
      }
      if (endpoint_.peer_info.addr_magic2 & 2) {
        rem_win = offset;
        rem_win_rkey = endpoint_.peer_info.dm_rkey;
      }
    }

    sender_ = std::unique_ptr<SharedCircularConnectionNotify>(
        new SharedCircularConnectionNotify(
            endpoint_.ep, remote_buffer_.get(), rem_head, rem_head_rkey,
            rem_win, rem_win_rkey, reinterpret_cast<uint64_t>(local_mem_),
            local_mr_->lkey));

    if (max_outstanding_ == 0) {
      std::cerr << "Invalid max_outstanding" << std::endl;
      std::exit(1);
    }
    const uint32_t send_mem_len = max_outstanding_ * max_payload_;
    char* send_mem = static_cast<char*>(aligned_alloc(4096, send_mem_len));
    if (!send_mem) {
      std::cerr << "Failed to allocate send buffer" << std::endl;
      std::exit(1);
    }
    send_mr_ =
        ibv_reg_mr(id_->pd, send_mem, send_mem_len, IBV_ACCESS_LOCAL_WRITE);
    if (!send_mr_) {
      std::cerr << "Failed to register send MR" << std::endl;
      std::exit(1);
    }
    send_regions_.reserve(max_outstanding_);
    for (uint32_t i = 0; i < max_outstanding_; ++i) {
      send_regions_.push_back(
          {0, send_mem + (i * max_payload_), 0, send_mr_->lkey});
    }
  }

  void Send(const std::string& message) {
    uint32_t msg_len = static_cast<uint32_t>(message.size());
    if (msg_len > max_payload_) msg_len = max_payload_;
    Region& region = send_regions_[send_index_];
    RpcSendBlocking(*sender_, region, message.data(), msg_len);
    send_index_ = (send_index_ + 1) % max_outstanding_;
  }

  uint64_t SendAsync(const std::string& message) {
    uint32_t msg_len = static_cast<uint32_t>(message.size());
    if (msg_len > max_payload_) msg_len = max_payload_;
    while (outstanding_ >= max_outstanding_) {
      ProgressOnce();
    }
    Region& region = send_regions_[send_index_];
    send_index_ = (send_index_ + 1) % max_outstanding_;
    PreparePayloadRegion(region, message.data(), msg_len);
    uint64_t send_id = sender_->SendAsync(region);
    inflight_ids_.push_back(send_id);
    ++outstanding_;
    return send_id;
  }

  uint32_t ProgressOnce() {
    uint32_t completed = 0;
    while (!inflight_ids_.empty() && sender_->TestSend(inflight_ids_.front())) {
      inflight_ids_.pop_front();
      if (outstanding_ > 0) --outstanding_;
      ++completed;
    }
    return completed;
  }

  uint32_t Outstanding() const { return outstanding_; }
  uint32_t MaxOutstanding() const { return max_outstanding_; }
  uint32_t MaxPayload() const { return max_payload_; }

  ~ClientImpl() {
    if (endpoint_.ep) {
      rdma_disconnect(endpoint_.ep->id);
      delete endpoint_.ep;
      endpoint_.ep = nullptr;
    }
  }

 private:
  std::string ip_;
  int port_;
  uint32_t max_outstanding_;
  uint32_t buffer_len_;
  uint32_t max_payload_;
  char* ring_mem_ = nullptr;
  struct rdma_cm_id* id_ = nullptr;
  struct ibv_mr* ring_mr_ = nullptr;
  struct ibv_mr* send_mr_ = nullptr;
  char* local_mem_ = nullptr;
  struct ibv_mr* local_mr_ = nullptr;
  Endpoint endpoint_{};
  std::unique_ptr<MagicRingBuffer> local_buffer_{};
  std::unique_ptr<MagicRemoteBuffer> remote_buffer_{};
  std::unique_ptr<SharedCircularConnectionNotify> sender_{};
  std::vector<Region> send_regions_{};
  uint32_t send_index_ = 0;
  std::deque<uint64_t> inflight_ids_{};
  uint32_t outstanding_ = 0;
};

class MultiServerImpl {
 public:
  MultiServerImpl(int port, uint32_t max_outstanding, uint32_t max_clients,
                  uint32_t buffer_len = 65536, uint32_t max_payload = 65536)
      : port_(port),
        max_clients_(max_clients),
        buffer_len_(buffer_len),
        max_payload_(max_payload) {
    (void)max_outstanding;
    ip_ = GetHostIpV4Impl();
    if (ip_.empty()) {
      std::cerr << "Failed to determine host IP" << std::endl;
      std::exit(1);
    }

    server_ = std::unique_ptr<ServerRDMA>(
        new ServerRDMA(const_cast<char*>(ip_.c_str()), port_));
    attr_ = prepare_qp(server_->getPD(), kMaxSendWr, kMaxRecvWr, true);

    ring_mem_ = static_cast<char*>(GetMagicBuffer(buffer_len_));
    if (!ring_mem_) {
      std::cerr << "Failed to allocate magic buffer" << std::endl;
      std::exit(1);
    }
    ring_mr_ = ibv_reg_mr(server_->getPD(), ring_mem_, buffer_len_ * 2,
                          IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                              IBV_ACCESS_REMOTE_READ);
    if (!ring_mr_) {
      std::cerr << "Failed to register ring MR" << std::endl;
      std::exit(1);
    }
    local_buffer_ = std::unique_ptr<MagicRingBuffer>(
        new MagicRingBuffer(ring_mr_, log2_pow2(buffer_len_), true));

    mailbox_mem_ = static_cast<char*>(aligned_alloc(4096, kMailboxSize));
    if (!mailbox_mem_) {
      std::cerr << "Failed to allocate mailbox" << std::endl;
      std::exit(1);
    }
    mailbox_mr_ =
        ibv_reg_mr(server_->getPD(), mailbox_mem_, kMailboxSize,
                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                       IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC);
    if (!mailbox_mr_) {
      std::cerr << "Failed to register mailbox MR" << std::endl;
      std::exit(1);
    }

    info_.code = kExperimentCode;
    info_.ctx = local_buffer_->GetContext();
    info_.rkey_magic = mailbox_mr_->rkey;
    info_.addr_magic = reinterpret_cast<uint64_t>(mailbox_mem_);
    info_.addr_magic2 = reinterpret_cast<uint64_t>(mailbox_mem_);
    info_.dm_rkey = 0;

    endpoints_.resize(max_clients_);
    eps_.resize(max_clients_, nullptr);
  }

  bool AcceptNext() {
    std::pair<struct rdma_cm_id*, void*> req;
    do {
      req = server_->get_connect_request(&stop_, 100);
      if (stop_.load(std::memory_order_acquire)) return false;
    } while (!req.first);  // Timeout: retry. Stop: handled above.
    uint32_t cid = next_cid_.load(std::memory_order_relaxed);
    Endpoint endpoint =
        AcceptEndpointWithRequest(*server_, req.first, req.second, info_, attr_, cid);
    if (cid >= max_clients_) {
      std::cerr << "[Server] rejected client id " << cid << std::endl;
      rdma_disconnect(endpoint.ep->id);
      delete endpoint.ep;
      return true;
    }
    next_cid_.store(cid + 1, std::memory_order_relaxed);

    VerbsEP* old_ep = eps_[cid];
    if (old_ep) {
      rdma_disconnect(old_ep->id);
      delete old_ep;
    }
    endpoints_[cid] = endpoint;
    eps_[cid] = endpoint.ep;
    if (!old_ep) {
      connected_.fetch_add(1, std::memory_order_release);
    }
    std::cout << "[Server] accepted client " << cid << std::endl;
    return true;
  }

  void AcceptLoop() {
    while (AcceptNext()) {
    }
  }

  void PollReceivesAll(
      const std::function<void(uint32_t, const std::string&)>& handler) {
    if (!recv_cq_) return;

    struct ibv_wc wcs[16];
    int ret = ibv_poll_cq(recv_cq_, 16, wcs);
    for (int i = 0; i < ret; ++i) {
      if (wcs[i].opcode != IBV_WC_RECV_RDMA_WITH_IMM) continue;
      const uint32_t length = wcs[i].byte_len;
      const uint32_t offset = wcs[i].imm_data;
      char* payload = local_buffer_->GetReadPtr(offset);
      const uint32_t cid = static_cast<uint32_t>(wcs[i].wr_id);
      if (cid >= max_clients_) continue;
      if (cid >= connected_.load(std::memory_order_acquire)) continue;
      VerbsEP* ep = eps_[cid];
      if (!ep) continue;
      handler(cid, std::string(payload, payload + length));
      ep->post_empty_recvs(1);

      const uint64_t new_head = local_buffer_->FreeOrdered(payload, length);
      if (new_head != lhead_) {
        lhead_ = new_head;
        if (head_ptr_) *head_ptr_ = lhead_;
      }
    }
  }

  void Run(const std::function<void(uint32_t, const std::string&)>& handler) {
    std::cout << "[Server] Ready on " << ip_ << ":" << port_ << std::endl;
    accept_thread_ = std::thread([this]() { AcceptLoop(); });

    while (connected_.load(std::memory_order_acquire) == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    if (eps_[0]) {
      recv_cq_ = eps_[0]->qp->recv_cq;
      lhead_ = local_buffer_->Free(0);
      head_ptr_ = reinterpret_cast<volatile uint64_t*>(mailbox_mem_);
      faa_ptr_ =
          reinterpret_cast<volatile uint64_t*>(mailbox_mem_ + sizeof(uint64_t));
      if (head_ptr_) *head_ptr_ = lhead_;
      if (faa_ptr_) *faa_ptr_ = 0;
    }

    while (!stop_.load(std::memory_order_acquire)) {
      PollReceivesAll(handler);
    }
  }

  void Stop() {
    stop_.store(true, std::memory_order_release);
    if (accept_thread_.joinable()) accept_thread_.join();
    if (server_) server_->Stop();
  }

 private:
  std::atomic<bool> stop_{false};
  int port_;
  std::string ip_;
  uint32_t max_clients_;
  uint32_t buffer_len_;
  uint32_t max_payload_;
  struct ibv_qp_init_attr attr_{};
  std::unique_ptr<ServerRDMA> server_{};
  char* ring_mem_ = nullptr;
  struct ibv_mr* ring_mr_ = nullptr;
  char* mailbox_mem_ = nullptr;
  struct ibv_mr* mailbox_mr_ = nullptr;
  connect_info info_{};
  std::unique_ptr<MagicRingBuffer> local_buffer_{};
  struct ibv_cq* recv_cq_ = nullptr;
  volatile uint64_t* head_ptr_ = nullptr;
  volatile uint64_t* faa_ptr_ = nullptr;
  uint64_t lhead_ = 0;
  std::atomic<uint32_t> next_cid_{0};
  std::atomic<uint32_t> connected_{0};
  std::thread accept_thread_;
  std::vector<Endpoint> endpoints_{};
  std::vector<VerbsEP*> eps_{};
};

// ── BasicRingBuffer client implementation ─────────────────────────────────
class BasicClientImpl {
 public:
  BasicClientImpl(const std::string& server_ip, int port,
                  uint32_t max_outstanding = kDefaultOutstanding,
                  uint32_t buffer_len = 65536, uint32_t max_payload = 65536)
      : ip_(server_ip),
        port_(port),
        max_outstanding_(max_outstanding),
        buffer_len_(buffer_len),
        max_payload_(max_payload) {
    id_ = ClientRDMA::sendConnectRequest(const_cast<char*>(ip_.c_str()), port_);
    if (!id_) {
      std::cerr << "[BasicClient] Failed to resolve address" << std::endl;
      std::exit(1);
    }
    if (!id_->pd) {
      id_->pd = ibv_alloc_pd(id_->verbs);
    }

    const uint32_t send_mem_len = max_outstanding_ * max_payload_;
    char* send_mem = static_cast<char*>(aligned_alloc(4096, send_mem_len));
    if (!send_mem) {
      std::cerr << "[BasicClient] Failed to allocate send buffer" << std::endl;
      std::exit(1);
    }
    send_mr_ = ibv_reg_mr(id_->pd, send_mem, send_mem_len,
                          IBV_ACCESS_LOCAL_WRITE);
    if (!send_mr_) {
      std::cerr << "[BasicClient] Failed to register send MR" << std::endl;
      std::exit(1);
    }
    send_regions_.reserve(max_outstanding_);
    for (uint32_t i = 0; i < max_outstanding_; ++i) {
      send_regions_.push_back(
          {0, send_mem + i * max_payload_, 0, send_mr_->lkey});
    }

    connect_info local_info{};
    local_info.code = kExperimentCode;
    struct ibv_qp_init_attr attr =
        prepare_qp(id_->pd, kMaxSendWr, kMaxRecvWr, false);
    endpoint_ = ConnectEndpoint(id_, local_info, attr);

    if (endpoint_.peer_info.ctx.length == 0) {
      std::cerr << "[BasicClient] No buffer context from server" << std::endl;
      std::exit(1);
    }

    remote_buffer_ = std::unique_ptr<BasicRemoteBuffer>(
        new BasicRemoteBuffer(endpoint_.peer_info.ctx));
    sender_ = std::unique_ptr<CircularConnectionNotify>(
        new CircularConnectionNotify(endpoint_.ep, remote_buffer_.get()));
  }

  void Send(const std::string& message) {
    uint32_t msg_len = static_cast<uint32_t>(message.size());
    if (msg_len > max_payload_) msg_len = max_payload_;
    Region& region = send_regions_[send_index_];
    std::memcpy(region.addr, message.data(), msg_len);
    region.length = msg_len;
    uint64_t wrid = sender_->SendAsync(region);
    sender_->WaitSend(wrid);
    sender_->AckSentBytes(msg_len);
    send_index_ = (send_index_ + 1) % max_outstanding_;
  }

  uint64_t SendAsync(const std::string& message) {
    uint32_t msg_len = static_cast<uint32_t>(message.size());
    if (msg_len > max_payload_) msg_len = max_payload_;
    while (outstanding_ >= max_outstanding_) {
      ProgressOnce();
    }
    Region& region = send_regions_[send_index_];
    send_index_ = (send_index_ + 1) % max_outstanding_;
    std::memcpy(region.addr, message.data(), msg_len);
    region.length = msg_len;
    uint64_t wrid = sender_->SendAsync(region);
    inflight_.push_back({wrid, msg_len});
    ++outstanding_;
    return wrid;
  }

  uint32_t ProgressOnce() {
    uint32_t completed = 0;
    while (!inflight_.empty() &&
           sender_->TestSend(inflight_.front().first)) {
      sender_->AckSentBytes(inflight_.front().second);
      inflight_.pop_front();
      if (outstanding_ > 0) --outstanding_;
      ++completed;
    }
    return completed;
  }

  uint32_t Outstanding() const { return outstanding_; }
  uint32_t MaxOutstanding() const { return max_outstanding_; }
  uint32_t MaxPayload() const { return max_payload_; }

  ~BasicClientImpl() {
    if (endpoint_.ep) {
      rdma_disconnect(endpoint_.ep->id);
      delete endpoint_.ep;
      endpoint_.ep = nullptr;
    }
  }

 private:
  std::string ip_;
  int port_;
  uint32_t max_outstanding_;
  uint32_t buffer_len_;
  uint32_t max_payload_;
  struct rdma_cm_id* id_ = nullptr;
  struct ibv_mr* send_mr_ = nullptr;
  Endpoint endpoint_{};
  std::unique_ptr<BasicRemoteBuffer> remote_buffer_{};
  std::unique_ptr<CircularConnectionNotify> sender_{};
  std::vector<Region> send_regions_{};
  uint32_t send_index_ = 0;
  std::deque<std::pair<uint64_t, uint32_t>> inflight_{};
  uint32_t outstanding_ = 0;
};

// ── BasicRingBuffer server implementation ─────────────────────────────────
class BasicMultiServerImpl {
 public:
  BasicMultiServerImpl(int port, uint32_t max_outstanding, uint32_t max_clients,
                       uint32_t buffer_len = 65536, uint32_t max_payload = 65536)
      : port_(port),
        max_clients_(max_clients),
        buffer_len_(buffer_len),
        max_payload_(max_payload) {
    (void)max_outstanding;
    ip_ = GetHostIpV4Impl();
    if (ip_.empty()) {
      std::cerr << "[BasicServer] Failed to determine host IP" << std::endl;
      std::exit(1);
    }

    server_ = std::unique_ptr<ServerRDMA>(
        new ServerRDMA(const_cast<char*>(ip_.c_str()), port_));
    attr_ = prepare_qp(server_->getPD(), kMaxSendWr, kMaxRecvWr, false);
    recv_cq_ = attr_.recv_cq;

    // Pre-allocate per-client ring buffers.
    ring_mems_.resize(max_clients_, nullptr);
    ring_mrs_.resize(max_clients_, nullptr);
    rings_.resize(max_clients_);
    for (uint32_t i = 0; i < max_clients_; ++i) {
      ring_mems_[i] = static_cast<char*>(aligned_alloc(4096, buffer_len_));
      if (!ring_mems_[i]) {
        std::cerr << "[BasicServer] OOM ring " << i << std::endl;
        std::exit(1);
      }
      std::memset(ring_mems_[i], 0, buffer_len_);
      ring_mrs_[i] = ibv_reg_mr(
          server_->getPD(), ring_mems_[i], buffer_len_,
          IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
              IBV_ACCESS_REMOTE_READ);
      if (!ring_mrs_[i]) {
        std::cerr << "[BasicServer] MR failed " << i << std::endl;
        std::exit(1);
      }
      rings_[i] = std::unique_ptr<BasicRingBuffer>(
          new BasicRingBuffer(ring_mrs_[i], buffer_len_, /*with_zero=*/true));
    }

    endpoints_.resize(max_clients_);
    eps_.resize(max_clients_, nullptr);
  }

  bool AcceptNext() {
    std::pair<struct rdma_cm_id*, void*> req;
    do {
      req = server_->get_connect_request(&stop_, 100);
      if (stop_.load(std::memory_order_acquire)) return false;
    } while (!req.first);

    uint32_t cid = next_cid_.load(std::memory_order_relaxed);
    if (cid >= max_clients_) {
      std::cerr << "[BasicServer] rejected client id " << cid
                << " (max=" << max_clients_ << ")" << std::endl;
      return true;
    }

    connect_info info{};
    info.code = kExperimentCode;
    info.ctx = rings_[cid]->GetContext();

    Endpoint endpoint = AcceptEndpointWithRequest(
        *server_, req.first, req.second, info, attr_, cid);
    next_cid_.store(cid + 1, std::memory_order_relaxed);

    VerbsEP* old_ep = eps_[cid];
    if (old_ep) {
      rdma_disconnect(old_ep->id);
      delete old_ep;
    }
    endpoints_[cid] = endpoint;
    eps_[cid] = endpoint.ep;
    if (!old_ep) {
      connected_.fetch_add(1, std::memory_order_release);
    }
    std::cout << "[BasicServer] accepted client " << cid << std::endl;
    return true;
  }

  void AcceptLoop() {
    while (AcceptNext()) {
    }
  }

  void PollReceivesAll(
      const std::function<void(uint32_t, const std::string&)>& handler) {
    if (!recv_cq_) return;
    struct ibv_wc wcs[16];
    int ret = ibv_poll_cq(recv_cq_, 16, wcs);
    for (int i = 0; i < ret; ++i) {
      if (wcs[i].opcode != IBV_WC_RECV_RDMA_WITH_IMM) continue;
      const uint32_t len = wcs[i].byte_len;
      const uint32_t cid = static_cast<uint32_t>(wcs[i].wr_id);
      if (cid >= max_clients_) continue;
      if (cid >= connected_.load(std::memory_order_acquire)) continue;
      VerbsEP* ep = eps_[cid];
      if (!ep) continue;
      char* data = rings_[cid]->Read(len);
      handler(cid, std::string(data, data + len));
      ep->post_empty_recvs(1);
      rings_[cid]->Free(len);
    }
  }

  void Run(const std::function<void(uint32_t, const std::string&)>& handler) {
    std::cout << "[BasicServer] Ready on " << ip_ << ":" << port_ << std::endl;
    accept_thread_ = std::thread([this]() { AcceptLoop(); });

    while (connected_.load(std::memory_order_acquire) == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    while (!stop_.load(std::memory_order_acquire)) {
      PollReceivesAll(handler);
    }
  }

  void Stop() {
    stop_.store(true, std::memory_order_release);
    if (accept_thread_.joinable()) accept_thread_.join();
    if (server_) server_->Stop();
  }

 private:
  std::atomic<bool> stop_{false};
  int port_;
  std::string ip_;
  uint32_t max_clients_;
  uint32_t buffer_len_;
  uint32_t max_payload_;
  struct ibv_qp_init_attr attr_{};
  struct ibv_cq* recv_cq_ = nullptr;
  std::unique_ptr<ServerRDMA> server_{};
  std::vector<char*> ring_mems_{};
  std::vector<struct ibv_mr*> ring_mrs_{};
  std::vector<std::unique_ptr<BasicRingBuffer>> rings_{};
  std::atomic<uint32_t> next_cid_{0};
  std::atomic<uint32_t> connected_{0};
  std::thread accept_thread_;
  std::vector<Endpoint> endpoints_{};
  std::vector<VerbsEP*> eps_{};
};

Endpoint AcceptEndpointWithRequest(ServerRDMA& server,
                                   struct rdma_cm_id* id, void* buf,
                                   const connect_info& local_info,
                                   struct ibv_qp_init_attr attr, uint32_t cid) {
  if (!id) {
    std::cerr << "Failed to accept connection" << std::endl;
    std::exit(1);
  }

  connect_info peer_info{};
  if (buf) {
    std::memcpy(&peer_info, buf, sizeof(peer_info));
    std::free(buf);
  }
  id->context = reinterpret_cast<void*>(static_cast<uint64_t>(cid));

  uint32_t max_recv_size = attr.cap.max_recv_wr;
  if (attr.srq) attr.cap.max_recv_wr = 0;
  if (rdma_create_qp(id, server.getPD(), &attr)) {
    perror("rdma_create_qp");
    std::exit(1);
  }
  if (attr.srq) attr.cap.max_recv_wr = max_recv_size;

  VerbsEP* ep = new VerbsEP(id, attr, kRecvBatch, false);

  struct rdma_conn_param conn_param;
  std::memset(&conn_param, 0, sizeof(conn_param));
  conn_param.responder_resources = 16;
  conn_param.initiator_depth = 16;
  conn_param.retry_count = 3;
  conn_param.rnr_retry_count = 3;
  conn_param.private_data = &local_info;
  conn_param.private_data_len = sizeof(local_info);

  if (rdma_accept(id, &conn_param)) {
    perror("rdma_accept");
    std::exit(1);
  }

  struct rdma_cm_event* event = nullptr;
  while (true) {
    if (rdma_get_cm_event(id->channel, &event)) {
      perror("rdma_get_cm_event");
      std::exit(1);
    }
    if (event->event == RDMA_CM_EVENT_ESTABLISHED) {
      rdma_ack_cm_event(event);
      break;
    }
    if (event->event == RDMA_CM_EVENT_REJECTED ||
        event->event == RDMA_CM_EVENT_CONNECT_ERROR ||
        event->event == RDMA_CM_EVENT_UNREACHABLE) {
      std::cerr << "Connection failed on server" << std::endl;
      rdma_ack_cm_event(event);
      std::exit(1);
    }
    rdma_ack_cm_event(event);
  }
  Endpoint endpoint;
  endpoint.ep = ep;
  endpoint.peer_info = peer_info;
  return endpoint;
}

Endpoint ConnectEndpoint(struct rdma_cm_id* id, const connect_info& local_info,
                         struct ibv_qp_init_attr attr) {
  if (rdma_create_qp(id, id->pd, &attr)) {
    perror("rdma_create_qp");
    std::exit(1);
  }

  VerbsEP* ep = new VerbsEP(id, attr, kRecvBatch, false);

  struct rdma_conn_param conn_param;
  std::memset(&conn_param, 0, sizeof(conn_param));
  conn_param.responder_resources = 16;
  conn_param.initiator_depth = 16;
  conn_param.retry_count = 3;
  conn_param.rnr_retry_count = 3;
  conn_param.private_data = &local_info;
  conn_param.private_data_len = sizeof(local_info);

  if (rdma_connect(id, &conn_param)) {
    perror("rdma_connect");
    std::exit(1);
  }

  struct rdma_cm_event* event = nullptr;
  connect_info peer_info{};
  bool got_peer_info = false;
  while (true) {
    if (rdma_get_cm_event(id->channel, &event)) {
      perror("rdma_get_cm_event");
      std::exit(1);
    }
    if (event->param.conn.private_data_len > 0) {
      uint32_t copy_len = event->param.conn.private_data_len;
      if (copy_len > sizeof(peer_info)) copy_len = sizeof(peer_info);
      std::memcpy(&peer_info, event->param.conn.private_data, copy_len);
      got_peer_info = true;
    }
    if (event->event == RDMA_CM_EVENT_ESTABLISHED) {
      rdma_ack_cm_event(event);
      break;
    }
    if (event->event == RDMA_CM_EVENT_REJECTED ||
        event->event == RDMA_CM_EVENT_CONNECT_ERROR ||
        event->event == RDMA_CM_EVENT_UNREACHABLE) {
      std::cerr << "Connection failed on client, event=" << event->event
                << std::endl;
      rdma_ack_cm_event(event);
      std::exit(1);
    }
    rdma_ack_cm_event(event);
  }

  if (!got_peer_info) {
    std::cerr << "Missing server buffer info" << std::endl;
    std::exit(1);
  }
  Endpoint endpoint;
  endpoint.ep = ep;
  endpoint.peer_info = peer_info;
  return endpoint;
}

}  // namespace

std::string GetHostIpV4() {
  std::string ip = GetHostIpV4Impl();
  return ip.empty() ? "127.0.0.1" : ip;
}

struct Client::Impl {
  bool use_basic;
  std::unique_ptr<ClientImpl> magic;
  std::unique_ptr<BasicClientImpl> basic;
  Impl(const std::string& ip, int port, uint32_t max_out,
       uint32_t buffer_len, uint32_t max_payload, bool use_basic_ring)
      : use_basic(use_basic_ring) {
    if (use_basic_ring) {
      basic = std::unique_ptr<BasicClientImpl>(
          new BasicClientImpl(ip, port, max_out, buffer_len, max_payload));
    } else {
      magic = std::unique_ptr<ClientImpl>(
          new ClientImpl(ip, port, max_out, buffer_len, max_payload));
    }
  }
};

struct MultiServer::Impl {
  bool use_basic;
  std::unique_ptr<MultiServerImpl> magic;
  std::unique_ptr<BasicMultiServerImpl> basic;
  Impl(int port, uint32_t max_out, uint32_t max_clients,
       uint32_t buffer_len, uint32_t max_payload, bool use_basic_ring)
      : use_basic(use_basic_ring) {
    if (use_basic_ring) {
      basic = std::unique_ptr<BasicMultiServerImpl>(
          new BasicMultiServerImpl(port, max_out, max_clients, buffer_len,
                                   max_payload));
    } else {
      magic = std::unique_ptr<MultiServerImpl>(
          new MultiServerImpl(port, max_out, max_clients, buffer_len,
                              max_payload));
    }
  }
};

Client::Client(const std::string& server_ip, int port,
               uint32_t max_outstanding, uint32_t buffer_len,
               uint32_t max_payload, bool use_basic_ring)
    : impl_(new Impl(server_ip, port, max_outstanding, buffer_len,
                    max_payload, use_basic_ring)) {}

Client::~Client() {
  delete impl_;
}

void Client::Send(const std::string& message) {
  if (impl_->use_basic) {
    impl_->basic->Send(message);
  } else {
    impl_->magic->Send(message);
  }
}

uint64_t Client::SendAsync(const std::string& message) {
  if (impl_->use_basic) {
    return impl_->basic->SendAsync(message);
  }
  return impl_->magic->SendAsync(message);
}

uint32_t Client::ProgressOnce() {
  if (impl_->use_basic) {
    return impl_->basic->ProgressOnce();
  }
  return impl_->magic->ProgressOnce();
}

void Client::Progress() {
  if (impl_->use_basic) {
    impl_->basic->ProgressOnce();
  } else {
    impl_->magic->ProgressOnce();
  }
}

uint32_t Client::Outstanding() const {
  if (impl_->use_basic) {
    return impl_->basic->Outstanding();
  }
  return impl_->magic->Outstanding();
}

uint32_t Client::MaxOutstanding() const {
  if (impl_->use_basic) {
    return impl_->basic->MaxOutstanding();
  }
  return impl_->magic->MaxOutstanding();
}

uint32_t Client::MaxPayload() const {
  if (impl_->use_basic) {
    return impl_->basic->MaxPayload();
  }
  return impl_->magic->MaxPayload();
}

MultiServer::MultiServer(int port, uint32_t max_outstanding,
                         uint32_t max_clients, uint32_t buffer_len,
                         uint32_t max_payload, bool use_basic_ring)
    : impl_(new Impl(port, max_outstanding, max_clients, buffer_len,
                    max_payload, use_basic_ring)) {}

MultiServer::~MultiServer() {
  delete impl_;
}

void MultiServer::Run(
    const std::function<void(uint32_t, const std::string&)>& handler) {
  if (impl_->use_basic) {
    impl_->basic->Run(handler);
  } else {
    impl_->magic->Run(handler);
  }
}

void MultiServer::Stop() {
  if (impl_->use_basic) {
    impl_->basic->Stop();
  } else {
    impl_->magic->Stop();
  }
}

}  // namespace zrpc
