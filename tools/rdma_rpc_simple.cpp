#include <arpa/inet.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <infiniband/verbs.h>
#include <netinet/in.h>
#include <rdma/rdma_cma.h>

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
#include <vector>

#include "ClientRDMA.hpp"
#include "ServerRDMA.hpp"
#include "VerbsEP.hpp"
#include "com/magic_ring.hpp"
#include "com/protocols.hpp"
#include "com/ring.hpp"
#include "com/utils.hpp"

namespace {

constexpr uint32_t kBufferLen = 4096;
constexpr uint32_t kRecvBatch = 16;
constexpr uint32_t kMaxSendWr = 128;
constexpr uint32_t kMaxRecvWr = 128;
constexpr uint32_t kExperimentCode = 3;
constexpr uint32_t kMaxPayload = 4096 - sizeof(uint32_t);
constexpr uint32_t kSendSlotSize = kMaxPayload + sizeof(uint32_t);

std::string GetHostIpV4() {
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

Endpoint AcceptEndpoint(ServerRDMA& server, const connect_info& local_info,
                        struct ibv_qp_init_attr attr);
Endpoint ConnectEndpoint(struct rdma_cm_id* id, const connect_info& local_info,
                         struct ibv_qp_init_attr attr);

uint32_t log2_pow2(uint32_t value) {
  uint32_t out = 0;
  while ((1u << out) < value) {
    ++out;
  }
  return out;
}

void PrepareSendRegion(Region& send_region, uint32_t freed_bytes,
                       const char* payload, uint32_t payload_len) {
  *reinterpret_cast<uint32_t*>(send_region.addr) = freed_bytes;
  if (payload_len > 0) {
    std::memcpy(send_region.addr + sizeof(uint32_t), payload, payload_len);
  }
  send_region.length = sizeof(uint32_t) + payload_len;
}

void SendAck(CircularConnectionNotify& sender, Region& send_region,
             uint32_t freed_bytes) {
  PrepareSendRegion(send_region, freed_bytes, nullptr, 0);
  uint64_t send_id = sender.SendAsync(send_region);
  sender.WaitSend(send_id);
}

void RpcSendBlocking(CircularConnectionNotify& sender,
                     CircularNotifyReceiver& receiver, Region& send_region,
                     const char* payload, uint32_t payload_len) {
  PrepareSendRegion(send_region, receiver.GetFreedReceiveBytes(), payload,
                    payload_len);
  uint64_t send_id = sender.SendAsync(send_region);
  sender.WaitSend(send_id);

  std::vector<Region> recvs;
  while (receiver.Receive(recvs) == 0) {
  }
  uint32_t ack_bytes = 0;
  for (auto& recv : recvs) {
    ack_bytes += *reinterpret_cast<const uint32_t*>(recv.addr);
    receiver.FreeReceive(recv);
  }
  if (ack_bytes > 0) {
    sender.AckSentBytes(ack_bytes);
  }
}

namespace zrpc {

class Client {
 public:
  Client(const std::string& server_ip, int port,
         uint32_t max_outstanding = kMaxSendWr)
      : ip_(server_ip), port_(port), max_outstanding_(max_outstanding) {
    ring_mem_ = static_cast<char*>(GetMagicBuffer(kBufferLen));
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

    ring_mr_ = ibv_reg_mr(id_->pd, ring_mem_, kBufferLen * 2,
                          IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                              IBV_ACCESS_REMOTE_READ);
    if (!ring_mr_) {
      std::cerr << "Failed to register ring MR" << std::endl;
      std::exit(1);
    }

    local_buffer_ = std::unique_ptr<MagicRingBuffer>(
        new MagicRingBuffer(ring_mr_, log2_pow2(kBufferLen), false));
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
    receiver_ = std::unique_ptr<CircularNotifyReceiver>(
        new CircularNotifyReceiver(endpoint_.ep, local_buffer_.get()));
    sender_ = std::unique_ptr<CircularConnectionNotify>(
        new CircularConnectionNotify(endpoint_.ep, remote_buffer_.get()));

    if (max_outstanding_ == 0) {
      std::cerr << "Invalid max_outstanding" << std::endl;
      std::exit(1);
    }
    const uint32_t send_mem_len = max_outstanding_ * kSendSlotSize;
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
          {0, send_mem + (i * kSendSlotSize), 0, send_mr_->lkey});
    }
  }

  void Send(const std::string& message) {
    uint32_t msg_len = static_cast<uint32_t>(message.size());
    if (msg_len > kMaxPayload) {
      msg_len = kMaxPayload;
    }
    Region& region = send_regions_[send_index_];
    RpcSendBlocking(*sender_, *receiver_, region, message.data(), msg_len);
    send_index_ = (send_index_ + 1) % max_outstanding_;
  }

  uint64_t SendAsync(const std::string& message) {
    uint32_t msg_len = static_cast<uint32_t>(message.size());
    if (msg_len > kMaxPayload) {
      msg_len = kMaxPayload;
    }
    while (outstanding_ >= max_outstanding_) {
      ProgressOnce();
    }

    Region& region = send_regions_[send_index_];
    send_index_ = (send_index_ + 1) % max_outstanding_;
    PrepareSendRegion(region, receiver_->GetFreedReceiveBytes(), message.data(),
                      msg_len);
    uint64_t send_id = sender_->SendAsync(region);
    inflight_ids_.push_back(send_id);
    ++outstanding_;
    return send_id;
  }

  uint32_t ProgressOnce() {
    uint32_t completed = 0;
    std::vector<Region> recvs;
    if (receiver_->Receive(recvs) > 0) {
      uint32_t ack_bytes = 0;
      for (auto& recv : recvs) {
        ack_bytes += *reinterpret_cast<const uint32_t*>(recv.addr);
        receiver_->FreeReceive(recv);
      }
      if (ack_bytes > 0) {
        sender_->AckSentBytes(ack_bytes);
      }
    }

    while (!inflight_ids_.empty() && sender_->TestSend(inflight_ids_.front())) {
      inflight_ids_.pop_front();
      if (outstanding_ > 0) {
        --outstanding_;
      }
      ++completed;
    }

    return completed;
  }

  void Progress() {
    ProgressOnce();
  }

  uint32_t Outstanding() const { return outstanding_; }

  uint32_t MaxOutstanding() const { return max_outstanding_; }

  ~Client() {
    if (endpoint_.ep) {
      rdma_disconnect(endpoint_.ep->id);
      delete endpoint_.ep;
      endpoint_.ep = nullptr;
    }
  }

 private:
  std::string ip_;
  int port_;
  uint32_t max_outstanding_ = kMaxSendWr;
  char* ring_mem_ = nullptr;
  struct rdma_cm_id* id_ = nullptr;
  struct ibv_mr* ring_mr_ = nullptr;
  struct ibv_mr* send_mr_ = nullptr;
  Endpoint endpoint_;
  std::unique_ptr<MagicRingBuffer> local_buffer_{};
  std::unique_ptr<MagicRemoteBuffer> remote_buffer_{};
  std::unique_ptr<CircularNotifyReceiver> receiver_{};
  std::unique_ptr<CircularConnectionNotify> sender_{};
  std::vector<Region> send_regions_{};
  uint32_t send_index_ = 0;
  std::deque<uint64_t> inflight_ids_{};
  uint32_t outstanding_ = 0;
};

class Server {
 public:
  Server(int port, uint32_t max_outstanding = kMaxSendWr)
      : port_(port), max_outstanding_(max_outstanding) {
    ip_ = GetHostIpV4();
    if (ip_.empty()) {
      std::cerr << "Failed to determine host IP" << std::endl;
      std::exit(1);
    }

    ring_mem_ = static_cast<char*>(GetMagicBuffer(kBufferLen));
    if (!ring_mem_) {
      std::cerr << "Failed to allocate magic buffer" << std::endl;
      std::exit(1);
    }

    server_ = std::unique_ptr<ServerRDMA>(
        new ServerRDMA(const_cast<char*>(ip_.c_str()), port_));
    ring_mr_ = ibv_reg_mr(server_->getPD(), ring_mem_, kBufferLen * 2,
                          IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                              IBV_ACCESS_REMOTE_READ);
    if (!ring_mr_) {
      std::cerr << "Failed to register ring MR" << std::endl;
      std::exit(1);
    }

    BufferContext ctx{reinterpret_cast<uint64_t>(ring_mr_->addr),
                      ring_mr_->rkey, kBufferLen};
    info_.code = kExperimentCode;
    info_.ctx = ctx;

    attr_ = prepare_qp(server_->getPD(), kMaxSendWr, kMaxRecvWr, false);

    char* send_mem = static_cast<char*>(aligned_alloc(4096, 4096));
    if (!send_mem) {
      std::cerr << "Failed to allocate send buffer" << std::endl;
      std::exit(1);
    }
    send_mr_ =
        ibv_reg_mr(server_->getPD(), send_mem, 4096, IBV_ACCESS_LOCAL_WRITE);
    if (!send_mr_) {
      std::cerr << "Failed to register send MR" << std::endl;
      std::exit(1);
    }
    send_region_ = {0, send_mem, 0, send_mr_->lkey};

    if (max_outstanding_ == 0) {
      std::cerr << "Invalid max_outstanding" << std::endl;
      std::exit(1);
    }
    const uint32_t send_mem_len = max_outstanding_ * kSendSlotSize;
    char* payload_mem = static_cast<char*>(aligned_alloc(4096, send_mem_len));
    if (!payload_mem) {
      std::cerr << "Failed to allocate payload send buffer" << std::endl;
      std::exit(1);
    }
    send_payload_mr_ = ibv_reg_mr(server_->getPD(), payload_mem, send_mem_len,
                                  IBV_ACCESS_LOCAL_WRITE);
    if (!send_payload_mr_) {
      std::cerr << "Failed to register payload send MR" << std::endl;
      std::exit(1);
    }
    send_regions_.reserve(max_outstanding_);
    for (uint32_t i = 0; i < max_outstanding_; ++i) {
      send_regions_.push_back(
          {0, payload_mem + (i * kSendSlotSize), 0, send_payload_mr_->lkey});
    }
  }

  bool AcceptOnce() {
    if (connected_) {
      return true;
    }

    local_buffer_ = std::unique_ptr<MagicRingBuffer>(
        new MagicRingBuffer(ring_mr_, log2_pow2(kBufferLen), false));
    endpoint_ = AcceptEndpoint(*server_, info_, attr_);
    if (endpoint_.peer_info.ctx.length == 0) {
      std::cerr << "Missing client buffer info" << std::endl;
      return false;
    }

    remote_buffer_ = std::unique_ptr<MagicRemoteBuffer>(
        new MagicRemoteBuffer(endpoint_.peer_info.ctx));
    receiver_ = std::unique_ptr<CircularNotifyReceiver>(
        new CircularNotifyReceiver(endpoint_.ep, local_buffer_.get()));
    sender_ = std::unique_ptr<CircularConnectionNotify>(
        new CircularConnectionNotify(endpoint_.ep, remote_buffer_.get()));

    orig_flags_ = fcntl(endpoint_.ep->id->channel->fd, F_GETFL, 0);
    if (orig_flags_ >= 0) {
      fcntl(endpoint_.ep->id->channel->fd, F_SETFL, orig_flags_ | O_NONBLOCK);
    }
    client_disconnected_ = false;
    connected_ = true;
    return true;
  }

  bool PollReceives(const std::function<void(const std::string&)>& handler) {
    if (!connected_) {
      return false;
    }

    std::vector<Region> recvs;
    if (receiver_->Receive(recvs) == 0) {
      struct rdma_cm_event* event = nullptr;
      if (rdma_get_cm_event(endpoint_.ep->id->channel, &event) == 0) {
        if (event->event == RDMA_CM_EVENT_DISCONNECTED) {
          client_disconnected_ = true;
          CloseConnection();
          rdma_ack_cm_event(event);
          return false;
        }
        rdma_ack_cm_event(event);
      } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
        perror("rdma_get_cm_event");
        CloseConnection();
      }
      return false;
    }

    uint32_t total_freed_bytes = 0;
    for (auto& recv : recvs) {
      total_freed_bytes += *reinterpret_cast<const uint32_t*>(recv.addr);
      if (recv.length > sizeof(uint32_t)) {
        const char* payload = recv.addr + sizeof(uint32_t);
        const uint32_t payload_len = recv.length - sizeof(uint32_t);
        handler(std::string(payload, payload + payload_len));
      }
      receiver_->FreeReceive(recv);
    }
    if (total_freed_bytes > 0) {
      sender_->AckSentBytes(total_freed_bytes);
    }
    SendAck(*sender_, send_region_, receiver_->GetFreedReceiveBytes());
    return true;
  }

  uint64_t SendAsync(const std::string& message) {
    if (!connected_) {
      return 0;
    }
    uint32_t msg_len = static_cast<uint32_t>(message.size());
    if (msg_len > kMaxPayload) {
      msg_len = kMaxPayload;
    }
    while (outstanding_ >= max_outstanding_) {
      DrainSendCompletions();
    }

    Region& region = send_regions_[send_index_];
    send_index_ = (send_index_ + 1) % max_outstanding_;
    PrepareSendRegion(region, receiver_->GetFreedReceiveBytes(),
                      message.data(), msg_len);
    uint64_t send_id = sender_->SendAsync(region);
    inflight_ids_.push_back(send_id);
    ++outstanding_;
    return send_id;
  }

  uint32_t DrainSendCompletions() {
    uint32_t completed = 0;
    while (!inflight_ids_.empty() && sender_->TestSend(inflight_ids_.front())) {
      inflight_ids_.pop_front();
      if (outstanding_ > 0) {
        --outstanding_;
      }
      ++completed;
    }
    return completed;
  }

  uint32_t Outstanding() const { return outstanding_; }

  uint32_t MaxOutstanding() const { return max_outstanding_; }

  void CloseConnection() {
    if (!connected_) {
      return;
    }
    if (orig_flags_ >= 0) {
      fcntl(endpoint_.ep->id->channel->fd, F_SETFL, orig_flags_);
    }
    if (!client_disconnected_) {
      rdma_disconnect(endpoint_.ep->id);
    }
    delete endpoint_.ep;
    endpoint_.ep = nullptr;
    receiver_.reset();
    sender_.reset();
    remote_buffer_.reset();
    local_buffer_.reset();
    connected_ = false;
  }

  void Run(const std::function<void(const std::string&)>& handler) {
    std::cout << "[Server] Ready on " << ip_ << ":" << port_ << std::endl;

    while (true) {
      if (!AcceptOnce()) {
        return;
      }
      while (connected_) {
        PollReceives(handler);
      }
    }
  }

 private:
  int port_;
  std::string ip_;
  uint32_t max_outstanding_ = kMaxSendWr;
  char* ring_mem_ = nullptr;
  struct ibv_mr* ring_mr_ = nullptr;
  struct ibv_mr* send_mr_ = nullptr;
  struct ibv_mr* send_payload_mr_ = nullptr;
  connect_info info_{};
  struct ibv_qp_init_attr attr_{};
  Region send_region_{};
  std::unique_ptr<ServerRDMA> server_{};
  Endpoint endpoint_{};
  std::unique_ptr<MagicRingBuffer> local_buffer_{};
  std::unique_ptr<MagicRemoteBuffer> remote_buffer_{};
  std::unique_ptr<CircularNotifyReceiver> receiver_{};
  std::unique_ptr<CircularConnectionNotify> sender_{};
  std::vector<Region> send_regions_{};
  uint32_t send_index_ = 0;
  std::deque<uint64_t> inflight_ids_{};
  uint32_t outstanding_ = 0;
  int orig_flags_ = -1;
  bool client_disconnected_ = false;
  bool connected_ = false;
};

}  // namespace zrpc

void P2PTest(zrpc::Client& client, const std::string& message,
             uint32_t measure_iters) {
  using namespace std::chrono;
  uint32_t message_size = message.size();
  const uint32_t max_outstanding = client.MaxOutstanding();
  printf("Message size %u and outstanding is %u\n", message_size,
         max_outstanding);

  std::chrono::seconds delay(5);
  auto next_print = steady_clock::now() + delay;
  uint32_t done = 0;
  uint32_t outstanding = 0;
  while (measure_iters > 0) {
    while (outstanding < max_outstanding) {
      client.SendAsync(message);
      ++outstanding;
    }

    uint32_t completed = client.ProgressOnce();
    if (completed > 0) {
      outstanding = (outstanding > completed) ? (outstanding - completed) : 0;
      done += completed;
    }

    if (steady_clock::now() > next_print) {
      printf("[zRPC BW test] completed %u sends in 5 sec\n", done);
      done = 0;
      --measure_iters;
      next_print = steady_clock::now() + delay;
    }
  }
}

void P2PTestServer(zrpc::Server& server, const std::string& message,
                   uint32_t measure_iters) {
  using namespace std::chrono;
  uint32_t message_size = message.size();
  const uint32_t max_outstanding = server.MaxOutstanding();
  printf("Message size %u and outstanding is %u\n", message_size,
         max_outstanding);

  std::chrono::seconds delay(5);
  auto next_print = steady_clock::now() + delay;
  uint32_t done = 0;
  uint32_t outstanding = 0;

  if (!server.AcceptOnce()) {
    return;
  }

  while (measure_iters > 0) {
    while (outstanding < max_outstanding) {
      server.SendAsync(message);
      ++outstanding;
    }

    server.PollReceives([](const std::string&) {});
    uint32_t completed = server.DrainSendCompletions();
    if (completed > 0) {
      outstanding = (outstanding > completed) ? (outstanding - completed) : 0;
      done += completed;
    }

    if (steady_clock::now() > next_print) {
      printf("[zRPC BW test] completed %u sends in 5 sec\n", done);
      done = 0;
      --measure_iters;
      next_print = steady_clock::now() + delay;
    }
  }
}

Endpoint AcceptEndpoint(ServerRDMA& server, const connect_info& local_info,
                        struct ibv_qp_init_attr attr) {
  std::pair<struct rdma_cm_id*, void*> req = server.get_connect_request();
  struct rdma_cm_id* id = req.first;
  void* buf = req.second;
  if (!id) {
    std::cerr << "Failed to accept connection" << std::endl;
    std::exit(1);
  }

  connect_info peer_info{};
  if (buf) {
    std::memcpy(&peer_info, buf, sizeof(peer_info));
    std::free(buf);
  }

  if (rdma_create_qp(id, server.getPD(), &attr)) {
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
      if (copy_len > sizeof(peer_info)) {
        copy_len = sizeof(peer_info);
      }
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
      std::cerr << "Connection failed on client" << std::endl;
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

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cout << "Usage: " << argv[0] << " server [port]\n"
              << "       " << argv[0]
              << " client <server_ip> [port] [message] [count] [async]"
                 " [--test p2p] [--iters N] [--outstanding N]\n";
    return 0;
  }

  const std::string mode = argv[1];

  if (mode == "server") {
    const int port = (argc >= 3) ? std::stoi(argv[2]) : 9999;
    bool test_p2p = false;
    uint32_t measure_iters = 5;
    uint32_t max_outstanding = kMaxSendWr;
    for (int i = 2; i < argc; ++i) {
      const std::string arg = argv[i];
      if (arg == "--test" && i + 1 < argc) {
        test_p2p = (std::string(argv[i + 1]) == "p2p");
      }
      if (arg == "--iters" && i + 1 < argc) {
        int value = std::stoi(argv[i + 1]);
        if (value > 0) {
          measure_iters = static_cast<uint32_t>(value);
        }
      }
      if (arg == "--outstanding" && i + 1 < argc) {
        int value = std::stoi(argv[i + 1]);
        if (value > 0) {
          max_outstanding = static_cast<uint32_t>(value);
        }
      }
    }
    zrpc::Server server(port, max_outstanding);
    if (test_p2p) {
      P2PTestServer(server, "pong", measure_iters);
      return 0;
    }
    server.Run([](const std::string& message) {
      std::cout << "[zRPC Server] recv: " << message << std::endl;
    });
    return 0;
  }

  if (mode == "client") {
    if (argc < 3) {
      std::cout << "Usage: " << argv[0]
                << " client <server_ip> [port] [message] [count] [async]"
                   " [--test p2p] [--iters N] [--outstanding N]\n";
      return 0;
    }
    const std::string ip = argv[2];
    const int port = (argc >= 4) ? std::stoi(argv[3]) : 9999;
    uint32_t max_outstanding = kMaxSendWr;
    for (int i = 2; i < argc; ++i) {
      const std::string arg = argv[i];
      if (arg == "--outstanding" && i + 1 < argc) {
        int value = std::stoi(argv[i + 1]);
        if (value > 0) {
          max_outstanding = static_cast<uint32_t>(value);
        }
      }
    }
    zrpc::Client client(ip, port, max_outstanding);
    std::string message = (argc >= 5) ? argv[4] : "pong";
    int count = (argc >= 6) ? std::stoi(argv[5]) : 1;
    const bool async_mode = (argc >= 7 && std::string(argv[6]) == "async");
    bool test_p2p = false;
    uint32_t measure_iters = 5;
    for (int i = 2; i < argc; ++i) {
      const std::string arg = argv[i];
      if (arg == "--test" && i + 1 < argc) {
        test_p2p = (std::string(argv[i + 1]) == "p2p");
      }
      if (arg == "--iters" && i + 1 < argc) {
        int value = std::stoi(argv[i + 1]);
        if (value > 0) {
          measure_iters = static_cast<uint32_t>(value);
        }
      }
    }
    if (test_p2p) {
      P2PTest(client, message, measure_iters);
      return 0;
    }
    if (async_mode) {
      for (int i = 0; i < count; ++i) {
        client.SendAsync(message);
        client.Progress();
      }
      for (int i = 0; i < count; ++i) {
        client.Progress();
      }
    } else {
      for (int i = 0; i < count; ++i) {
        client.Send(message);
      }
    }
    return 0;
  }

  std::cerr << "Unknown mode: " << mode << std::endl;
  return 1;
}
