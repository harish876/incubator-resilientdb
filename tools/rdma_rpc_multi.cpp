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
#include "cxxopts.hpp"
#include "com/magic_ring.hpp"
#include "com/protocols.hpp"
#include "com/ring.hpp"
#include "com/utils.hpp"

namespace {

constexpr uint32_t kBufferLen = 4096;
constexpr uint32_t kRecvBatch = 16;
constexpr uint32_t kMaxSendWr = 128;
constexpr uint32_t kMaxRecvWr = 128;
constexpr uint32_t kExperimentCode = 6;
constexpr uint32_t kMaxPayload = 4096;
constexpr uint32_t kSendSlotSize = kMaxPayload;
constexpr uint32_t kDefaultOutstanding = 8;
constexpr uint32_t kMailboxSize = sizeof(uint64_t) * 2;
constexpr uint32_t kLocalMemSize = sizeof(uint64_t) * 32;

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
                        struct ibv_qp_init_attr attr, uint32_t cid);
Endpoint ConnectEndpoint(struct rdma_cm_id* id, const connect_info& local_info,
                         struct ibv_qp_init_attr attr);

uint32_t log2_pow2(uint32_t value) {
  uint32_t out = 0;
  while ((1u << out) < value) {
    ++out;
  }
  return out;
}

bool IsUnsignedNumber(const std::string& value) {
  if (value.empty()) {
    return false;
  }
  for (char c : value) {
    if (c < '0' || c > '9') {
      return false;
    }
  }
  return true;
}

std::vector<char*> BuildArgv(std::vector<std::string>& args) {
  std::vector<char*> argv;
  argv.reserve(args.size());
  for (auto& arg : args) {
    argv.push_back(const_cast<char*>(arg.c_str()));
  }
  return argv;
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

namespace zrpc {

class Client {
 public:
  Client(const std::string& server_ip, int port,
         uint32_t max_outstanding = kDefaultOutstanding)
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
        new MagicRingBuffer(ring_mr_, log2_pow2(kBufferLen), true));
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
    RpcSendBlocking(*sender_, region, message.data(), msg_len);
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
      if (outstanding_ > 0) {
        --outstanding_;
      }
      ++completed;
    }

    return completed;
  }

  void Progress() { ProgressOnce(); }

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
  uint32_t max_outstanding_ = kDefaultOutstanding;
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

void BwSharedTest(Client& client, const std::string& message,
                  uint32_t measure) {
  using std::chrono::seconds;
  using std::chrono::steady_clock;
  auto next_print = steady_clock::now() + seconds(5);
  uint32_t done = 0;

  while (measure > 0) {
    if (client.Outstanding() < client.MaxOutstanding()) {
      client.SendAsync(message);
    }
    done += client.ProgressOnce();

    if (steady_clock::now() >= next_print) {
      std::cout << "[BW test] completed " << done << " sends in 5 sec"
                << std::endl;
      done = 0;
      --measure;
      next_print = steady_clock::now() + seconds(5);
    }
  }
}

class MultiServer {
 public:
  MultiServer(int port, uint32_t max_outstanding, uint32_t max_clients)
      : port_(port), max_clients_(max_clients) {
    (void)max_outstanding;
    ip_ = GetHostIpV4();
    if (ip_.empty()) {
      std::cerr << "Failed to determine host IP" << std::endl;
      std::exit(1);
    }

    server_ = std::unique_ptr<ServerRDMA>(
        new ServerRDMA(const_cast<char*>(ip_.c_str()), port_));
    attr_ = prepare_qp(server_->getPD(), kMaxSendWr, kMaxRecvWr, false);

    ring_mem_ = static_cast<char*>(GetMagicBuffer(kBufferLen));
    if (!ring_mem_) {
      std::cerr << "Failed to allocate magic buffer" << std::endl;
      std::exit(1);
    }
    ring_mr_ = ibv_reg_mr(server_->getPD(), ring_mem_, kBufferLen * 2,
                          IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                              IBV_ACCESS_REMOTE_READ);
    if (!ring_mr_) {
      std::cerr << "Failed to register ring MR" << std::endl;
      std::exit(1);
    }
    local_buffer_ = std::unique_ptr<MagicRingBuffer>(
        new MagicRingBuffer(ring_mr_, log2_pow2(kBufferLen), true));

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
    uint32_t cid = next_cid_.load(std::memory_order_relaxed);
    Endpoint endpoint = AcceptEndpoint(*server_, info_, attr_, cid);
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
    if (!recv_cq_) {
      return;
    }

    struct ibv_wc wcs[16];
    int ret = ibv_poll_cq(recv_cq_, 16, wcs);
    for (int i = 0; i < ret; ++i) {
      if (wcs[i].opcode != IBV_WC_RECV_RDMA_WITH_IMM) {
        continue;
      }
      const uint32_t length = wcs[i].byte_len;
      const uint32_t offset = wcs[i].imm_data;
      char* payload = local_buffer_->GetReadPtr(offset);
      const uint32_t cid = static_cast<uint32_t>(wcs[i].wr_id);
      if (cid >= max_clients_) {
        continue;
      }
      if (cid >= connected_.load(std::memory_order_acquire)) {
        continue;
      }
      VerbsEP* ep = eps_[cid];
      if (!ep) {
        continue;
      }
      handler(cid, std::string(payload, payload + length));
      ep->post_empty_recvs(1);

      const uint64_t new_head = local_buffer_->FreeOrdered(payload, length);
      if (new_head != lhead_) {
        lhead_ = new_head;
        if (head_ptr_) {
          *head_ptr_ = lhead_;
        }
      }
    }
  }

  void Run(const std::function<void(uint32_t, const std::string&)>& handler) {
    std::cout << "[Server] Ready on " << ip_ << ":" << port_ << std::endl;
    accept_thread_ = std::thread([this]() { AcceptLoop(); });
    accept_thread_.detach();

    while (connected_.load(std::memory_order_acquire) == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    if (eps_[0]) {
      recv_cq_ = eps_[0]->qp->recv_cq;
      lhead_ = local_buffer_->Free(0);
      head_ptr_ = reinterpret_cast<volatile uint64_t*>(mailbox_mem_);
      faa_ptr_ =
          reinterpret_cast<volatile uint64_t*>(mailbox_mem_ + sizeof(uint64_t));
      if (head_ptr_) {
        *head_ptr_ = lhead_;
      }
      if (faa_ptr_) {
        *faa_ptr_ = 0;
      }
      // std::cout << "Head value: " << lhead_ << std::endl;
      // std::cout << "Clients fetch from "
      //           << static_cast<void*>(const_cast<uint64_t*>(faa_ptr_))
      //           << std::endl;
      // std::cout << "I write my head progress here: "
      //           << static_cast<void*>(const_cast<uint64_t*>(head_ptr_))
      //           << std::endl;
    }

    while (true) {
      PollReceivesAll(handler);
    }
  }

 private:
  int port_;
  std::string ip_;
  uint32_t max_clients_ = 1;
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

}  // namespace zrpc

Endpoint AcceptEndpoint(ServerRDMA& server, const connect_info& local_info,
                        struct ibv_qp_init_attr attr, uint32_t cid) {
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
  id->context = reinterpret_cast<void*>(static_cast<uint64_t>(cid));

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

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cout << "Usage: " << argv[0] << " server [port] --clients N"
              << " [--outstanding N] [--quiet|--bw]\n"
              << "       " << argv[0]
              << " client <server_ip> [port] [message] [count] [async]"
              " [--outstanding N] [--bw [N]]\n";
    return 0;
  }

  const std::string mode = argv[1];

  std::vector<std::string> args;
  args.reserve(static_cast<size_t>(argc));
  args.push_back(argv[0]);
  for (int i = 2; i < argc; ++i) {
    args.push_back(argv[i]);
  }

  if (mode == "server") {
    cxxopts::Options options(argv[0], "zRPC multi server");
    options.positional_help("[port]").show_positional_help();
    options.add_options()
        ("clients", "max clients", cxxopts::value<uint32_t>())
        ("outstanding", "max outstanding",
         cxxopts::value<uint32_t>()->default_value(
             std::to_string(kDefaultOutstanding)))
        ("quiet", "suppress per-message output")
        ("bw", "alias for --quiet")
        ("port", "port", cxxopts::value<int>()->default_value("9999"))
        ("h,help", "Print help");
    options.parse_positional({"port"});

    auto argv_storage = BuildArgv(args);
    int argc_parsed = static_cast<int>(argv_storage.size());
    char** argv_parsed = argv_storage.data();
    auto result = options.parse(argc_parsed, argv_parsed);
    if (result.count("help")) {
      std::cout << options.help() << std::endl;
      return 0;
    }
    if (!result.count("clients")) {
      std::cerr << "Missing required --clients N for server\n";
      std::cout << options.help() << std::endl;
      return 1;
    }
    const int port = result["port"].as<int>();
    const uint32_t max_outstanding = result["outstanding"].as<uint32_t>();
    const uint32_t max_clients = result["clients"].as<uint32_t>();
    const bool quiet = result.count("quiet") || result.count("bw");

    zrpc::MultiServer server(port, max_outstanding, max_clients);
    if (quiet) {
      server.Run([](uint32_t, const std::string&) {});
    } else {
      server.Run([](uint32_t client_id, const std::string& message) {
        std::cout << "[zRPC Multi Server] client " << client_id
                  << " recv: " << message << std::endl;
      });
    }
    return 0;
  }

  if (mode == "client") {
    if (argc < 3) {
      std::cout << "Usage: " << argv[0]
                << " client <server_ip> [port] [message] [count] [async]"
                   " [--outstanding N] [--bw [N]]\n";
      return 0;
    }
    const std::string ip = argv[2];
    cxxopts::Options options(argv[0], "zRPC multi client");
    options.allow_unrecognised_options();
    options.add_options()
        ("outstanding", "max outstanding",
         cxxopts::value<uint32_t>()->default_value(
             std::to_string(kDefaultOutstanding)))
        ("bw", "bandwidth test windows",
         cxxopts::value<uint32_t>()->implicit_value("20"))
        ("h,help", "Print help");
    auto argv_storage = BuildArgv(args);
    int argc_parsed = static_cast<int>(argv_storage.size());
    char** argv_parsed = argv_storage.data();
    auto result = options.parse(argc_parsed, argv_parsed);
    if (result.count("help")) {
      std::cout << options.help() << std::endl;
      return 0;
    }
    const uint32_t max_outstanding = result["outstanding"].as<uint32_t>();
    const bool bw_mode = result.count("bw") > 0;
    const uint32_t bw_measure =
        bw_mode ? result["bw"].as<uint32_t>() : 20;
    std::vector<std::string> positionals;
    positionals.reserve(static_cast<size_t>(argc));
    for (int i = 3; i < argc; ++i) {
      const std::string arg = argv[i];
      if (arg == "--outstanding" || arg == "--bw") {
        if (i + 1 < argc && IsUnsignedNumber(argv[i + 1])) {
          ++i;
        }
        continue;
      }
      if (!arg.empty() && arg[0] == '-') {
        continue;
      }
      positionals.push_back(arg);
    }

    int port = 9999;
    size_t pos_index = 0;
    if (pos_index < positionals.size() &&
        IsUnsignedNumber(positionals[pos_index])) {
      port = std::stoi(positionals[pos_index]);
      ++pos_index;
    }
    std::string message =
        (pos_index < positionals.size()) ? positionals[pos_index] : "pong";
    ++pos_index;
    int count = 1;
    if (pos_index < positionals.size() &&
        IsUnsignedNumber(positionals[pos_index])) {
      count = std::stoi(positionals[pos_index]);
      ++pos_index;
    }
    const bool async_mode =
        (pos_index < positionals.size() && positionals[pos_index] == "async");
    zrpc::Client client(ip, port, max_outstanding);
    if (bw_mode) {
      BwSharedTest(client, message, bw_measure);
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
