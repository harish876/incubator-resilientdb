#include <arpa/inet.h>
#include <ifaddrs.h>
#include <infiniband/verbs.h>
#include <netinet/in.h>
#include <rdma/rdma_cma.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <deque>
#include <iostream>

#include "ClientRDMA.hpp"
#include "ServerRDMA.hpp"
#include "VerbsEP.hpp"
#include "com/basic_ring.hpp"
#include "com/protocols.hpp"
#include "com/ring.hpp"

// Basic ring buffer demo — structurally mirrors rdma_fetchadd.cpp.
//
//   FetchAddWriter  ↔  BasicRingWriter
//   fetch-and-add slot allocation  ↔  BasicRemoteBuffer::GetWriteAddr()
//   rdmapp coroutines  ↔  blocking rdma-rpc calls (rdmapp submodule not init'd)
//
// Usage:
//   server:     rdma_rpc_basic server [port]
//   client:     rdma_rpc_basic client <ip> [port] [msg] [num_requests]
//   bw server:  rdma_rpc_basic bw server [port]
//   bw client:  rdma_rpc_basic bw client <ip> [port] [msg_size] [window]

namespace {

// ── ring parameters ───────────────────────────────────────────────────────

static constexpr uint32_t kBufLen    = 4096;    // functional test ring size
static constexpr uint32_t kBwBufLen  = 1 << 20; // 1 MB for BW test
static constexpr uint32_t kMaxMsgSize = 4096;
static constexpr int      kNumDataTransfers = 10;   // default num_requests
static constexpr uint32_t kBwWindow  = 16;
static constexpr uint32_t kMaxSendWr = 128;
static constexpr uint32_t kMaxRecvWr = 128;
static constexpr uint32_t kRecvBatch = 16;

// ── BasicRingWriter ────────────────────────────────────────────────────────
//
// Analogous to FetchAddWriter in rdma_fetchadd.cpp.
// Owns the RemoteBuffer state and send logic for one connection.
// Instead of fetch-and-add for slot selection, GetWriteAddr() on
// BasicRemoteBuffer advances the tail with linear wrap-around.

struct BasicRingWriter {
  BasicRingWriter(VerbsEP* ep, BufferContext ctx)
      : remote_(ctx), sender_(ep, &remote_) {}

  BasicRingWriter(const BasicRingWriter&) = delete;
  BasicRingWriter& operator=(const BasicRingWriter&) = delete;

  // Blocking write — posts RDMA write-with-imm and waits for send completion.
  void Write(Region& region) {
    uint64_t wrid = sender_.SendAsync(region);
    sender_.WaitSend(wrid);
    // Speculatively free remote ring space (no head feedback from server).
    sender_.AckSentBytes(region.length);
  }

  // Non-blocking write — returns wr_id for later polling.
  uint64_t WriteAsync(Region& region) {
    return sender_.SendAsync(region);
  }

  bool TestSend(uint64_t wrid)  { return sender_.TestSend(wrid); }
  void AckSent(uint32_t bytes)  { sender_.AckSentBytes(bytes); }

 private:
  BasicRemoteBuffer        remote_;   // must be constructed before sender_
  CircularConnectionNotify sender_;   // holds &remote_ — no copy allowed
};

// ── connection helpers ─────────────────────────────────────────────────────

std::string GetHostIpV4() {
  ifaddrs* ifa = nullptr;
  if (getifaddrs(&ifa) != 0) return "";
  std::string ip;
  for (auto* p = ifa; p; p = p->ifa_next) {
    if (!p->ifa_addr || p->ifa_addr->sa_family != AF_INET) continue;
    if (std::strcmp(p->ifa_name, "lo") == 0) continue;
    char buf[INET_ADDRSTRLEN] = {};
    if (inet_ntop(AF_INET,
                  &reinterpret_cast<sockaddr_in*>(p->ifa_addr)->sin_addr,
                  buf, sizeof(buf))) {
      ip = buf; break;
    }
  }
  freeifaddrs(ifa);
  return ip;
}

std::pair<VerbsEP*, connect_info> AcceptOne(ServerRDMA& srv,
                                            const connect_info& local_info) {
  auto [id, buf] = srv.get_connect_request();
  if (!id) { perror("get_connect_request"); std::exit(1); }

  connect_info peer{};
  if (buf) { std::memcpy(&peer, buf, sizeof(peer)); std::free(buf); }

  auto attr = prepare_qp(srv.getPD(), kMaxSendWr, kMaxRecvWr, false);
  if (rdma_create_qp(id, srv.getPD(), &attr)) { perror("rdma_create_qp"); std::exit(1); }

  VerbsEP* ep = new VerbsEP(id, attr, kRecvBatch, false);

  rdma_conn_param cp{};
  cp.responder_resources = 16; cp.initiator_depth = 16;
  cp.retry_count = 3;          cp.rnr_retry_count = 3;
  cp.private_data = &local_info;
  cp.private_data_len = sizeof(local_info);
  if (rdma_accept(id, &cp)) { perror("rdma_accept"); std::exit(1); }

  rdma_cm_event* ev = nullptr;
  while (true) {
    if (rdma_get_cm_event(id->channel, &ev)) { perror("get_cm_event"); std::exit(1); }
    bool est = (ev->event == RDMA_CM_EVENT_ESTABLISHED);
    rdma_ack_cm_event(ev);
    if (est) break;
  }
  return {ep, peer};
}

std::pair<VerbsEP*, connect_info> ConnectTo(rdma_cm_id* id,
                                             const connect_info& local_info) {
  auto attr = prepare_qp(id->pd, kMaxSendWr, kMaxRecvWr, false);
  if (rdma_create_qp(id, id->pd, &attr)) { perror("rdma_create_qp"); std::exit(1); }

  VerbsEP* ep = new VerbsEP(id, attr, kRecvBatch, false);

  rdma_conn_param cp{};
  cp.responder_resources = 16; cp.initiator_depth = 16;
  cp.retry_count = 3;          cp.rnr_retry_count = 3;
  cp.private_data = &local_info;
  cp.private_data_len = sizeof(local_info);
  if (rdma_connect(id, &cp)) { perror("rdma_connect"); std::exit(1); }

  rdma_cm_event* ev = nullptr;
  connect_info peer{};
  while (true) {
    if (rdma_get_cm_event(id->channel, &ev)) { perror("get_cm_event"); std::exit(1); }
    if (ev->param.conn.private_data_len > 0) {
      uint32_t n = std::min((uint32_t)ev->param.conn.private_data_len,
                            (uint32_t)sizeof(peer));
      std::memcpy(&peer, ev->param.conn.private_data, n);
    }
    bool est = (ev->event == RDMA_CM_EVENT_ESTABLISHED);
    rdma_ack_cm_event(ev);
    if (est) break;
  }
  return {ep, peer};
}

// ── server ────────────────────────────────────────────────────────────────
// Mirrors server() in rdma_fetchadd.cpp:
//   - registers the ring buffer MR and shares it with the client
//   - polls recv CQ; reads each message via BasicRingBuffer::Read()

void server(int port) {
  std::string ip = GetHostIpV4();
  ServerRDMA srv(const_cast<char*>(ip.c_str()), port);

  char* mem = static_cast<char*>(aligned_alloc(4096, kBufLen));
  std::memset(mem, 0, kBufLen);
  ibv_mr* mr = ibv_reg_mr(srv.getPD(), mem, kBufLen,
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
  if (!mr) { perror("ibv_reg_mr"); std::exit(1); }

  // BasicRingBuffer: plain linear wrap-around, no double-mapped virtual memory.
  BasicRingBuffer ring(mr, kBufLen, /*with_zero=*/true);

  connect_info local_info{};
  local_info.ctx = ring.GetContext();   // addr + rkey + length → sent to client

  std::cout << "[Server] MR sent (BasicRing buffer)" << std::endl;
  auto [ep, peer] = AcceptOne(srv, local_info);
  std::cout << "[Server] Connected. Waiting for writes..." << std::endl;

  ibv_cq* recv_cq = ep->qp->recv_cq;
  int count = 0;
  while (true) {
    ibv_wc wcs[16];
    int n = ibv_poll_cq(recv_cq, 16, wcs);
    for (int i = 0; i < n; ++i) {
      if (wcs[i].opcode != IBV_WC_RECV_RDMA_WITH_IMM) continue;
      uint32_t len = wcs[i].byte_len;
      // BasicRingBuffer::Read() advances read_ptr_ sequentially with wrap-around.
      // (contrast with FetchAdd which uses imm_data slot index for random access)
      char* data = ring.Read(len);
      ++count;
      std::cout << "[Server] recv #" << count
                << " len=" << len
                << " data=\"" << std::string(data, len) << "\""
                << std::endl;
      ring.Free(len);
      ep->post_empty_recvs(1);
    }
  }
}

// ── client ────────────────────────────────────────────────────────────────
// Mirrors client() in rdma_fetchadd.cpp:
//   - connects, receives server's BufferContext (analogous to remote_mr)
//   - constructs BasicRingWriter (analogous to FetchAddWriter)
//   - sends num_requests messages and prints timing

void client(const char* ip, int port, const std::string& msg, int num_requests) {
  rdma_cm_id* id = ClientRDMA::sendConnectRequest(const_cast<char*>(ip), port);
  if (!id) { std::cerr << "sendConnectRequest failed" << std::endl; std::exit(1); }
  if (!id->pd) id->pd = ibv_alloc_pd(id->verbs);

  connect_info local_info{};
  auto [ep, peer] = ConnectTo(id, local_info);

  if (peer.ctx.length == 0) {
    std::cerr << "server did not send buffer context" << std::endl; std::exit(1);
  }

  // BasicRingWriter owns BasicRemoteBuffer + CircularConnectionNotify.
  // Analogous to FetchAddWriter owning qp + base + rkey.
  BasicRingWriter writer(ep, peer.ctx);

  uint32_t msg_len = static_cast<uint32_t>(msg.size());
  char* send_mem = static_cast<char*>(aligned_alloc(4096, msg_len));
  std::memcpy(send_mem, msg.data(), msg_len);
  ibv_mr* send_mr = ibv_reg_mr(id->pd, send_mem, msg_len, IBV_ACCESS_LOCAL_WRITE);
  if (!send_mr) { perror("ibv_reg_mr"); std::exit(1); }

  Region region{0, send_mem, msg_len, send_mr->lkey};

  std::cout << "[Client] Connected (BasicRing mode)" << std::endl;

  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < num_requests; ++i) {
    writer.Write(region);
  }
  auto end = std::chrono::steady_clock::now();

  double sec = std::chrono::duration<double>(end - start).count();
  std::cout << "[Client] "
            << num_requests << " writes in "
            << sec << " s -> "
            << static_cast<int>(num_requests / sec)
            << " writes/s"
            << std::endl;

  std::free(send_mem);
  rdma_disconnect(ep->id);
  delete ep;
}

// ── bandwidth test ─────────────────────────────────────────────────────────

void bw_server(int port) {
  using clk = std::chrono::steady_clock;
  std::string ip = GetHostIpV4();
  ServerRDMA srv(const_cast<char*>(ip.c_str()), port);

  char* mem = static_cast<char*>(aligned_alloc(4096, kBwBufLen));
  std::memset(mem, 0, kBwBufLen);
  ibv_mr* mr = ibv_reg_mr(srv.getPD(), mem, kBwBufLen,
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
  if (!mr) { perror("ibv_reg_mr"); std::exit(1); }

  BasicRingBuffer ring(mr, kBwBufLen, /*with_zero=*/false);  // skip zero for speed

  connect_info local_info{};
  local_info.ctx = ring.GetContext();

  std::cout << "[BW-Server] listening on " << ip << ":" << port << std::endl;
  auto [ep, peer] = AcceptOne(srv, local_info);
  std::cout << "[BW-Server] client connected — measuring..." << std::endl;

  ibv_cq* recv_cq = ep->qp->recv_cq;
  uint64_t msg_count = 0, byte_count = 0;
  auto t0 = clk::now();

  while (true) {
    ibv_wc wcs[16];
    int n = ibv_poll_cq(recv_cq, 16, wcs);
    for (int i = 0; i < n; ++i) {
      if (wcs[i].opcode != IBV_WC_RECV_RDMA_WITH_IMM) continue;
      uint32_t len = wcs[i].byte_len;
      ring.Read(len);
      ring.Free(len);
      ep->post_empty_recvs(1);
      ++msg_count;
      byte_count += len;
    }
    double elapsed = std::chrono::duration<double>(clk::now() - t0).count();
    if (elapsed >= 1.0) {
      printf("[BW-Server] %.3f Mops/s   %.1f MB/s   msg_size=%lu B\n",
             msg_count / elapsed / 1e6,
             byte_count / elapsed / 1e6,
             msg_count ? byte_count / msg_count : 0UL);
      msg_count = 0; byte_count = 0;
      t0 = clk::now();
    }
  }
}

void bw_client(const char* ip, int port, uint32_t msg_size, uint32_t window) {
  using clk = std::chrono::steady_clock;

  rdma_cm_id* id = ClientRDMA::sendConnectRequest(const_cast<char*>(ip), port);
  if (!id) { std::cerr << "sendConnectRequest failed" << std::endl; std::exit(1); }
  if (!id->pd) id->pd = ibv_alloc_pd(id->verbs);

  connect_info local_info{};
  auto [ep, peer] = ConnectTo(id, local_info);
  if (peer.ctx.length == 0) {
    std::cerr << "server did not send buffer context" << std::endl; std::exit(1);
  }

  BasicRingWriter writer(ep, peer.ctx);

  char* send_mem = static_cast<char*>(aligned_alloc(4096, msg_size));
  std::memset(send_mem, 0xAB, msg_size);
  ibv_mr* send_mr = ibv_reg_mr(id->pd, send_mem, msg_size, IBV_ACCESS_LOCAL_WRITE);
  if (!send_mr) { perror("ibv_reg_mr"); std::exit(1); }

  Region region{0, send_mem, msg_size, send_mr->lkey};

  std::deque<uint64_t> inflight;
  uint64_t completed = 0;
  auto t0 = clk::now();

  printf("[BW-Client] msg_size=%u B  window=%u\n", msg_size, window);

  while (true) {
    while (!inflight.empty() && writer.TestSend(inflight.front())) {
      inflight.pop_front();
      writer.AckSent(msg_size);
      ++completed;
    }
    while (inflight.size() < window) {
      region.length = msg_size;
      inflight.push_back(writer.WriteAsync(region));
    }
    double elapsed = std::chrono::duration<double>(clk::now() - t0).count();
    if (elapsed >= 1.0) {
      printf("[BW-Client] %.3f Mops/s   %.1f MB/s\n",
             completed / elapsed / 1e6,
             completed * msg_size / elapsed / 1e6);
      completed = 0;
      t0 = clk::now();
    }
  }
}

}  // namespace

int main(int argc, char* argv[]) {
  if (argc < 2) {
    printf("Usage:\n"
           "  server:    %s server [port]\n"
           "  client:    %s client <ip> [port] [msg] [num_requests]\n"
           "  bw server: %s bw server [port]\n"
           "  bw client: %s bw client <ip> [port] [msg_size] [window]\n",
           argv[0], argv[0], argv[0], argv[0]);
    return 0;
  }

  std::string mode = argv[1];
  int port = 9999;

  if (mode == "server") {
    if (argc >= 3) port = std::atoi(argv[2]);
    server(port);
    return 0;
  }

  if (mode == "client") {
    if (argc < 3) { fprintf(stderr, "client needs <ip>\n"); return 1; }
    const char* ip  = argv[2];
    if (argc >= 4)  port = std::atoi(argv[3]);
    std::string msg = (argc >= 5) ? argv[4] : "bench";
    int num         = (argc >= 6) ? std::atoi(argv[5]) : kNumDataTransfers;
    client(ip, port, msg, num);
    return 0;
  }

  if (mode == "bw") {
    if (argc < 3) { fprintf(stderr, "bw needs server|client\n"); return 1; }
    std::string sub = argv[2];
    if (sub == "server") {
      if (argc >= 4) port = std::atoi(argv[3]);
      bw_server(port);
      return 0;
    }
    if (sub == "client") {
      if (argc < 4) { fprintf(stderr, "bw client needs <ip>\n"); return 1; }
      const char* ip   = argv[3];
      if (argc >= 5)   port      = std::atoi(argv[4]);
      uint32_t msg_size = (argc >= 6) ? std::atoi(argv[5]) : 64;
      uint32_t window   = (argc >= 7) ? std::atoi(argv[6]) : kBwWindow;
      bw_client(ip, port, msg_size, window);
      return 0;
    }
    fprintf(stderr, "bw: unknown sub-mode %s\n", sub.c_str()); return 1;
  }

  fprintf(stderr, "unknown mode: %s\n", mode.c_str());
  return 1;
}
