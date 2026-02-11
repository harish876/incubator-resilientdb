#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <fcntl.h>
#include <iostream>
#include <string>
#include <thread>
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

Endpoint ConnectEndpoint(struct rdma_cm_id* id,
                         const connect_info& local_info,
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
  if (argc < 3) {
    std::cout << "Usage: " << argv[0]
              << " server <bind_ip> [port]\n"
              << "       " << argv[0]
              << " client <server_ip> [port] [message] [count]\n";
    return 0;
  }

  const std::string mode = argv[1];
  const std::string ip = argv[2];
  const int port = (argc >= 4) ? std::stoi(argv[3]) : 9999;

  char* ring_mem = static_cast<char*>(GetMagicBuffer(kBufferLen));
  if (!ring_mem) {
    std::cerr << "Failed to allocate magic buffer" << std::endl;
    return 1;
  }

  Endpoint endpoint;
  struct ibv_qp_init_attr attr;

  if (mode == "server") {
    ServerRDMA server(const_cast<char*>(ip.c_str()), port);
    struct ibv_mr* ring_mr =
        ibv_reg_mr(server.getPD(), ring_mem, kBufferLen * 2,
                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                       IBV_ACCESS_REMOTE_READ);
    if (!ring_mr) {
      std::cerr << "Failed to register ring MR" << std::endl;
      return 1;
    }

    BufferContext ctx{reinterpret_cast<uint64_t>(ring_mr->addr), ring_mr->rkey,
                      kBufferLen};
    connect_info info{};
    info.code = kExperimentCode;
    info.ctx = ctx;

    attr = prepare_qp(server.getPD(), kMaxSendWr, kMaxRecvWr, false);

    char* send_mem = static_cast<char*>(aligned_alloc(4096, 4096));
    if (!send_mem) {
      std::cerr << "Failed to allocate send buffer" << std::endl;
      return 1;
    }
    struct ibv_mr* send_mr =
        ibv_reg_mr(server.getPD(), send_mem, 4096, IBV_ACCESS_LOCAL_WRITE);
    if (!send_mr) {
      std::cerr << "Failed to register send MR" << std::endl;
      return 1;
    }
    Region send_region{0, send_mem, 0, send_mr->lkey};

    std::cout << "[Server] Ready on " << ip << ":" << port << std::endl;

    while (true) {
      MagicRingBuffer local_buffer(ring_mr, log2_pow2(kBufferLen), false);

      endpoint = AcceptEndpoint(server, info, attr);
      if (endpoint.peer_info.ctx.length == 0) {
        std::cerr << "Missing client buffer info" << std::endl;
        return 1;
      }

      MagicRemoteBuffer remote_buffer(endpoint.peer_info.ctx);
      CircularNotifyReceiver receiver(endpoint.ep, &local_buffer);
      CircularConnectionNotify sender(endpoint.ep, &remote_buffer);

      int orig_flags = fcntl(endpoint.ep->id->channel->fd, F_GETFL, 0);
      if (orig_flags >= 0) {
        fcntl(endpoint.ep->id->channel->fd, F_SETFL, orig_flags | O_NONBLOCK);
      }

      bool disconnected = false;
      while (!disconnected) {
        std::vector<Region> recvs;
        if (receiver.Receive(recvs) == 0) {
          struct rdma_cm_event* event = nullptr;
          if (rdma_get_cm_event(endpoint.ep->id->channel, &event) == 0) {
            if (event->event == RDMA_CM_EVENT_DISCONNECTED) {
              disconnected = true;
            }
            rdma_ack_cm_event(event);
          } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
            perror("rdma_get_cm_event");
            return 1;
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
          continue;
        }

        for (auto& recv : recvs) {
          uint32_t freed_bytes =
              *reinterpret_cast<const uint32_t*>(recv.addr);
          sender.AckSentBytes(freed_bytes);
          if (recv.length > sizeof(uint32_t)) {
            const char* payload = recv.addr + sizeof(uint32_t);
            const uint32_t payload_len = recv.length - sizeof(uint32_t);
            std::string message(payload, payload + payload_len);
            std::cout << "[Server] recv: " << message << std::endl;
          }
          receiver.FreeReceive(recv);
          uint32_t reply_freed = receiver.GetFreedReceiveBytes();
          *reinterpret_cast<uint32_t*>(send_region.addr) = reply_freed;
          send_region.length = sizeof(uint32_t);
          uint64_t send_id = sender.SendAsync(send_region);
          sender.WaitSend(send_id);
        }
      }

      if (orig_flags >= 0) {
        fcntl(endpoint.ep->id->channel->fd, F_SETFL, orig_flags);
      }
      rdma_disconnect(endpoint.ep->id);
      delete endpoint.ep;
      endpoint.ep = nullptr;
    }
  }

  if (mode == "client") {
    struct rdma_cm_id* id = ClientRDMA::sendConnectRequest(
        const_cast<char*>(ip.c_str()), port);
    if (!id) {
      std::cerr << "Failed to resolve address" << std::endl;
      return 1;
    }
    if (!id->pd) {
      id->pd = ibv_alloc_pd(id->verbs);
    }

    struct ibv_mr* ring_mr =
        ibv_reg_mr(id->pd, ring_mem, kBufferLen * 2,
                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                       IBV_ACCESS_REMOTE_READ);
    if (!ring_mr) {
      std::cerr << "Failed to register ring MR" << std::endl;
      return 1;
    }

    MagicRingBuffer local_buffer(ring_mr, log2_pow2(kBufferLen), false);
    connect_info info{};
    info.code = kExperimentCode;
    info.ctx = local_buffer.GetContext();

    attr = prepare_qp(id->pd, kMaxSendWr, kMaxRecvWr, false);
    endpoint = ConnectEndpoint(id, info, attr);

    if (endpoint.peer_info.ctx.length == 0) {
      std::cerr << "Missing server buffer info" << std::endl;
      return 1;
    }
    MagicRemoteBuffer remote_buffer(endpoint.peer_info.ctx);
    CircularNotifyReceiver receiver(endpoint.ep, &local_buffer);
    CircularConnectionNotify sender(endpoint.ep, &remote_buffer);

    char* send_mem = static_cast<char*>(aligned_alloc(4096, 4096));
    if (!send_mem) {
      std::cerr << "Failed to allocate send buffer" << std::endl;
      return 1;
    }
    struct ibv_mr* send_mr =
        ibv_reg_mr(id->pd, send_mem, 4096, IBV_ACCESS_LOCAL_WRITE);
    if (!send_mr) {
      std::cerr << "Failed to register send MR" << std::endl;
      return 1;
    }
    Region send_region{0, send_mem, 0, send_mr->lkey};

    std::string message = (argc >= 5) ? argv[4] : "hello";
    uint32_t msg_len = static_cast<uint32_t>(message.size());
    if (msg_len > kMaxPayload) {
      msg_len = kMaxPayload;
    }
    std::memcpy(send_region.addr + sizeof(uint32_t), message.data(), msg_len);
    int count = (argc >= 6) ? std::stoi(argv[5]) : 1;
    for (int i = 0; i < count; ++i) {
      uint32_t freed_bytes = receiver.GetFreedReceiveBytes();
      *reinterpret_cast<uint32_t*>(send_region.addr) = freed_bytes;
      send_region.length = sizeof(uint32_t) + msg_len;
      uint64_t id_send = sender.SendAsync(send_region);
      sender.WaitSend(id_send);
      std::vector<Region> recvs;
      while (receiver.Receive(recvs) == 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      for (auto& recv : recvs) {
        uint32_t ack_bytes =
            *reinterpret_cast<const uint32_t*>(recv.addr);
        sender.AckSentBytes(ack_bytes);
        receiver.FreeReceive(recv);
      }
    }
    return 0;
  }

  std::cerr << "Unknown mode: " << mode << std::endl;
  return 1;
}
