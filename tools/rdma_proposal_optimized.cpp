#include <rdmapp/rdmapp.h>

#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

#include <infiniband/verbs.h>

#include "acceptor.h"
#include "connector.h"

static constexpr int kPrepostRecvCount = 256;

struct SlotTable {
  static constexpr int kNumSlots = 256;
  static constexpr size_t kSlotSize = 8;
  static constexpr size_t kBitmapSize = (kNumSlots + 7) / 8;
  static constexpr size_t kSlotTableSize =
      kBitmapSize + static_cast<size_t>(kNumSlots) * kSlotSize;

  static size_t slot_offset(int slot_id) {
    return kBitmapSize + static_cast<size_t>(slot_id) * kSlotSize;
  }
  static int find_first_free_slot(const uint8_t* bitmap) {
    for (int i = 0; i < kNumSlots; ++i) {
      if (bitmap[i / 8] & (1u << (i % 8))) return i;
    }
    return -1;
  }

  static void set_slot_free(uint8_t* bitmap, int slot_id) {
    bitmap[slot_id / 8] |= (1u << (slot_id % 8));
  }
  static void set_slot_in_use(uint8_t* bitmap, int slot_id) {
    bitmap[slot_id / 8] &= ~(1u << (slot_id % 8));
  }
};

static constexpr int kNumDataTransfers = 10;
static constexpr int kServerWorkMs = 0;

struct SlotTableWriter {
  std::shared_ptr<rdmapp::qp> qp;
  void* base = nullptr;
  uint32_t rkey = 0;
  uint8_t bitmap_buf[SlotTable::kBitmapSize];
  char payload_buf[SlotTable::kSlotSize];

  rdmapp::task<void> Write(const void* data, size_t len) {
    if (len > SlotTable::kSlotSize) len = SlotTable::kSlotSize;
    std::memcpy(payload_buf, data, len);

    rdmapp::remote_mr bitmap_region(
        static_cast<char*>(base) + 0,
        static_cast<uint32_t>(SlotTable::kBitmapSize), rkey);
    co_await qp->read(bitmap_region, bitmap_buf, SlotTable::kBitmapSize);
    int slot = SlotTable::find_first_free_slot(bitmap_buf);
    while (slot < 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      co_await qp->read(bitmap_region, bitmap_buf, SlotTable::kBitmapSize);
      slot = SlotTable::find_first_free_slot(bitmap_buf);
    }
    rdmapp::remote_mr slot_region(
        static_cast<char*>(base) + SlotTable::slot_offset(slot),
        static_cast<uint32_t>(SlotTable::kSlotSize), rkey);
    co_await qp->write_with_imm(slot_region, payload_buf, len,
                                static_cast<uint32_t>(slot));
    co_return;
  }
};

rdmapp::task<void> server(rdmapp::acceptor& acceptor,
                          std::shared_ptr<rdmapp::pd> pd,
                          std::shared_ptr<rdmapp::cq> recv_cq) {
  auto buffer = std::make_shared<std::array<char, SlotTable::kSlotTableSize>>();
  std::memset(buffer->data(), 0, buffer->size());
  uint8_t* bitmap = reinterpret_cast<uint8_t*>(buffer->data());
  for (int i = 0; i < SlotTable::kNumSlots; ++i)
    SlotTable::set_slot_free(bitmap, i);

  auto local_mr = std::make_shared<rdmapp::local_mr>(
      pd->reg_mr(buffer->data(), buffer->size()));
  auto mr_serialized = local_mr->serialize();
  acceptor.set_server_user_data(
      std::vector<uint8_t>(mr_serialized.begin(), mr_serialized.end()));
  std::cout << "[Server] Control plane: slot-table MR sent ("
            << SlotTable::kNumSlots << " slots, " << SlotTable::kSlotSize
            << " B/slot)" << std::endl;

  auto qp = co_await acceptor.accept();
  std::cout << "[Server] QP established; preposting " << kPrepostRecvCount
            << " recvs (reuse: repost after each completion)" << std::endl;

  using RecvBuf = std::array<char, kPrepostRecvCount>;
  auto recv_buf = std::make_shared<RecvBuf>();
  std::memset(recv_buf->data(), 0, recv_buf->size());
  auto recv_mr = std::make_shared<rdmapp::local_mr>(
      pd->reg_mr(recv_buf->data(), recv_buf->size()));

  std::vector<struct ibv_sge> sges(kPrepostRecvCount);
  std::vector<struct ibv_recv_wr> wrs(kPrepostRecvCount);
  for (int i = 0; i < kPrepostRecvCount; ++i) {
    std::memset(&sges[i], 0, sizeof(sges[i]));
    sges[i].addr = reinterpret_cast<uint64_t>(recv_buf->data() + i);
    sges[i].length = 1;
    sges[i].lkey = recv_mr->lkey();
    std::memset(&wrs[i], 0, sizeof(wrs[i]));
    wrs[i].wr_id = static_cast<uint64_t>(i);
    wrs[i].next = (i + 1 < kPrepostRecvCount) ? &wrs[i + 1] : nullptr;
    wrs[i].sg_list = &sges[i];
    wrs[i].num_sge = 1;
  }

  struct ibv_recv_wr* bad_wr = nullptr;
  qp->post_recv(wrs[0], bad_wr);
  if (bad_wr != nullptr) {
    throw std::runtime_error("prepost recv failed");
  }

  std::atomic<uint64_t> count{0};
  std::atomic<bool> stop{false};

  std::thread recv_thread([qp, recv_cq, buffer, bitmap, &wrs, &count, &stop]() {
    std::vector<struct ibv_wc> wc_vec(64);
    while (!stop.load(std::memory_order_acquire)) {
      size_t n = recv_cq->poll(wc_vec);
      for (size_t i = 0; i < n; ++i) {
        const struct ibv_wc& wc = wc_vec[i];
        if (wc.opcode != IBV_WC_RECV && wc.opcode != IBV_WC_RECV_RDMA_WITH_IMM)
          continue;
        if (wc.status != IBV_WC_SUCCESS) {
          std::cerr << "[Server] recv completion error status=" << wc.status
                    << std::endl;
          continue;
        }

        uint32_t slot_id = (wc.wc_flags & IBV_WC_WITH_IMM) ? wc.imm_data : 0;
        if (slot_id >= static_cast<uint32_t>(SlotTable::kNumSlots)) slot_id = 0;
        int sid = static_cast<int>(slot_id);
        SlotTable::set_slot_in_use(bitmap, sid);
        const char* slot_data = buffer->data() + SlotTable::slot_offset(sid);
        uint64_t c = count.fetch_add(1, std::memory_order_relaxed) + 1;
        std::cout << "[Server] recv #" << c << " slot=" << slot_id << " data=\""
                  << slot_data << "\"" << std::endl;
        if (kServerWorkMs > 0) {
          std::this_thread::sleep_for(
              std::chrono::milliseconds(kServerWorkMs));
        }
        SlotTable::set_slot_free(bitmap, sid);

        int idx = static_cast<int>(wc.wr_id);
        if (idx >= 0 && idx < kPrepostRecvCount) {
          wrs[idx].next = nullptr;
          struct ibv_recv_wr* repost_bad = nullptr;
          qp->post_recv(wrs[idx], repost_bad);
          if (repost_bad != nullptr) {
            std::cerr << "[Server] repost recv failed" << std::endl;
          }
        }
      }
    }
  });

  while (true) {
    std::this_thread::sleep_for(std::chrono::hours(1));
  }
  stop.store(true, std::memory_order_release);
  recv_thread.join();
  co_return;
}

rdmapp::task<void> client(rdmapp::connector& connector, int num_requests) {
  auto qp = co_await connector.connect();

  auto const& user_data = qp->user_data();
  if (user_data.size() < rdmapp::remote_mr::kSerializedSize) {
    throw std::runtime_error("connect: user_data too small for MR");
  }
  auto remote_mr = rdmapp::remote_mr::deserialize(user_data.begin());
  SlotTableWriter writer;
  writer.qp = qp;
  writer.base = remote_mr.addr();
  writer.rkey = remote_mr.rkey();
  std::cout << "[Client] Control plane: slot-table MR (addr=" << writer.base
            << " len=" << remote_mr.length() << " rkey=" << writer.rkey << ")"
            << std::endl;

  char payload[SlotTable::kSlotSize];
  size_t payload_len =
      static_cast<size_t>(snprintf(payload, sizeof(payload), "bench")) + 1;

  /** NORMAL THROUGHPUT */
  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < num_requests; ++i) {
    co_await writer.Write(payload, payload_len);
  }
  auto end = std::chrono::steady_clock::now();
  double sec = std::chrono::duration<double>(end - start).count();
  std::cout << "[Client] benchmark (Write): " << num_requests << " writes in "
            << sec << " s -> " << (num_requests / sec) << " writes/s"
            << std::endl;
  co_return;
}

int main(int argc, char* argv[]) {
  auto device = std::make_shared<rdmapp::device>(0, 1, 3);
  auto pd = std::make_shared<rdmapp::pd>(device);
  auto recv_cq = std::make_shared<rdmapp::cq>(device, kPrepostRecvCount * 2);
  auto send_cq = std::make_shared<rdmapp::cq>(device, 128);
  auto cq_poller = std::make_shared<rdmapp::cq_poller>(send_cq);
  auto loop = rdmapp::socket::event_loop::new_loop();
  auto looper = std::thread([loop]() { loop->loop(); });
  if (argc == 2) {
    rdmapp::acceptor acceptor(loop, std::stoi(argv[1]), pd, recv_cq, send_cq);
    server(acceptor, pd, recv_cq);
  } else if (argc >= 3) {
    rdmapp::connector connector(loop, argv[1], std::stoi(argv[2]), pd, recv_cq,
                                send_cq);
    int num_requests = (argc >= 4) ? std::stoi(argv[3]) : kNumDataTransfers;
    client(connector, num_requests);
  } else {
    std::cout << "Usage: server: " << argv[0] << " <port>\n"
              << "       client: " << argv[0]
              << " <server_ip> <port> [num_requests]\n"
              << "  num_requests: if set, run benchmark (writes/s); else demo ("
              << kNumDataTransfers << " msgs)." << std::endl;
  }
  loop->close();
  looper.join();
  return 0;
}
