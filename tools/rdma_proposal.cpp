#include <rdmapp/rdmapp.h>

#include <array>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <thread>

#include "acceptor.h"
#include "connector.h"

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
  // Fill slots[0..n-1] with first n free slot ids. Returns number filled (0 to
  // n).
  static int find_first_n_free_slots(const uint8_t* bitmap, int n, int* slots) {
    int count = 0;
    for (int i = 0; i < kNumSlots && count < n; ++i) {
      if (bitmap[i / 8] & (1u << (i % 8))) slots[count++] = i;
    }
    return count;
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
static constexpr int kBatchSize = 100;

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

  // 1 read + N writes per round. Keeps doing rounds until exactly n writes are
  // done.
  // TODO: Needs more work. Specifically batch posting receieves
  rdmapp::task<void> WriteN(const void* data, size_t len, int n) {
    if (n <= 0) co_return;
    if (len > SlotTable::kSlotSize) len = SlotTable::kSlotSize;
    std::memcpy(payload_buf, data, len);

    static constexpr int kMaxBatch = 64;
    int slots[kMaxBatch];
    rdmapp::remote_mr bitmap_region(
        static_cast<char*>(base) + 0,
        static_cast<uint32_t>(SlotTable::kBitmapSize), rkey);

    int total_done = 0;
    while (total_done < n) {
      int remaining = n - total_done;
      int want = (remaining < kMaxBatch) ? remaining : kMaxBatch;
      co_await qp->read(bitmap_region, bitmap_buf, SlotTable::kBitmapSize);
      int k = SlotTable::find_first_n_free_slots(bitmap_buf, want, slots);
      while (k == 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        co_await qp->read(bitmap_region, bitmap_buf, SlotTable::kBitmapSize);
        k = SlotTable::find_first_n_free_slots(bitmap_buf, want, slots);
      }
      for (int i = 0; i < k; ++i) {
        rdmapp::remote_mr slot_region(
            static_cast<char*>(base) + SlotTable::slot_offset(slots[i]),
            static_cast<uint32_t>(SlotTable::kSlotSize), rkey);
        co_await qp->write_with_imm(slot_region, payload_buf, len,
                                    static_cast<uint32_t>(slots[i]));
      }
      total_done += k;
    }
    co_return;
  }
};

rdmapp::task<void> server(rdmapp::acceptor& acceptor,
                          std::shared_ptr<rdmapp::pd> pd) {
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
  std::cout
      << "[Server] QP established; data plane: recv loop (slot table + bitmap)"
      << std::endl;

  char dummy_recv_buf[1];
  int count = 0;
  while (true) {
    auto [len, imm] = co_await qp->recv(dummy_recv_buf, sizeof(dummy_recv_buf));
    ++count;
    uint32_t slot_id = imm.has_value() ? imm.value() : 0u;
    if (slot_id >= static_cast<uint32_t>(SlotTable::kNumSlots)) slot_id = 0;
    int sid = static_cast<int>(slot_id);
    SlotTable::set_slot_in_use(bitmap, sid);
    const char* slot_data = buffer->data() + SlotTable::slot_offset(sid);
    std::cout << "[Server] recv #" << count << " slot=" << slot_id << " data=\""
              << slot_data << "\"" << std::endl;
    if (kServerWorkMs > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(kServerWorkMs));
    }
    SlotTable::set_slot_free(bitmap, sid);
  }
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
  auto cq = std::make_shared<rdmapp::cq>(device);
  auto cq_poller = std::make_shared<rdmapp::cq_poller>(cq);
  auto loop = rdmapp::socket::event_loop::new_loop();
  auto looper = std::thread([loop]() { loop->loop(); });
  if (argc == 2) {
    rdmapp::acceptor acceptor(loop, std::stoi(argv[1]), pd, cq);
    server(acceptor, pd);
  } else if (argc >= 3) {
    rdmapp::connector connector(loop, argv[1], std::stoi(argv[2]), pd, cq);
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
