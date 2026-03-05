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

#include "acceptor.h"
#include "connector.h"

// -----------------------------
// Shared ring with seq numbers.
// MPSC: many clients (producers), one server (consumer).
//
// Remote layout (server MR):
//   [0..7]      uint64_t tail (ticket allocator)
//   [8..]       slots[N]:
//                slot[i].seq (uint64_t)
//                slot[i].payload[kPayloadSize]
// -----------------------------

struct SharedRing {
  static constexpr int kNumSlots = 1024;          // bump this for fewer stalls
  static constexpr size_t kPayloadSize = 32;      // payload bytes
  static constexpr size_t kSeqSize = sizeof(uint64_t);
  static constexpr size_t kSlotSize = kSeqSize + kPayloadSize;
  static constexpr size_t kTailOffset = 0;
  static constexpr size_t kSlotsOffset = sizeof(uint64_t);
  static constexpr size_t kTotalSize =
      kSlotsOffset + static_cast<size_t>(kNumSlots) * kSlotSize;

  static size_t slot_base(int idx) {
    return kSlotsOffset + static_cast<size_t>(idx) * kSlotSize;
  }
  static size_t slot_seq_off(int idx) { return slot_base(idx) + 0; }
  static size_t slot_payload_off(int idx) { return slot_base(idx) + kSeqSize; }
};

static constexpr int kNumRequestsDefault = 10000;

// Backoff helper (very simple; good enough for project)
static inline void backoff(int &iters) {
  // small spin then sleep
  if (iters < 1000) {
    // CPU relax-ish
    asm volatile("pause" ::: "memory");
  } else {
    std::this_thread::sleep_for(std::chrono::microseconds(50));
  }
  ++iters;
}

struct ClientRingWriter {
  std::shared_ptr<rdmapp::qp> qp;
  void *base = nullptr;
  uint32_t rkey = 0;

  // Local buffers used for RDMA ops
  alignas(8) uint64_t seq_buf = 0;
  alignas(8) uint64_t ticket_buf = 0;
  char payload_buf[SharedRing::kPayloadSize];

  rdmapp::task<void> Write(const void *data, size_t len) {
    if (len > SharedRing::kPayloadSize) len = SharedRing::kPayloadSize;
    std::memset(payload_buf, 0, sizeof(payload_buf));
    std::memcpy(payload_buf, data, len);

    // Remote pointer to tail (atomic target)
    rdmapp::remote_mr tail_mr(static_cast<char *>(base) + SharedRing::kTailOffset,
                             sizeof(uint64_t), rkey);

    // 1) Reserve ticket via atomic fetch_add (delta=1)
    co_await qp->fetch_and_add(tail_mr, &ticket_buf, sizeof(uint64_t), 1);
    uint64_t t = ticket_buf;
    int idx = static_cast<int>(t % SharedRing::kNumSlots);

    // 2) Poll slot.seq until it equals t (slot is free for this ticket)
    rdmapp::remote_mr seq_mr(static_cast<char *>(base) + SharedRing::slot_seq_off(idx),
                            sizeof(uint64_t), rkey);

    int iters = 0;
    while (true) {
      co_await qp->read(seq_mr, &seq_buf, sizeof(uint64_t));
      if (seq_buf == t) break;
      backoff(iters);
    }

    // 3) Write payload first
    rdmapp::remote_mr payload_mr(static_cast<char *>(base) + SharedRing::slot_payload_off(idx),
                                SharedRing::kPayloadSize, rkey);
    co_await qp->write(payload_mr, payload_buf, SharedRing::kPayloadSize);

    // 4) Publish readiness by writing seq = t+1 last
    uint64_t ready = t + 1;
    co_await qp->write(seq_mr, &ready, sizeof(uint64_t));

    // 5) Optional: notify server via IMM (so server doesn't poll)
    // We'll send 1 dummy byte; IMM carries idx.
    char dummy = 0;
    rdmapp::remote_mr doorbell(static_cast<char *>(base) + SharedRing::kSlotsOffset,
                              1, rkey);
    co_await qp->write_with_imm(doorbell, &dummy, 1, static_cast<uint32_t>(idx));

    co_return;
  }
};

rdmapp::task<void> server(rdmapp::acceptor &acceptor,
                          std::shared_ptr<rdmapp::pd> pd) {
  auto buffer = std::make_shared<std::array<char, SharedRing::kTotalSize>>();
  std::memset(buffer->data(), 0, buffer->size());

  // Init tail = 0
  auto *tail_ptr = reinterpret_cast<uint64_t *>(buffer->data() + SharedRing::kTailOffset);
  *tail_ptr = 0;

  // Init slot seqs: slot[i].seq = i
  for (int i = 0; i < SharedRing::kNumSlots; ++i) {
    auto *seq_ptr = reinterpret_cast<uint64_t *>(buffer->data() + SharedRing::slot_seq_off(i));
    *seq_ptr = static_cast<uint64_t>(i);
  }

  auto local_mr = std::make_shared<rdmapp::local_mr>(pd->reg_mr(buffer->data(), buffer->size()));
  auto mr_serialized = local_mr->serialize();
  acceptor.set_server_user_data(
      std::vector<uint8_t>(mr_serialized.begin(), mr_serialized.end()));

  std::cout << "[Server] SharedRing MR sent (N=" << SharedRing::kNumSlots
            << ", payload=" << SharedRing::kPayloadSize << "B)\n";

  auto qp = co_await acceptor.accept();
  std::cout << "[Server] QP established; waiting for notifications...\n";

  uint64_t head = 0;

  char dummy_recv[1];
  while (true) {
    // We receive IMM notifications (idx). This is just a wakeup/hint.
    auto [len, imm] = co_await qp->recv(dummy_recv, sizeof(dummy_recv));
    (void)len;

    // Process in ticket order using head (not using idx directly for correctness).
    int idx = static_cast<int>(head % SharedRing::kNumSlots);
    uint64_t expected_ready = head + 1;

    auto *seq_ptr = reinterpret_cast<uint64_t *>(buffer->data() + SharedRing::slot_seq_off(idx));
    // Wait until producer published (seq == head+1)
    while (*seq_ptr != expected_ready) {
      // tight local spin is fine; could yield if needed
      asm volatile("pause" ::: "memory");
    }

    // Read payload locally
    const char *payload = buffer->data() + SharedRing::slot_payload_off(idx);

    // (Optional) print occasionally
    if ((head % 10000) == 0) {
      std::cout << "[Server] head=" << head << " idx=" << idx
                << " payload=\"" << payload << "\"\n";
    }

    // Mark slot free for next wrap: seq = head + N
    *seq_ptr = head + SharedRing::kNumSlots;

    ++head;
  }

  co_return;
}

rdmapp::task<void> client(rdmapp::connector &connector, int num_requests) {
  auto qp = co_await connector.connect();

  auto const &user_data = qp->user_data();
  if (user_data.size() < rdmapp::remote_mr::kSerializedSize) {
    throw std::runtime_error("connect: user_data too small for MR");
  }
  auto remote = rdmapp::remote_mr::deserialize(user_data.begin());

  ClientRingWriter writer;
  writer.qp = qp;
  writer.base = remote.addr();
  writer.rkey = remote.rkey();

  std::cout << "[Client] MR addr=" << writer.base
            << " len=" << remote.length() << " rkey=" << writer.rkey << "\n";

  char msg[SharedRing::kPayloadSize];
  std::snprintf(msg, sizeof(msg), "bench");

  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < num_requests; ++i) {
    co_await writer.Write(msg, std::strlen(msg) + 1);
  }
  auto end = std::chrono::steady_clock::now();
  double sec = std::chrono::duration<double>(end - start).count();
  std::cout << "[Client] " << num_requests << " ops in " << sec
            << " s -> " << (num_requests / sec) << " ops/s\n";

  double mrps = (num_requests / sec) / 1e6;
  std::cout << "[Client] Throughput: "
            << mrps << " MRPS\n";

  co_return;
}

int main(int argc, char *argv[]) {
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
    int n = (argc >= 4) ? std::stoi(argv[3]) : kNumRequestsDefault;
    client(connector, n);
  } else {
    std::cout << "Usage:\n"
              << "  server: " << argv[0] << " <port>\n"
              << "  client: " << argv[0] << " <server_ip> <port> [num_requests]\n";
  }

  loop->close();
  looper.join();
  return 0;
}