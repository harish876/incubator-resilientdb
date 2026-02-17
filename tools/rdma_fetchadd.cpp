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
  static constexpr size_t kTailSize = sizeof(uint64_t);

  static constexpr size_t kSlotTableSize =
      kTailSize + static_cast<size_t>(kNumSlots) * kSlotSize;

  static size_t slot_offset(int slot_id) {
    return kTailSize + static_cast<size_t>(slot_id) * kSlotSize;
  }
};

static constexpr int kNumDataTransfers = 10;
static constexpr int kServerWorkMs = 0;

struct FetchAddWriter {
  std::shared_ptr<rdmapp::qp> qp;
  void* base = nullptr;
  uint32_t rkey = 0;
  char payload_buf[SlotTable::kSlotSize];

  rdmapp::task<void> Write(const void* data, size_t len) {
    if (len > SlotTable::kSlotSize) len = SlotTable::kSlotSize;
    std::memcpy(payload_buf, data, len);

    // Remote tail counter at offset 0
    rdmapp::remote_mr tail_region(
        static_cast<char*>(base),
        sizeof(uint64_t),
        rkey);

    uint64_t ticket = 0;

    // Atomic fetch-and-add (returns old value in ticket)
    co_await qp->fetch_and_add(tail_region, &ticket, sizeof(ticket), 1);

    int slot = static_cast<int>(ticket % SlotTable::kNumSlots);

    rdmapp::remote_mr slot_region(
        static_cast<char*>(base) + SlotTable::slot_offset(slot),
        static_cast<uint32_t>(SlotTable::kSlotSize),
        rkey);

    co_await qp->write_with_imm(slot_region,
                                payload_buf,
                                len,
                                static_cast<uint32_t>(slot));

    co_return;
  }
};

rdmapp::task<void> server(rdmapp::acceptor& acceptor,
                          std::shared_ptr<rdmapp::pd> pd) {

  auto buffer =
      std::make_shared<std::array<char, SlotTable::kSlotTableSize>>();

  std::memset(buffer->data(), 0, buffer->size());

  // Initialize tail counter
  *reinterpret_cast<uint64_t*>(buffer->data()) = 0;

  auto local_mr =
      std::make_shared<rdmapp::local_mr>(
          pd->reg_mr(buffer->data(), buffer->size()));

  auto mr_serialized = local_mr->serialize();

  acceptor.set_server_user_data(
      std::vector<uint8_t>(mr_serialized.begin(), mr_serialized.end()));

  std::cout << "[Server] MR sent (FetchAdd ring buffer)"
            << std::endl;

  auto qp = co_await acceptor.accept();

  std::cout << "[Server] Connected. Waiting for writes..."
            << std::endl;

  char dummy_recv_buf[1];
  int count = 0;

  while (true) {
    auto [len, imm] =
        co_await qp->recv(dummy_recv_buf, sizeof(dummy_recv_buf));

    ++count;

    uint32_t slot_id = imm.has_value() ? imm.value() : 0u;
    slot_id %= SlotTable::kNumSlots;

    const char* slot_data =
        buffer->data() + SlotTable::slot_offset((int)slot_id);

    std::cout << "[Server] recv #" << count
              << " slot=" << slot_id
              << " data=\"" << slot_data << "\""
              << std::endl;

    if (kServerWorkMs > 0) {
      std::this_thread::sleep_for(
          std::chrono::milliseconds(kServerWorkMs));
    }
  }

  co_return;
}

rdmapp::task<void> client(rdmapp::connector& connector,
                          int num_requests) {

  auto qp = co_await connector.connect();

  auto const& user_data = qp->user_data();

  if (user_data.size() < rdmapp::remote_mr::kSerializedSize) {
    throw std::runtime_error("connect: user_data too small for MR");
  }

  auto remote_mr =
      rdmapp::remote_mr::deserialize(user_data.begin());

  FetchAddWriter writer;
  writer.qp = qp;
  writer.base = remote_mr.addr();
  writer.rkey = remote_mr.rkey();

  std::cout << "[Client] Connected (FetchAdd mode)"
            << std::endl;

  char payload[SlotTable::kSlotSize];
  size_t payload_len =
      static_cast<size_t>(
          snprintf(payload, sizeof(payload), "bench")) + 1;

  auto start = std::chrono::steady_clock::now();

  for (int i = 0; i < num_requests; ++i) {
    co_await writer.Write(payload, payload_len);
  }

  auto end = std::chrono::steady_clock::now();

  double sec =
      std::chrono::duration<double>(end - start).count();

  std::cout << "[Client] "
            << num_requests << " writes in "
            << sec << " s -> "
            << (num_requests / sec)
            << " writes/s"
            << std::endl;

  co_return;
}

int main(int argc, char* argv[]) {

  auto device =
      std::make_shared<rdmapp::device>(0, 1, 3);

  auto pd =
      std::make_shared<rdmapp::pd>(device);

  auto cq =
      std::make_shared<rdmapp::cq>(device);

  auto cq_poller =
      std::make_shared<rdmapp::cq_poller>(cq);

  auto loop =
      rdmapp::socket::event_loop::new_loop();

  auto looper =
      std::thread([loop]() { loop->loop(); });

  if (argc == 2) {
    rdmapp::acceptor acceptor(loop,
                              std::stoi(argv[1]),
                              pd,
                              cq);
    server(acceptor, pd);
  } else if (argc >= 3) {
    rdmapp::connector connector(loop,
                                argv[1],
                                std::stoi(argv[2]),
                                pd,
                                cq);

    int num_requests =
        (argc >= 4) ? std::stoi(argv[3])
                    : kNumDataTransfers;

    client(connector, num_requests);
  } else {
    std::cout
        << "Usage:\n"
        << "  server: " << argv[0] << " <port>\n"
        << "  client: " << argv[0]
        << " <server_ip> <port> [num_requests]\n";
  }

  loop->close();
  looper.join();
  return 0;
}
