#include <infiniband/verbs.h>

#include <chrono>
#include <iostream>  // std::cout

#include "ServerRDMA.hpp"
#include "VerbsEP.hpp"
#include "com/magic_ring.hpp"
#include "com/protocols.hpp"
#include "com/reverse_ring.hpp"
#include "com/utils.hpp"
#include "cxxopts.hpp"

#ifdef MAPDM
#include <infiniband/mlx5dv.h>
#endif

#define DM_MEM_SIZE (8)

#define MIN(a, b) (((a) < (b)) ? (a) : (b))
#define CHECK_BIT(var, pos) ((var) & (1 << (pos)))

std::vector<VerbsEP*> cons;
std::vector<void*> bufs;

char* memory = NULL;
uint64_t mem_offset = 0;
uint64_t mem_size = 0;

int connect_client(ServerRDMA* server, uint64_t cid,
                   struct ibv_qp_init_attr attr, void* my_info,
                   uint32_t recv_batch, bool recv_with_data) {
  struct ibv_pd* pd = server->getPD();
  struct rdma_cm_id* id;
  void* buf = NULL;

  std::tie(id, buf) = server->get_connect_request();
  bufs.push_back(buf);

  uint32_t max_recv_size = attr.cap.max_recv_wr;
  if (attr.srq) attr.cap.max_recv_wr = 0;

  if (rdma_create_qp(id, pd, &attr)) {
    perror("rdma_create_qp");
    exit(1);
  }
  id->context = (void*)cid;

  attr.cap.max_recv_wr = max_recv_size;
  cons.push_back(new VerbsEP(id, attr, recv_batch, recv_with_data));

  struct rdma_conn_param conn_param;
  memset(&conn_param, 0, sizeof(conn_param));
  conn_param.responder_resources = 16;  // up to 16 reads
  conn_param.initiator_depth = 16;
  conn_param.retry_count = 3;
  conn_param.rnr_retry_count = 3;
  conn_param.private_data = my_info;
  conn_param.private_data_len = sizeof(connect_info);

  if (rdma_accept(id, &conn_param)) {
    printf(" failed to accept\n");
    exit(1);
  }
  printf("Accepted one\n");

  return 0;
}

// shoud work for latency and bw
void p2p_test(SendCommunicator& sc, ReceiveCommunicator& rc,
              std::vector<Region>& send_regions, bool with_copy = false) {
  if (with_copy) printf(">>>>>>>>>Start test with copy\n");
  std::vector<Region> recvs;
  uint64_t latest_id = 0;
  uint64_t pending_id = 1;

  uint32_t can_send = send_regions.size();
  uint32_t max_sends = can_send;
  uint32_t i = 0;

  uint32_t message_size = sc.ReqSize(send_regions[0]);

  printf("Message size %u and outstanding is %u\n", message_size, max_sends);

  while (true) {
    int ret = 0;
    for (uint32_t i = 0; i < 5; i++) ret += rc.Receive(recvs);

    if (ret) {
      //   printf("Received %d\n",ret);
      uint32_t freedBytes = 0;
      for (auto& recv : recvs) {
        freedBytes += *(uint32_t*)(recv.addr);

        if (with_copy) {
          memcpy(memory + mem_offset, recv.addr, recv.length);
          mem_offset = (mem_offset + recv.length) % mem_size;
        }
        rc.FreeReceive(recv);
      }
      // the other side will send us its FreedReceivebytes.
      sc.AckSentBytes(freedBytes);
      // printf("freedBytes %u \n",freedBytes);
      // process receive buffer
      recvs.clear();
    }
    while (pending_id <= latest_id && sc.TestSend(pending_id)) {
      pending_id += 1;
      can_send += 1;
    }
    if (can_send) {
      uint32_t bytes = rc.GetFreedReceiveBytes();
      if (bytes) {
        *(uint32_t*)send_regions[i].addr =
            bytes;  // send freed bytes as a message
        latest_id = sc.SendAsync(send_regions[i]);  // send pong
        // printf("send %u \n",bytes);
        can_send--;
        i = ((i + 1) % max_sends);
      }
    }
  }
}

// for shared receive tests with atomics.
// shoud work for latency and bw
void shared_test(ReceiveCommunicator& rc, bool with_copy = false) {
  if (with_copy) printf(">>>>>>>>>Start test with copy\n");
  std::vector<Region> recvs;

  while (true) {
    int ret = 0;

    for (uint32_t i = 0; i < 5; i++) ret += rc.Receive(recvs);

    if (ret) {
      //  printf("Received %d\n",ret);
      for (auto& recv : recvs) {
        if (with_copy) {
          memcpy(memory + mem_offset, recv.addr, recv.length);
          mem_offset = (mem_offset + recv.length) % mem_size;
        }

        rc.FreeReceive(recv);
      }
      // process receive buffer
      recvs.clear();

      uint32_t freed_bytes =
          rc.GetFreedReceiveBytes();  // send freed bytes as a message
    }
  }
}

void latency_shared_test(SendCommunicator& sc, Region& send_region,
                         uint32_t warmup, uint32_t test) {
  using namespace std::chrono;
  {  // it is to sleep a bit
    std::chrono::seconds delay(1);
    auto next_print = high_resolution_clock::now() + delay;
    while (high_resolution_clock::now() < next_print) {
      // do nothing
    }
  }

  std::vector<float> latency_rtt;
  latency_rtt.reserve(test);

  uint32_t message_size = sc.ReqSize(send_region);

  printf("Message size %u\n", message_size);

  for (uint32_t i = 0; i < warmup; i++) {
    uint64_t id = sc.SendAsync(send_region);
    // std::chrono::seconds two(2);
    //   std::this_thread::sleep_for(two);
    sc.WaitSend(id);
    //    printf("Received completion %d\n",i);
  }
  for (uint32_t i = 0; i < test; i++) {
    auto t1 = high_resolution_clock::now();

    uint64_t id = sc.SendAsync(send_region);
    sc.WaitSend(id);
    auto t2 = high_resolution_clock::now();

    //   printf("Received a test message %d\n",i);

    float lat = duration_cast<nanoseconds>(t2 - t1).count() / 1000.0;
    latency_rtt.push_back(lat);
  }

  std::sort(latency_rtt.begin(), latency_rtt.end());
  int q025 = (int)(test * 0.025);
  int q050 = (int)(test * 0.05);
  int q500 = (int)(test * 0.5);
  int q950 = (int)(test * 0.950);
  int q975 = (int)(test * 0.975);
  printf("latency_rtt {%f-%f-%f-%f-%f} us \n", latency_rtt[q025],
         latency_rtt[q050], latency_rtt[q500], latency_rtt[q950],
         latency_rtt[q975]);
  for (auto f : latency_rtt) {
    printf("%f ", f);
  }
  printf("\n");
}

void bw_shared_test(SendCommunicator& sc, std::vector<Region>& send_regions,
                    uint32_t measure) {
  using namespace std::chrono;

  std::chrono::seconds delay(5);
  auto next_print = high_resolution_clock::now() + delay;

  uint32_t message_size = sc.ReqSize(send_regions[0]);

  const uint32_t max_sends = send_regions.size();
  uint32_t cur = 1 % max_sends;

  uint32_t outstaning = 1;
  uint32_t done = 0;

  uint64_t latest_id = sc.SendAsync(send_regions[cur]);
  uint64_t pending_id = latest_id;

  while (measure) {
    if (outstaning < max_sends) {
      outstaning++;
      latest_id = sc.SendAsync(send_regions[cur]);
      cur = (cur + 1) % max_sends;
    }

    while (pending_id <= latest_id && sc.TestSend(pending_id)) {
      pending_id +=
          message_size;  // I use the trick that ids here are head pointers.
      outstaning -= 1;
      done += 1;
    }

    if (high_resolution_clock::now() > next_print) {
      printf("[BW test] completed %u sends in 5 sec\n", done);
      done = 0;
      measure--;
      next_print = high_resolution_clock::now() + delay;
    }
  }
}

// ParseResult class: used for the parsing of line arguments
cxxopts::ParseResult parse(int argc, char* argv[]) {
  cxxopts::Options options(argv[0], "Sender of UD test");
  options.positional_help("[optional args]").show_positional_help();

  try {
    options.add_options()("a,address", "IPADDR", cxxopts::value<std::string>(),
                          "IP")(
        "t,test", "test iterations",
        cxxopts::value<uint32_t>()->default_value("1024"),
        "N")("send-size", "The IB maximum send size",
             cxxopts::value<uint32_t>()->default_value(std::to_string(128)),
             "N")  // this is the size of the queue request on the device
        ("recv-size", "The IB maximum receive size",
         cxxopts::value<uint32_t>()->default_value(std::to_string(128)),
         "N")  // this is the size of the queue request on the device
        ("clients", "clients to expect",
         cxxopts::value<uint32_t>()->default_value(std::to_string(1)),
         "N")  // this is the size of the queue request on the device
        ("e,exp", "experiment id",
         cxxopts::value<uint32_t>()->default_value("0"),
         "N")("s,size", "buffer size. For send and for recv.",
              cxxopts::value<uint32_t>()->default_value(std::to_string(8)),
              "N")("rsize", "recv size. For send/recv comm.",
                   cxxopts::value<uint32_t>()->default_value(std::to_string(8)),
                   "N")("with-dm-atomics", "use device memory for atomics.")(
            "with-dm-reads", "use device memory for reads.")(
            "with-srq", "use shared receive queues")(
            "o,outstaning",
            "total number of outstanding. Used for BW experiments",
            cxxopts::value<uint32_t>()->default_value(std::to_string(8)), "N")(
            "m,memory", "Size of ring buffers in bytes. must be page aligned",
            cxxopts::value<uint32_t>()->default_value(std::to_string(4096)),
            "N")("with-copy", "copy messages",
                 cxxopts::value<uint64_t>()->default_value(std::to_string(0)),
                 "N")("send-sges", "number of send sges",
                      cxxopts::value<uint32_t>()->default_value("1"),
                      "N")("with-map-dm", "use new driver to map DM memory")(
            "help", "Print help");

    auto result = options.parse(argc, argv);
    if (result.count("address") == 0) {
      std::cout << "No address is provided" << options.help({""}) << std::endl;
      exit(0);
    }
    if (result.count("help")) {
      std::cout << options.help({""}) << std::endl;
      exit(0);
    }

    return result;

  } catch (const cxxopts::OptionException& e) {
    std::cout << "error parsing options: " << e.what() << std::endl;
    std::cout << options.help({""}) << std::endl;
    exit(1);
  }
}

static inline uint32_t log2(const uint32_t x) {
  uint32_t y;
  asm("\tbsr %1, %0\n" : "=r"(y) : "r"(x));
  return y;
}

Buffer* create_buffer(uint32_t experiment, ibv_mr* mr, uint32_t len) {
  if (experiment == 4) {
    return new ReverseRingBuffer(mr, log2(len));
  }
  bool with_zero = experiment == 2 || experiment > 5;
  if (experiment == 11) with_zero = false;

  return new MagicRingBuffer(mr, log2(len), with_zero);
}

RemoteBuffer* create_remote_buffer(uint32_t experiment, BufferContext& bc) {
  if (experiment == 4) {
    return new ReverseRemoteBuffer(bc);
  }

  return new MagicRemoteBuffer(bc);
}

int main(int argc, char* argv[]) {
  // parse the arguments and creates a dictionary which maps arguments to values
  auto allparams = parse(argc, argv);

  std::string ip =
      allparams["address"].as<std::string>();  // "192.168.1.20"; .c_str()
  int port = 9999;

  ServerRDMA* server = new ServerRDMA(const_cast<char*>(ip.c_str()), port);

  uint32_t experiment = allparams["exp"].as<uint32_t>();

  uint32_t test = allparams["test"].as<uint32_t>();
  uint32_t max_recv_size = allparams["recv-size"].as<uint32_t>();
  uint32_t max_send_size = allparams["send-size"].as<uint32_t>();
  uint32_t data_size = allparams["size"].as<uint32_t>();

  bool with_srq = allparams.count("with-srq");

  uint32_t total_clients = allparams["clients"].as<uint32_t>();

  uint32_t buffer_len = allparams["memory"].as<uint32_t>();
  printf("Create Magic memory of %u bytes\n", buffer_len);

  mem_size = allparams["with-copy"].as<uint64_t>();
  bool with_copy = mem_size != 0;
  if (with_copy) {
    memory = (char*)GetMagicBuffer(mem_size);
  }

  char* mem = (char*)GetMagicBuffer(buffer_len);
  struct ibv_mr* mr =
      ibv_reg_mr(server->getPD(), mem, buffer_len * 2,
                 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                     IBV_ACCESS_REMOTE_READ);

  printf("MagicBuffer at %p and it repeats at %p\n", mem, mem + buffer_len);

  // create local buffer
  Buffer* lb = create_buffer(experiment, mr, buffer_len);
  BufferContext bc = lb->GetContext();

  // memory for mailboxes.
  char* mem2 = (char*)aligned_alloc(
      4096, sizeof(uint64_t) * (total_clients + 1));  // +1 is for faa
  memset(mem2, 0, sizeof(uint64_t) * (total_clients + 1));
  struct ibv_mr* mr2 =
      ibv_reg_mr(server->getPD(), mem2, sizeof(uint64_t) * (total_clients + 1),
                 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                     IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC);

  struct ibv_dm* dm = NULL;
  struct ibv_mr* dm_mr = NULL;

  if (allparams.count("with-dm-atomics") || allparams.count("with-dm-reads")) {
    printf("use device memory\n");
    uint32_t dmlen = 0;
    if (allparams.count("with-dm-atomics") &&
        allparams.count("with-dm-reads")) {
      dmlen = 2 * DM_MEM_SIZE;
    } else {
      dmlen = DM_MEM_SIZE;
    }
    struct ibv_alloc_dm_attr attr = {
        dmlen, 0, 0};  // DM_MEM_SIZE bytes. TODO check alignment
    dm = ibv_alloc_dm(server->getPD()->context, &attr);
    char temp[dmlen];
    memset(temp, 0, dmlen);
    if (ibv_memcpy_to_dm(dm, 0, temp, dmlen)) {
      printf("failed to zero dev memory\n");
    }

    // I always use first 8 bytes for HEAD reads, and next 8 bytes for FAAs
    dm_mr = ibv_reg_dm_mr(server->getPD(), dm, 0, dmlen,
                          IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                              IBV_ACCESS_REMOTE_READ |
                              IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_ZERO_BASED);
    if (!dm_mr) {
      printf("failed to reg\n");
    }
    printf("DEv memory is at rkey %u\n", dm_mr->rkey);
  }

  connect_info info;
  info.code = experiment;
  info.ctx = bc;
  info.rkey_magic = mr2->rkey;
  info.addr_magic = (uint64_t)mr2->addr;
  info.addr_magic2 = (uint64_t)mr2->addr;

  if (dm_mr != NULL) {
    info.dm_rkey = dm_mr->rkey;
    info.addr_magic2 = 0;  // 1 only reads. 2 only faas. 3-both
    if (allparams.count("with-dm-reads")) info.addr_magic2 += 1;
    if (allparams.count("with-dm-atomics")) info.addr_magic2 += 2;
  } else {
    info.dm_rkey = 0;
  }

  printf("My info: %lx %u . Magic at %lx %u \n", info.ctx.addr, info.ctx.rkey,
         info.addr_magic, info.rkey_magic);

  struct ibv_qp_init_attr attr =
      prepare_qp(server->getPD(), max_send_size, max_recv_size, with_srq);

  attr.cap.max_send_sge = allparams["send-sges"].as<uint32_t>();
  bool recv_with_data = (experiment == 0 || experiment == 11);

  for (uint32_t i = 0; i < total_clients; i++) {
    if (experiment == 7 || experiment == 8 || experiment == 9 ||
        experiment == 10) {
      info.addr_magic +=
          sizeof(uint64_t);  // each remote client get different write offset
    }
    connect_client(server, i, attr, &info, 16, recv_with_data);
  }

  info = *(connect_info*)bufs[0];

  printf("Received info: %lx %u . Magic at %lx %u \n", info.ctx.addr,
         info.ctx.rkey, info.addr_magic, info.rkey_magic);

  RemoteBuffer* rb = create_remote_buffer(info.code, info.ctx);

  uint32_t outstaning = allparams["outstaning"].as<uint32_t>();
  uint32_t send_mem_len = (data_size + PREFIX_SIZE + SUFFIX_SIZE) * outstaning;

  char* mem3 = (char*)aligned_alloc(4096, send_mem_len);

  struct ibv_mr* mr3 =
      ibv_reg_mr(server->getPD(), mem3, send_mem_len,
                 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                     IBV_ACCESS_REMOTE_READ);

  // Region send_region{0,mem3+PREFIX_SIZE,data_size,mr3->lkey}; // 64 is for
  // prefix

  std::vector<Region> send_regions;
  for (uint32_t i = 0; i < outstaning; i++) {
    send_regions.push_back(
        {0, mem3 + i * (data_size + PREFIX_SIZE + SUFFIX_SIZE) + PREFIX_SIZE,
         data_size, mr3->lkey});
  }

  VerbsEP* ep = cons[0];

  if (experiment == 0) {
    uint32_t rsize = allparams["rsize"].as<uint32_t>();
    printf("I post buffers of %u bytes\n", rsize);

    ReceiveReceiver rc{ep, lb, rsize, max_recv_size};
    SendConnection sc{ep};
    rc.PrintInfo();
    p2p_test(sc, rc, send_regions, with_copy);
  }

  if (experiment == 1) {
    // local memory for SGEs experiments
    uint64_t local_mem = (uint64_t)mr2->addr;
    uint32_t local_mem_lkey = mr2->rkey;

    CircularMailBoxReceiver rc{lb, local_mem, local_mem_lkey};
    CircularConnectionMailBox sc{ep,
                                 rb,
                                 info.addr_magic,
                                 info.rkey_magic,
                                 local_mem + 8,
                                 local_mem_lkey};
    rc.PrintInfo();
    p2p_test(sc, rc, send_regions, with_copy);
  }

  if (experiment == 2) {
    uint64_t local_mem = (uint64_t)mr2->addr;
    uint32_t local_mem_lkey = mr2->rkey;

    CircularMagicbyteReceiver rc{lb};
    CircularConnectionMagicbyte sc{ep, rb, local_mem, local_mem_lkey};
    rc.PrintInfo();
    p2p_test(sc, rc, send_regions, with_copy);
  }

  if (experiment == 3) {
    CircularNotifyReceiver rc{ep, lb};
    CircularConnectionNotify sc{ep, rb};

    rc.PrintInfo();
    p2p_test(sc, rc, send_regions, with_copy);
  }

  if (experiment == 4) {
    uint64_t local_mem = (uint64_t)mr2->addr;
    uint32_t local_mem_lkey = mr2->rkey;

    CircularReverseReceiver rc{lb};
    CircularConnectionReverse sc{ep, rb, local_mem, local_mem_lkey};
    rc.PrintInfo();
    p2p_test(sc, rc, send_regions, with_copy);
  }

  if (experiment == 5) {
    void* mailbox = mr2->addr;  // I will write my progress here.
    void* faaaddr = ((char*)mr2->addr) + sizeof(uint64_t);

    if (dm_mr != NULL && allparams.count("with-dm-reads")) {
      mailbox = NULL;

#ifdef MAPDM
      if (allparams.count("with-map-dm")) {
        mailbox = mlx5dv_dm_map_op_addr(dm, 0);
      }
#endif
    }
    if (dm_mr != NULL && allparams.count("with-dm-atomics")) {
      faaaddr = NULL;
#ifdef MAPDM
      if (allparams.count("with-map-dm")) {
        if (allparams.count("with-dm-reads")) {  // was already mapped
          faaaddr = ((char*)mailbox) + 8;
        } else {
          faaaddr = mlx5dv_dm_map_op_addr(dm, 0);
        }
      }
#endif
    }
    // people write to me. I am a receiver.
    SharedCircularMagicbyteReceiver rc{(MagicRingBuffer*)lb, mailbox, faaaddr,
                                       dm};
    rc.PrintInfo();
    shared_test(rc, with_copy);
  }

  if (experiment == 6) {
    void* mailbox = mr2->addr;
    void* faaaddr = ((char*)mr2->addr) + sizeof(uint64_t);
    if (dm_mr != NULL && allparams.count("with-dm-reads")) {
      mailbox = NULL;
#ifdef MAPDM
      if (allparams.count("with-map-dm")) {
        mailbox = mlx5dv_dm_map_op_addr(dm, 0);
      }
#endif
    }

    if (dm_mr != NULL && allparams.count("with-dm-atomics")) {
      faaaddr = NULL;
#ifdef MAPDM
      if (allparams.count("with-map-dm")) {
        if (allparams.count("with-dm-reads")) {  // was already mapped
          faaaddr = ((char*)mailbox) + 8;
        } else {
          faaaddr = mlx5dv_dm_map_op_addr(dm, 0);
        }
      }
#endif
    }
    // people write to me. I am a receiver.
    SharedCircularNotifyReceiver rc{cons, (MagicRingBuffer*)lb, mailbox,
                                    faaaddr, dm};
    rc.PrintInfo();
    shared_test(rc, with_copy);
  }

  if (experiment == 7) {
    uint64_t tail_ptr =
        (uint64_t)mr2->addr +
        sizeof(uint64_t);  // it is pointer to an array of tails that are rdma
                           // accessible by clients. they write here.
    BufferContext bc{(uint64_t)mr->addr, mr->lkey, buffer_len};
    MagicRemoteBuffer lrb{bc};

    ReadCircularConnectionNotify sc{cons, &lrb, tail_ptr};  // it is sender
    printf("start loop with \n");

    sc.PrintInfo();
    latency_shared_test(sc, send_regions[0], 1024, test);
    bw_shared_test(sc, send_regions, 20);
  }

  if (experiment == 8 || experiment == 9 || experiment == 10) {
    uint64_t local_tail_ptr =
        (uint64_t)mr2->addr;  // it is pointer to my tail  that is rdma
                              // accessible by clients. they read it.
    uint64_t tail_ptrs =
        local_tail_ptr +
        sizeof(uint64_t);  // it is pointer to an array of tails  that are rdma
                           // accessible by clients. they write here.

    BufferContext bc{(uint64_t)mr->addr, mr->lkey, buffer_len};
    MagicRemoteBuffer lrb{bc};
    if (dm_mr != NULL) {
      local_tail_ptr = 0;
#ifdef MAPDM
      if (allparams.count("with-map-dm")) {
        local_tail_ptr = (uint64_t)mlx5dv_dm_map_op_addr(dm, 0);
      }
#endif
    }

    ReadCircularConnectionMagicByte sc{(uint32_t)cons.size(), &lrb,
                                       local_tail_ptr, tail_ptrs,
                                       dm};  // it is sender
    printf("start loop with \n");
    sc.PrintInfo();
    latency_shared_test(sc, send_regions[0], 1024, test);

    bw_shared_test(sc, send_regions, 20);
  }

  if (experiment == 11) {
    // people write to me. I am a receiver.
    uint32_t rsize = allparams["rsize"].as<uint32_t>();
    printf("I post buffers of %u bytes\n", rsize);

    SharedReceiveReceiver rc{cons, lb, rsize, max_recv_size};
    rc.PrintInfo();
    shared_test(rc, with_copy);
  }
}
