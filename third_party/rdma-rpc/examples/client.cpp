#include <iostream>       // std::cout
#include <infiniband/verbs.h>
#include <chrono>
#include "cxxopts.hpp"
#include  "ClientRDMA.hpp"
#include  "VerbsEP.hpp"

#include  "com/protocols.hpp"

#include  "com/magic_ring.hpp"
#include  "com/reverse_ring.hpp"
#include  "com/utils.hpp"
#include <unistd.h>


std::vector<void*> bufs;

char* memory = NULL;
uint64_t mem_offset = 0;
uint64_t mem_size = 0;


// ParseResult class: used for the parsing of line arguments
cxxopts::ParseResult
parse(int argc, char* argv[])
{
    cxxopts::Options options(argv[0], "Sender of the test");
    options
      .positional_help("[optional args]")
      .show_positional_help();

  try
  {
 
    options.add_options()
      ("a,address", "IPADDR", cxxopts::value<std::string>(), "IP")
      ("w,warmup", "warmup iterations", cxxopts::value<uint32_t>()->default_value("1024"), "N")
      ("t,test", "test iterations", cxxopts::value<uint32_t>()->default_value("1024"), "N")
      ("send-size", "The IB maximum send size in pages", cxxopts::value<uint32_t>()->default_value(std::to_string(128)), "N") // this is the size of the queue request on the device
      ("recv-size", "The IB maximum receive size in pages", cxxopts::value<uint32_t>()->default_value(std::to_string(128)), "N") // this is the size of the queue request on the device
      ("e,exp", "experiment id", cxxopts::value<uint32_t>()->default_value("0"), "N")
      ("s,size", "buffer size. For send and for recv.", cxxopts::value<uint32_t>()->default_value(std::to_string(8)), "N")     
      ("rsize", "receive size. For send/recv exp.", cxxopts::value<uint32_t>()->default_value(std::to_string(8)), "N")       
      ("o,outstaning", "total number of outstanding. Used for BW experiments", cxxopts::value<uint32_t>()->default_value(std::to_string(8)), "N") 
      ("m,memory", "Size of ring buffers in bytes. must be page aligned", cxxopts::value<uint32_t>()->default_value(std::to_string(4096)), "N") 
      ("with-copy", "copy messages", cxxopts::value<uint64_t>()->default_value(std::to_string(0)), "N") 
      ("send-sges", "number of send sges", cxxopts::value<uint32_t>()->default_value("1"), "N") 
      ("help", "Print help")
     ;
  
    auto result = options.parse(argc, argv);
    if (result.count("address") == 0)
    {
      std::cout <<"No address is provided" << options.help({""}) << std::endl;
      exit(0);
    }
    if (result.count("help"))
    {
      std::cout << options.help({""}) << std::endl;
      exit(0);
    }
 
    return result;

  }
  catch (const cxxopts::OptionException& e)
  {
    std::cout << "error parsing options: " << e.what() << std::endl;
    std::cout << options.help({""}) << std::endl;
    exit(1);
  }
}


void latency_p2p_test(SendCommunicator& sc, ReceiveCommunicator& rc, Region& send_region, uint32_t warmup, uint32_t test, bool with_copy = false){
  using namespace std::chrono;
  {
    std::chrono::seconds delay(1);
    auto next_print = high_resolution_clock::now() + delay; 
    while(high_resolution_clock::now() < next_print){
      // do nothing
    }
  }
    
  if(with_copy) printf(">>>>>>>>>Start test with copy\n");
  std::vector<float> latency_rtt; 
  latency_rtt.reserve(test); 
  
  std::vector<Region> recvs;
  uint64_t last_id = 0;

  for(uint32_t i=0; i<warmup; i++){
        uint32_t bytes = rc.GetFreedReceiveBytes();
       // printf("send %u \n",bytes);
        *(uint32_t*)send_region.addr = bytes; // send freed bytes as a message

        last_id = sc.SendAsync(send_region);

        while(rc.Receive(recvs) == 0){ // wait for pong
            sc.TestSend(last_id);
        }
       // printf("Received a w message %d\n",i);

        uint32_t freedBytes = *(uint32_t*)(recvs[0].addr);  // the other side will send us its FreedReceivebytes.

        if(with_copy){
          memcpy(memory+mem_offset,recvs[0].addr,recvs[0].length);
          mem_offset = (mem_offset + recvs[0].length) % mem_size;
        }

        sc.AckSentBytes(freedBytes);
     //   printf("freedBytes %u \n",freedBytes);

        // process receive buffer
        rc.FreeReceive(recvs[0]);
        recvs.clear();
 

  }
  for(uint32_t i=0; i<test; i++){
    auto t1 = high_resolution_clock::now(); 
    *(uint32_t*)send_region.addr = rc.GetFreedReceiveBytes(); // send freed bytes as a message

    last_id = sc.SendAsync(send_region);

    while(rc.Receive(recvs) == 0){ // wait for pong
       // sc.TestSend(last_id);
    }
    auto t2 = high_resolution_clock::now();
    
   // printf("Received a test message %d\n",i);

    uint32_t freedBytes = *(uint32_t*)(recvs[0].addr);  // the other side will send us its FreedReceivebytes.

    if(with_copy){
      memcpy(memory+mem_offset,recvs[0].addr,recvs[0].length);
      mem_offset = (mem_offset + recvs[0].length) % mem_size;
    }
    sc.AckSentBytes(freedBytes);

    // process receive buffer
    rc.FreeReceive(recvs[0]);
    recvs.clear();

    sc.TestSend(last_id);

    float lat = duration_cast<nanoseconds>(t2 - t1).count()/ 1000.0;
    latency_rtt.push_back(lat);       
  }
 
  std::sort (latency_rtt.begin(), latency_rtt.end());
  int q025 = (int)(test*0.025);
  int q050 = (int)(test*0.05);
  int q500 = (int)(test*0.5);
  int q950 = (int)(test*0.950);
  int q975 = (int)(test*0.975);
  printf("latency_rtt {%f-%f-%f-%f-%f} us \n",  latency_rtt[q025], latency_rtt[q050], latency_rtt[q500],  latency_rtt[q950], latency_rtt[q975]);
  for(auto f : latency_rtt){
    printf("%f ",f);
  }
  printf("\n");
}

// it send the buffer in roundrobin 
void bw_p2p_test(SendCommunicator& sc, ReceiveCommunicator& rc, std::vector<Region> &send_regions, uint32_t measure, bool with_copy = false){
  using namespace std::chrono;
  if(with_copy) printf(">>>>>>>>>Start test with copy\n");
  std::chrono::seconds delay(5);
  auto next_print = high_resolution_clock::now() + delay; 

  std::vector<Region> recvs;

  uint32_t message_size = sc.ReqSize(send_regions[0]); 
  const uint32_t max_sends = send_regions.size();

  printf("Message size %u and outstanding is %u\n",message_size,max_sends);
  uint32_t cur = 0; 
  uint32_t outstaning = 0; 
  uint32_t done = 0; 
  uint64_t last_id = 0;
  while(measure){
      if(outstaning<max_sends){
        outstaning++;
        *(uint32_t*)send_regions[cur].addr = rc.GetFreedReceiveBytes();
        last_id = sc.SendAsync(send_regions[cur]);
  //      printf("Dispatch %u\n",cur);
        cur = (cur+1) % max_sends;
      }
      sc.TestSend(last_id);
      if(rc.Receive(recvs)){
          uint32_t freedBytes = 0;
          uint32_t completed = 0;
          for(auto &recv : recvs){
            freedBytes += *(uint32_t*)(recv.addr);
         //   printf("got bytes %u\n",freedBytes); 

            if(with_copy){
              memcpy(memory+mem_offset,recv.addr,recv.length);
              mem_offset = (mem_offset + recv.length) % mem_size;
            }

            rc.FreeReceive(recv);
          }
             // the other side will send us its FreedReceivebytes.
          sc.AckSentBytes(freedBytes);
          // process receive buffer
          recvs.clear();
          completed = freedBytes / message_size; // chaeck. should be as in send_regions
          if(outstaning < completed ) printf("error\n");
          outstaning-=completed;
          done+=completed;
      }
 

      if(high_resolution_clock::now() > next_print){
        printf("[BW test] completed %u sends in 5 sec\n",done);
        done = 0;
        measure--;
        next_print = high_resolution_clock::now() + delay;
      }
  }
}






void latency_shared_test(SendCommunicator& sc, Region& send_region, uint32_t warmup, uint32_t test){
  using namespace std::chrono;
  {
    std::chrono::seconds delay(1);
    auto next_print = high_resolution_clock::now() + delay; 
    while(high_resolution_clock::now() < next_print){
      // do nothing
    }
  }
    
  std::vector<float> latency_rtt; 
  latency_rtt.reserve(test); 

  for(uint32_t i=0; i<warmup; i++){
        uint64_t id = sc.SendAsync(send_region);
        sc.WaitSend(id);
       // printf("Received completion %d\n",i);
  }

  for(uint32_t i=0; i<test; i++){
    auto t1 = high_resolution_clock::now(); 
   
    uint64_t id = sc.SendAsync(send_region);
    sc.WaitSend(id);
    auto t2 = high_resolution_clock::now();
    
  //  printf("Received a test message %d\n",i);

    float lat = duration_cast<nanoseconds>(t2 - t1).count()/ 1000.0;
    latency_rtt.push_back(lat);       
  }
 
  std::sort (latency_rtt.begin(), latency_rtt.end());
  int q025 = (int)(test*0.025);
  int q050 = (int)(test*0.05);
  int q500 = (int)(test*0.5);
  int q950 = (int)(test*0.950);
  int q975 = (int)(test*0.975);
  printf("latency_rtt {%f-%f-%f-%f-%f} us \n",  latency_rtt[q025], latency_rtt[q050], latency_rtt[q500],  latency_rtt[q950], latency_rtt[q975]);
  for(auto f : latency_rtt){
    printf("%f ",f);
  }
  printf("\n");
}

void bw_shared_test(SendCommunicator& sc, std::vector<Region> &send_regions, uint32_t measure){
  using namespace std::chrono;
  
  std::chrono::seconds delay(5);
  auto next_print = high_resolution_clock::now() + delay; 
 
  uint32_t message_size = sc.ReqSize(send_regions[0]); 
  const uint32_t max_sends = send_regions.size();

  printf("Message size %u and outstanding is %u\n",message_size,max_sends);

  uint32_t cur = 0; 
  
  uint32_t outstaning = 1; 
  uint32_t done = 0;  
  
  uint64_t pending_id = sc.SendAsync(send_regions[cur]);
  uint64_t latest_id = pending_id;
 
 while(measure){
      if(outstaning<max_sends){
        outstaning++; 
        latest_id = sc.SendAsync(send_regions[cur]);
     //   printf("Dispatch %u. and got id %lu\n",cur,latest_id);
        cur = (cur+1) % max_sends;
      }

      while( pending_id<=latest_id && sc.TestSend(pending_id) ){
    //    printf("completed one\n");
        pending_id+=1;
        if(outstaning == 0){
          printf("error with counting;\n"); exit(1);
        }
        outstaning-=1;
        done+=1;
      }
 
      if(high_resolution_clock::now() > next_print){
        if(done ==0){
          return;
        }
        printf("[BW test] completed %u sends in 5 sec\n",done);
        done = 0;
        measure--;
        next_print = high_resolution_clock::now() + delay;
      }
  }
  
}

// shoud work for latency and bw
void shared_test(PartialReceiveCommunicator& rc, Region & recv_region){ 
  std::vector<Region> recvs;
  printf("recv size is %u\n",recv_region.length);

  while(true){
    int ret = rc.Receive(recvs);

    if(ret){
        //printf("Received %d\n",ret);
        for(auto &recv : recvs){
          if(recv.context == 1){ // it is a temp read that we need to compelte!
         //   printf("got temp recv.\n");
            rc.Receive(recv, recv_region); 
          }
          else{ // completed read!
         //   printf("got full recv.\n");
            rc.FreeReceive(recv);
          }          
        }
        // process receive buffer
        recvs.clear();

        uint32_t freed_bytes = rc.GetFreedReceiveBytes(); // send freed bytes as a message . we need it!
      //  printf("ack %u bytes.\n", freed_bytes);
    }
  }
}


VerbsEP* connect_to(struct rdma_cm_id* id, struct ibv_qp_init_attr attr,void* my_info, uint32_t recv_batch, bool recv_with_data ){
    VerbsEP* ep = NULL;
    struct ibv_pd *pd = id->pd;
 
    uint32_t max_recv_size = attr.cap.max_recv_wr;
    if(attr.srq) attr.cap.max_recv_wr = 0;
    

    if (rdma_create_qp(id, pd, &attr)) {
        perror("rdma_create_qp");
        exit(1);
    }
    
    attr.cap.max_recv_wr = max_recv_size;
    ep = new VerbsEP(id, attr, recv_batch, recv_with_data); 
    // attr.cap.max_inline_data, attr.cap.max_send_wr, max_recv_size, , attr.srq ); 

    struct rdma_conn_param conn_param;
    memset(&conn_param, 0 , sizeof(conn_param));
    conn_param.responder_resources = 16; // up to 16 reads
    conn_param.initiator_depth = 16;
    conn_param.retry_count = 3;
    conn_param.rnr_retry_count = 3; 
    conn_param.private_data = my_info;
    conn_param.private_data_len = sizeof(connect_info);

    if(rdma_connect(id, &conn_param)){
      printf(" failed to accept\n"); 
      exit(1);
    }

    struct rdma_cm_event *event;
   
    if (rdma_get_cm_event(id->channel, &event)) {
        perror("rdma_get_cm_event");
        exit(1);
    }
    if(!event->param.conn.private_data_len) {
      printf("Error did not get data on connect \n");
      exit(1);
    }
    void*  connect_buffer = malloc(event->param.conn.private_data_len);
    memcpy(connect_buffer,event->param.conn.private_data,event->param.conn.private_data_len);
  
    bufs.push_back(connect_buffer);

    rdma_ack_cm_event(event);

 
    return ep;
}

static inline uint32_t log2(const uint32_t x) {
  uint32_t y;
  asm ( "\tbsr %1, %0\n"
      : "=r"(y)
      : "r" (x)
  );
  return y;
}


Buffer* create_buffer(uint32_t experiment, ibv_mr *mr, uint32_t len){
    if(experiment == 4){
      return new ReverseRingBuffer(mr, log2(len));
    }
    bool with_zero = experiment==2 || experiment > 5; 
    if(experiment == 11) with_zero = false;
    return new MagicRingBuffer(mr, log2(len), with_zero );
}

RemoteBuffer* create_remote_buffer(uint32_t experiment, BufferContext &bc){
  if(experiment == 4){
    return new ReverseRemoteBuffer(bc);
  }

  return new MagicRemoteBuffer(bc);
}

int main(int argc, char* argv[]){
// parse the arguments and creates a dictionary which maps arguments to values
    auto allparams = parse(argc,argv);

    std::string ip = allparams["address"].as<std::string>(); // "192.168.1.20"; .c_str()
    int port = 9999;
    uint32_t experiment = allparams["exp"].as<uint32_t>(); 

    uint32_t warmup = allparams["warmup"].as<uint32_t>(); 
    uint32_t test = allparams["test"].as<uint32_t>(); 


    uint32_t max_recv_size = allparams["recv-size"].as<uint32_t>();
    uint32_t max_send_size = allparams["send-size"].as<uint32_t>();  
    bool with_srq = false;


    uint32_t data_size = allparams["size"].as<uint32_t>();  


    struct rdma_cm_id * id = ClientRDMA::sendConnectRequest((char*)ip.c_str(),port);
    if (!id->pd) id->pd = ibv_alloc_pd(id->verbs);

    uint32_t buffer_len = allparams["memory"].as<uint32_t>();;
    printf("Create Magic memory of %u bytes\n",buffer_len);


    mem_size = allparams["with-copy"].as<uint64_t>();
    bool with_copy = mem_size!=0;
    if(with_copy){
      memory = (char*)GetMagicBuffer(mem_size);
    }


    char* mem = (char*)GetMagicBuffer(buffer_len);
    struct ibv_mr * mr = ibv_reg_mr(id->pd,mem,buffer_len*2, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ );
    printf("MagicBuffer at %p and it repeats at %p\n",mem,mem+buffer_len);
    // create local buffer
    Buffer* lb = create_buffer(experiment, mr, buffer_len);
    BufferContext bc = lb->GetContext();


    // memory for extra things


    uint32_t mem2_size = sizeof(uint64_t)*32;
    if(experiment == 8){
      mem2_size = allparams["rsize"].as<uint32_t>() * 16;
    }

    char* mem2 = (char*)aligned_alloc(4096,mem2_size); // for shared we need a lot of them. We use also for partial fetch
    struct ibv_mr * mr2 = ibv_reg_mr(id->pd,mem2,mem2_size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);


    connect_info info;
    info.code = experiment;
    info.ctx = bc;
    info.rkey_magic = mr2->rkey;
    info.addr_magic = (uint64_t)mr2->addr;
    printf("My info: %lx %u . Magic at %lx %u \n",info.ctx.addr,info.ctx.rkey,info.addr_magic,info.rkey_magic);


    struct ibv_qp_init_attr attr = prepare_qp(id->pd,  max_send_size, max_recv_size, with_srq);
    attr.cap.max_send_sge = allparams["send-sges"].as<uint32_t>();
    bool recv_with_data = (experiment == 0);
    VerbsEP* ep = connect_to(id,attr, &info, /*recv_batch*/ 16, recv_with_data);

    info = *(connect_info*)bufs[0];

    printf("Received info: %lx %u . Magic at %lx %u \n",info.ctx.addr,info.ctx.rkey,info.addr_magic,info.rkey_magic);

    RemoteBuffer* rb = create_remote_buffer(info.code, info.ctx); 

    uint32_t outstaning = allparams["outstaning"].as<uint32_t>(); 
    uint32_t send_mem_len = (data_size+PREFIX_SIZE+SUFFIX_SIZE)*outstaning;

    char* mem3 = (char*)aligned_alloc(4096,send_mem_len);

    struct ibv_mr * mr3 = ibv_reg_mr(id->pd,mem3,send_mem_len, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    
    //Region send_region{0,mem3+PREFIX_SIZE,data_size,mr3->lkey}; // 64 is for prefix

    Region recv_region{0,mem3+PREFIX_SIZE,data_size,mr3->lkey}; // 64 is for prefix


    if(experiment >=8 && experiment <=10 ){
      recv_region = {0,mem3, data_size+8,mr3->lkey}; // 64 is for prefix
    }

    std::vector<Region> send_regions;
    for(uint32_t i=0;i<outstaning; i++){
      send_regions.push_back({0,mem3+i*(data_size+PREFIX_SIZE+SUFFIX_SIZE)+PREFIX_SIZE, data_size, mr3->lkey});
    }




    // here the tests when I am a sender:
    if(experiment == 0){
      uint32_t rsize = allparams["rsize"].as<uint32_t>();
      printf("I post buffers of %u bytes\n", rsize);
      ReceiveReceiver rc{ep, lb, rsize, max_recv_size};
      SendConnection sc{ep};

      sc.PrintInfo();
      latency_p2p_test(sc, rc, send_regions[0],  warmup, test,with_copy);

      printf("Start BW test\n");
      uint32_t measure = 20;
      bw_p2p_test(sc,rc, send_regions, measure,with_copy);
    }
    if(experiment == 1){
      // local memory for SGEs experiments
      uint64_t local_mem = (uint64_t)mr2->addr;
      uint32_t local_mem_lkey = mr2->rkey;

      CircularMailBoxReceiver rc{lb, local_mem, local_mem_lkey};
      CircularConnectionMailBox sc{ep, rb, info.addr_magic, info.rkey_magic, local_mem +8, local_mem_lkey};
      sc.PrintInfo();
      latency_p2p_test(sc, rc, send_regions[0],  warmup, test,with_copy);

      printf("Start BW test\n");
      uint32_t measure = 20;
      bw_p2p_test(sc,rc, send_regions, measure,with_copy);
    }

    if(experiment == 2){
      uint64_t local_mem = (uint64_t)mr2->addr;
      uint32_t local_mem_lkey = mr2->rkey;

      CircularMagicbyteReceiver rc{lb};
      CircularConnectionMagicbyte sc{ep, rb,local_mem,local_mem_lkey};
      sc.PrintInfo();
      latency_p2p_test(sc, rc, send_regions[0],  warmup, test,with_copy);

      printf("Start BW test\n");
      uint32_t measure = 20;
      bw_p2p_test(sc,rc, send_regions, measure,with_copy);
    }

    if(experiment == 3){
      CircularNotifyReceiver rc{ep,lb};
      CircularConnectionNotify sc{ep, rb}; 
      sc.PrintInfo();
      latency_p2p_test(sc, rc, send_regions[0],  warmup, test,with_copy);

      printf("Start BW test\n");
      uint32_t measure = 20;
      bw_p2p_test(sc,rc, send_regions, measure,with_copy);
    }   

    if(experiment == 4){
      uint64_t local_mem = (uint64_t)mr2->addr;
      uint32_t local_mem_lkey = mr2->rkey;

      CircularReverseReceiver rc{lb};
      CircularConnectionReverse sc{ep, rb,local_mem,local_mem_lkey}; 
      sc.PrintInfo();
      latency_p2p_test(sc, rc, send_regions[0],  warmup, test,with_copy);

      printf("Start BW test\n");
      uint32_t measure = 20;
      bw_p2p_test(sc,rc, send_regions, measure,with_copy);
    }   

    if(experiment == 5){
      uint64_t rem_head = info.addr_magic; // magic byte
      uint32_t rem_head_rkey = info.rkey_magic;
     
      uint64_t rem_win = info.addr_magic + sizeof(uint64_t);  // for faa
      uint32_t rem_win_rkey = info.rkey_magic;

      if(info.dm_rkey != 0){
         uint32_t offset =0;
         if(info.addr_magic2 & 1){ // for head reads
            rem_head=0;
            rem_head_rkey=info.dm_rkey;
            offset+=8;
            printf(">>>>> Remote Head is in DM\n");
         }
         if(info.addr_magic2 & 2){ // for faas wins
            rem_win = offset;  // for faa
            rem_win_rkey = info.dm_rkey;
            printf(">>>>> Remote FAA is in DM\n");
         }
      }

      // local memory for faa and reads
      uint64_t local_mem = (uint64_t)mr2->addr;
      uint32_t local_mem_lkey = mr2->rkey;

      SharedCircularConnectionMailbox sc{ep, (MagicRemoteBuffer*)rb, rem_head, rem_head_rkey, rem_win, rem_win_rkey,local_mem, local_mem_lkey}; 
      sc.PrintInfo();
      latency_shared_test(sc, send_regions[0],  warmup, test);

      printf("Start BW test\n");
      uint32_t measure = 20;
      bw_shared_test(sc, send_regions, measure);
      sc.PrintDebug();
    } 

    if(experiment == 6){
      uint64_t rem_head = info.addr_magic; // magic byte
      uint32_t rem_head_rkey = info.rkey_magic;
      
      uint64_t rem_win = info.addr_magic + sizeof(uint64_t);  // for faa
      uint32_t rem_win_rkey = info.rkey_magic;

      if(info.dm_rkey != 0){
         uint32_t offset =0;
         if(info.addr_magic2 & 1){ // for head reads
            rem_head=0;
            rem_head_rkey=info.dm_rkey;
            offset+=8;
            printf(">>>>> Remote Head is in DM\n");
         }
         if(info.addr_magic2 & 2){ // for faas wins
            rem_win = offset;  // for faa
            rem_win_rkey = info.dm_rkey;
            printf(">>>>> Remote FAA is in DM\n");
         }
      }

            // local memory for faa and reads
      uint64_t local_mem = (uint64_t)mr2->addr;
      uint32_t local_mem_lkey = mr2->rkey;

      SharedCircularConnectionNotify sc{ep, (MagicRemoteBuffer*)rb, rem_head, rem_head_rkey, rem_win, rem_win_rkey,local_mem, local_mem_lkey}; 
 
      sc.PrintInfo();

      latency_shared_test(sc, send_regions[0],  warmup, test);

      printf("Start BW test\n");
      uint32_t measure = 20;
      bw_shared_test(sc,send_regions, measure);

      sc.PrintDebug();
    } 



    // here the tests when I am a receiver:


    if(experiment == 7){
      uint64_t rem_tail = info.addr_magic; // magic byte
      uint32_t rem_tail_rkey = info.rkey_magic;

      struct ibv_mr mr; 
      mr.addr = (void*)info.ctx.addr;
      mr.rkey = info.ctx.rkey;

      MagicRingBuffer rlb { &mr ,log2(info.ctx.length) ,false} ;
      ReadCircularNotifyReceiver rc{ep, &rlb, rem_tail, rem_tail_rkey};
      rc.PrintInfo();
      shared_test(rc,recv_region); //we use it  as recv
    }
  

    if(experiment == 8){
      uint64_t rem_tail = info.addr_magic; // magic byte
      uint32_t rem_tail_rkey = info.rkey_magic;

            // local memory for faa and reads
      uint64_t local_mem = (uint64_t)mr2->addr;
      uint32_t local_mem_lkey = mr2->rkey;
  
      uint32_t prefetch_size = allparams["rsize"].as<uint32_t>();
      printf("prefetch size is %u bytes\n", prefetch_size);

      struct ibv_mr mr; 
      mr.addr = (void*)info.ctx.addr;
      mr.rkey = info.ctx.rkey;

      MagicRingBuffer rlb { &mr ,log2(info.ctx.length) ,false} ;

      ReadCircularMagicByteReceiver rc{ep, &rlb, rem_tail, rem_tail_rkey,local_mem,local_mem_lkey,prefetch_size};
      rc.PrintInfo();
      shared_test(rc,recv_region); //we use it  as recv region
    }
  
    if(experiment == 9 || experiment == 10){
      uint64_t rem_tail = info.addr_magic; // magic byte
      uint32_t rem_tail_rkey = info.rkey_magic;

      uint64_t rem_head = info.addr_magic2; // magic byte
      uint32_t rem_head_rkey = info.rkey_magic;
      if(info.dm_rkey){
        rem_head_rkey = info.dm_rkey; 
        rem_head = 0;
      }

            // local memory for faa and reads
      uint64_t local_mem = (uint64_t)mr2->addr;
      uint32_t local_mem_lkey = mr2->rkey;

  
      struct ibv_mr mr; 
      mr.addr = (void*)info.ctx.addr;
      mr.rkey = info.ctx.rkey;

      MagicRingBuffer rlb { &mr ,log2(info.ctx.length) ,false} ;

      bool in_order = (experiment == 9);

      ReadCircularMailboxReceiver rc{ep, &rlb, rem_tail, rem_tail_rkey,
       rem_head , rem_head_rkey,  local_mem ,  local_mem_lkey, in_order};

      rc.PrintInfo();
      //recv_region={0,mem3,data_size+PREFIX_SIZE,mr3->lkey};
      shared_test(rc,recv_region); //we use it  as recv region
    }

    if(experiment == 11){
      SharedSendConnection sc{ep};

      sc.PrintInfo(); 
      latency_shared_test(sc, send_regions[0],  warmup, test);

      printf("Start BW test\n");
      uint32_t measure = 20;
      bw_shared_test(sc, send_regions, measure);
     // sc.PrintDebug();
    }

  
}
 
