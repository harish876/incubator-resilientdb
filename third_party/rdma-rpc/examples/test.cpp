#include <iostream>      
#include <infiniband/verbs.h>
#include "cxxopts.hpp"
#include <assert.h> 
#include  "com/basic_ring.hpp"
#include  "com/utils.hpp"


// ParseResult class: used for the parsing of line arguments
cxxopts::ParseResult
parse(int argc, char* argv[])
{
    cxxopts::Options options(argv[0], "Sender of UD test");
    options
      .positional_help("[optional args]")
      .show_positional_help();

  try
  {
 
    options.add_options()
      ("s,size", "buf size", cxxopts::value<uint32_t>()->default_value("4096"), "N")
      ("help", "Print help")
     ;
 
    auto result = options.parse(argc, argv);
 
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

static inline uint32_t log2(const uint32_t x) {
  uint32_t y;
  asm ( "\tbsr %1, %0\n"
      : "=r"(y)
      : "r" (x)
  );
  return y;
}


// this is a test of different buffers and tools.
 
int main(int argc, char* argv[]){
// parse the arguments and creates a dictionary which maps arguments to values
    auto allparams = parse(argc,argv);

  

    uint32_t size = allparams["size"].as<uint32_t>(); 
    char* mem = (char*)GetMagicBuffer(size);
    


    struct ibv_mr mr = {NULL, NULL, mem, size, 0, 0, 0};


    {
      BasicRingBuffer mb{&mr, size, true};
      BufferContext ctx = mb.GetContext();
      BasicRemoteBuffer rmb{ctx};

      for(uint32_t i=0; i < 10; i++){
        char* ptr = mb.Read(64);
        uint64_t rem_addr = rmb.GetWriteAddr(64);
        if((uint64_t)(void*)ptr != rem_addr){
          printf("error1\n");
        }
        printf("%lx \n",rem_addr);
      }
      {
        uint64_t head = mb.Free(640);
        uint32_t bytes = mb.GetFreedBytes();
        if( bytes != 640){
            printf("error bytes 1 %u\n",bytes);
        }
        rmb.UpdateHead(head);
      }
      for(uint32_t i=0; i < 10; i++){
        char* ptr = mb.Read(64);
        uint64_t rem_addr = rmb.GetWriteAddr(64);
        if((uint64_t)(void*)ptr != rem_addr){
          printf("error2\n");
        }
      }
      {
        uint64_t head = mb.Free(640);
        uint32_t bytes = mb.GetFreedBytes();
        if( bytes != 640){
            printf("error bytes 2\n");
        }
        rmb.FreeBytes(bytes); 
      }
      for(uint32_t i=0; i < 3; i++){
        char* ptr = mb.Read(1000);
        uint64_t rem_addr = rmb.GetWriteAddr(1000);
        if((uint64_t)(void*)ptr != rem_addr){
          printf("error3 %lx %lx\n", (uint64_t)(void*)ptr , rem_addr);
        } 
      }
      {
        uint64_t head = mb.Free(3000); 
        uint32_t bytes = mb.GetFreedBytes();
        if( bytes != 3000){
            printf("error bytes\n");
        }
        rmb.FreeBytes(bytes);
      }
      for(uint32_t i=0; i < 10; i++){
        char* ptr = mb.Read(64);
        uint64_t rem_addr = rmb.GetWriteAddr(64);
        if((uint64_t)(void*)ptr != rem_addr){
          printf("error4\n");
        }
      }
    }
    

    FreeMagicBuffer(mem, size);

    
}
 
