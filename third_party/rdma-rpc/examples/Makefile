APPS = test client server

INCLUDE = -I../lib/include/rdma_rpc -I./third_party
FLAGS = -std=c++11 -O2 -Wall
LDFLAGS = -libverbs -lrdmacm


all: ${APPS}

test: test.cpp
	g++ test.cpp -o test $(INCLUDE) $(FLAGS)  $(LDFLAGS)

server: server.cpp
	g++ server.cpp -o server $(INCLUDE) $(FLAGS)  $(LDFLAGS)

client: client.cpp  
	g++ client.cpp -o client $(INCLUDE) $(FLAGS)  $(LDFLAGS)

clean:
	$(foreach fname,${APPS}, rm -f ${fname})

.DELETE_ON_ERROR:
.PHONY: all clean
