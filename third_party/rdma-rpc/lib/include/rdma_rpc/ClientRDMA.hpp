#pragma once 

#include <cstdio>
#include <cstdlib>
#include <cerrno>
#include <string>
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>

class ClientRDMA{

public:
 

	static struct rdma_cm_id * sendConnectRequest(char* ip, int port){
 
		struct rdma_cm_id *id = NULL;
		struct rdma_addrinfo *addrinfo = NULL;
		struct rdma_addrinfo hints;

		memset(&hints, 0, sizeof hints);
		hints.ai_port_space = RDMA_PS_TCP;
        
        if (rdma_getaddrinfo(ip, (char*)std::to_string(port).c_str(), &hints, &addrinfo)) {
            perror("rdma_getaddrinfo\n");
            if(addrinfo){
            	rdma_freeaddrinfo(addrinfo);
            }
            return NULL;
        } 

        struct rdma_event_channel * channel = rdma_create_event_channel();

        if(!channel){
            printf("failed to create event channel\n");
            exit(1);
        }
 
        if(rdma_create_id(channel, &id, NULL, RDMA_PS_TCP))
        {
            printf("failed to create listen id\n");
            exit(1);
        }

        if(rdma_resolve_addr(id, NULL, addrinfo->ai_dst_addr, 2000) ){
            printf("failed to rdma_resolve_addr\n");
            exit(1); 
        } 

        struct rdma_cm_event *event;
       
        if (rdma_get_cm_event(channel, &event)) {
            perror("rdma_get_cm_event");
            exit(1);
        }
        rdma_ack_cm_event(event);

        if(rdma_resolve_route(id, 2000)){
            printf("failed to rdma_resolve_route\n");
            exit(1); 
        }

        if (rdma_get_cm_event(channel, &event)) {
            perror("rdma_get_cm_event");
            exit(1);
        }
        rdma_ack_cm_event(event);

        if(addrinfo){
        	rdma_freeaddrinfo(addrinfo);
        }

        return id;
	}


};