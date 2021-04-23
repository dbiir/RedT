/*
   Copyright 2016 Massachusetts Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef _TRANSPORT_H_
#define _TRANSPORT_H_
#include "global.h"
//#include "nn.hpp"
#ifdef USE_RDMA
  #include "nn.hpp"
  #include "lib.hh"
  #include "qps/rc_recv_manager.hh"
  #include "qps/recv_iter.hh"
  #include "qps/mod.hh"
  #include "utils/timer.hh"
  #include "msg_thread.h"
  using namespace std;
  using namespace rdmaio;
  using namespace rdmaio::rmem;
  using namespace rdmaio::qp;
#else
  #include "nn.hpp"
#endif
#include <nanomsg/bus.h>
#include <nanomsg/pair.h>
#include "query.h"
#include "pthread.h"

class Workload;
class Message;

/*
	 Message format:
Header: 4 Byte receiver ID
				4 Byte sender ID
				4 Byte # of bytes in msg
Data:	MSG_SIZE - HDR_SIZE bytes
	 */

#define GET_RCV_NODE_ID(b)  ((uint32_t*)b)[0]
#ifdef USE_RDMA
class SimpleAllocator : public AbsRecvAllocator {
  RMem::raw_ptr_t buf = nullptr;
  usize total_mem = 0;
  mr_key_t key;

 public:
  SimpleAllocator(Arc<RMem> mem, mr_key_t key)
      : buf(mem->raw_ptr), total_mem(mem->sz), key(key) {
    // RDMA_LOG(4) << "simple allocator use key: " << key;
  }

  Option<std::pair<rmem::RMem::raw_ptr_t, rmem::mr_key_t>> alloc_one(
      const usize &sz) override {
    if (total_mem < sz) return {};
    auto ret = buf;
    buf = static_cast<char *>(buf) + sz;
    total_mem -= sz;
    return std::make_pair(ret, key);
  }

  Option<std::pair<rmem::RMem::raw_ptr_t, rmem::RegAttr>> alloc_one_for_remote(
      const usize &sz) override {
    return {};
  }
};
#endif

class Socket {
	public:

		Socket () : sock(AF_SP,NN_PAIR) {}
		~Socket () { delete &sock;}
        char _pad1[CL_SIZE];
		nn::socket sock;
        //char _pad[CL_SIZE - sizeof(nn::socket)];//?usage of _pad
};
#ifdef USE_RDMA
class rdma_send_qps{
  public:
    rdmaio::Arc<rdmaio::qp::RDMARC> send_qps;
    uint64_t buffer_idx;
    uint64_t buffer_size;
    uint64_t count;
    uint64_t max_count;
    rdma_send_qps() {
      send_qps = NULL;
      count = 0;
      buffer_idx = 0;
      buffer_size = (RDMA_BUFFER_SIZE / MSG_SIZE_MAX);
      max_count = RDMA_SEND_COUNT;
    }
    rdma_send_qps(rdmaio::Arc<rdmaio::qp::RDMARC> qp) {
      send_qps = qp;
      count = 0;
      buffer_idx = 0;
      buffer_size = (RDMA_BUFFER_SIZE / MSG_SIZE_MAX);
      max_count = RDMA_SEND_COUNT;
    }
};
#endif
class Transport {
	public:
		void read_ifconfig(const char * ifaddr_file);
		void init();
    void shutdown();
    uint64_t get_socket_count();
    string get_path();
    Socket * get_socket();
    uint64_t get_port_id(uint64_t src_node_id, uint64_t dest_node_id);
    uint64_t get_port_id(uint64_t src_node_id, uint64_t dest_node_id, uint64_t send_thread_id);
    uint64_t get_twoside_port_id(uint64_t src_node_id, uint64_t dest_node_id, uint64_t send_thread_id);
    uint64_t get_twoside_id(uint64_t src_node_id, uint64_t dest_node_id, uint64_t send_thread_id);
    uint64_t get_thd_port_id(uint64_t src_node_id, uint64_t dest_node_id, uint64_t send_thread_id);
    Socket * bind(uint64_t port_id);
    Socket * connect(uint64_t dest_id,uint64_t port_id);
    void send_msg(uint64_t send_thread_id, uint64_t dest_node_id, void * sbuf,int size);
    void rdma_send_msg(uint64_t send_thread_id, uint64_t dest_node_id, char * sbuf,int size);
    void rdma_thd_send_msg(uint64_t send_thread_id, uint64_t dest_node_id, Message * msg);
    std::vector<Message*> * recv_msg(uint64_t thd_id);
    std::vector<Message*> * rdma_recv_msg(uint64_t thd_id);
		void simple_send_msg(int size);
		uint64_t simple_recv_msg();
    void create_server(uint64_t port, uint64_t dest_node_id);
    void create_client(uint64_t port, uint64_t dest_node_id);
#ifdef USE_RDMA
    //client need
    rdmaio::Arc<rdmaio::RNic> nic;
    //-----------------send------------------//
    std::vector<rdmaio::ConnectManager> cms;
    std::vector<rdmaio::Arc<rdmaio::rmem::RegHandler>> send_handlers;
    std::vector<rdmaio::Arc<rdmaio::rmem::RegHandler>> send_thread_handlers;
    // rdmaio::Arc<rdmaio::qp::RDMARC> send_qps[50];
    rdma_send_qps send_qps[200];
    rdma_send_qps sned_thread_qps[200];
    //-----------------recv------------------//
    std::vector<rdmaio::Arc<rdmaio::rmem::RegHandler>> recv_handlers;
    rdmaio::Arc<rdmaio::qp::Dummy> recv_qps[200];
    rdmaio::Arc<rdmaio::qp::RecvEntries<RDMA_ENTRY_NUM>> recv_rss[200];

    std::vector<rdmaio::Arc<rdmaio::rmem::RegHandler>> recv_thread_handlers;
    rdmaio::Arc<rdmaio::qp::Dummy> recv_thread_qps[200];
    rdmaio::Arc<rdmaio::qp::RecvEntries<RDMA_ENTRY_NUM>> recv_thread_rss[200];

    pthread_mutex_t * latch;
    pthread_mutex_t * latch_send;
    mbuf ** buffer; 
    char * get_next_buf(rdma_send_qps* send_qpi);
#endif

	private:
    uint64_t rr;
    std::map<std::pair<uint64_t, uint64_t>, Socket*> send_sockets;  // dest_node_id,send_thread_id :
                                                                  // socket
    std::vector<Socket*> recv_sockets;
    uint64_t _node_cnt;
    uint64_t _sock_cnt;
    uint64_t _s_cnt;
		char ** ifaddr;
    int * endpoint_id;

};

#endif
