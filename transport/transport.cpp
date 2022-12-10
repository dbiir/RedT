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
#include "transport.h"

#include <stdio.h>
#include "pthread.h"
#include <iostream>

#include "global.h"
#include "manager.h"
#include "message.h"
#include "nn.hpp"
#include "query.h"
#include "tpcc_query.h"

#ifdef USE_RDMA
	#include "sig_sync.hpp"
	#include "lib.hh"
	#include "qps/rc_recv_manager.hh"
	#include "qps/recv_iter.hh"
	#include "qps/mod.hh"
	#include "utils/timer.hh"
	using namespace rdmaio;
	using namespace rdmaio::rmem;
	using namespace rdmaio::qp;
#endif

/*
#include <stdio.h>
#include <iostream>
#include "global.h"
#include "manager.h"
#include "transport.h"
#include "nn.hpp"
#include "tpcc_query.h"
#include "query.h"
#include "message.h"
*/

#define MAX_IFADDR_LEN 20 // max # of characters in name of address

void Transport::read_ifconfig(const char * ifaddr_file) {

		ifaddr = new char *[g_total_node_cnt];

		uint64_t cnt = 0;
	printf("Reading ifconfig file: %s\n",ifaddr_file);
		ifstream fin(ifaddr_file);
		string line;
	while (getline(fin, line)) {
			//memcpy(ifaddr[cnt],&line[0],12);
			ifaddr[cnt] = new char[line.length()+1];
		strcpy(ifaddr[cnt],&line[0]);
			printf("%ld: %s\n",cnt,ifaddr[cnt]);
			cnt++;
		}
	printf("%lu %u\n", cnt, g_total_node_cnt);
	assert(cnt == g_total_node_cnt);
}

uint64_t Transport::get_socket_count() {
	uint64_t sock_cnt = 0;
	if(ISCLIENT)
		sock_cnt = (g_total_node_cnt)*2 + g_client_send_thread_cnt * g_servers_per_client;
	else
		sock_cnt = (g_total_node_cnt)*2 + g_client_send_thread_cnt;
	return sock_cnt;
}

string Transport::get_path() {
	string path;
	#if SHMEM_ENV
	path = "/dev/shm/";
	#else
		char * cpath;
	cpath = getenv("SCHEMA_PATH");
		if(cpath == NULL)
			path = "./";
		else
			path = string(cpath);
	#endif
		path += "ifconfig.txt";
	return path;

}

Socket * Transport::get_socket() {
  //Socket * socket = new Socket;
  Socket * socket = (Socket*) mem_allocator.align_alloc(sizeof(Socket));
  new(socket) Socket();
	int timeo = 1000; // timeout in ms
	int stimeo = 1000; // timeout in ms
  int opt = 0;
  socket->sock.setsockopt(NN_SOL_SOCKET,NN_RCVTIMEO,&timeo,sizeof(timeo));
  socket->sock.setsockopt(NN_SOL_SOCKET,NN_SNDTIMEO,&stimeo,sizeof(stimeo));
  // NN_TCP_NODELAY doesn't cause TCP_NODELAY to be set -- nanomsg issue #118
  socket->sock.setsockopt(NN_SOL_SOCKET,NN_TCP_NODELAY,&opt,sizeof(opt));
  return socket;
}

uint64_t Transport::get_port_id(uint64_t src_node_id, uint64_t dest_node_id) {
  uint64_t port_id = TPORT_PORT;
  port_id += g_total_node_cnt * dest_node_id;
  port_id += src_node_id;
  DEBUG("Port ID:  %ld -> %ld : %ld\n",src_node_id,dest_node_id,port_id);
  return port_id;
}

uint64_t Transport::get_twoside_port_id(uint64_t src_node_id, uint64_t dest_node_id,
								uint64_t send_thread_id) {
  uint64_t port_id = 0;
  //DEBUG("Calc port id %ld %ld %ld\n",src_node_id,dest_node_id,send_thread_id);
  port_id += g_total_node_cnt * dest_node_id;
  //DEBUG("%ld\n",port_id);
  port_id += src_node_id;
  //DEBUG("%ld\n",port_id);
  //  uint64_t max_send_thread_cnt = g_send_thread_cnt > g_client_send_thread_cnt ?
  // g_send_thread_cnt : g_client_send_thread_cnt;
//  port_id *= max_send_thread_cnt;
  port_id += send_thread_id * g_total_node_cnt * g_total_node_cnt;
  //DEBUG("%ld\n",port_id);
  port_id += TPORT_TWOSIDE_PORT;
  //DEBUG("%ld\n",port_id);
  return port_id;
}

uint64_t Transport::get_twoside_id(uint64_t src_node_id, uint64_t dest_node_id,
								uint64_t send_thread_id) {
  uint64_t port_id = 0;
  //DEBUG("Calc port id %ld %ld %ld\n",src_node_id,dest_node_id,send_thread_id);
  port_id += g_total_node_cnt * dest_node_id;
  //DEBUG("%ld\n",port_id);
  port_id += src_node_id;
  //DEBUG("%ld\n",port_id);
  //  uint64_t max_send_thread_cnt = g_send_thread_cnt > g_client_send_thread_cnt ?
  // g_send_thread_cnt : g_client_send_thread_cnt;
//  port_id *= max_send_thread_cnt;
  port_id += send_thread_id * g_total_node_cnt * g_total_node_cnt;
  //DEBUG("%ld\n",port_id);
  //DEBUG("%ld\n",port_id);
  return port_id;
}

uint64_t Transport::get_thd_port_id(uint64_t src_node_id, uint64_t dest_node_id,
								uint64_t send_thread_id) {
  uint64_t port_id = TPORT_TWOSIDE_PORT+100;
  port_id += src_node_id * g_thread_cnt * g_thread_cnt;
  port_id += dest_node_id * g_thread_cnt;
  port_id += send_thread_id;
  //DEBUG("%ld\n",port_id);
  //DEBUG("%ld\n",port_id);
  return port_id;
}
#if NETWORK_DELAY_TEST || !ENVIRONMENT_EC2
uint64_t Transport::get_port_id(uint64_t src_node_id, uint64_t dest_node_id,
								uint64_t send_thread_id) {
  uint64_t port_id = 0;
  DEBUG("Calc port id %ld %ld %ld\n",src_node_id,dest_node_id,send_thread_id);
  port_id += g_total_node_cnt * dest_node_id;
  DEBUG("%ld\n",port_id);
  port_id += src_node_id;
  DEBUG("%ld\n",port_id);
  //  uint64_t max_send_thread_cnt = g_send_thread_cnt > g_client_send_thread_cnt ?
  // g_send_thread_cnt : g_client_send_thread_cnt;
//  port_id *= max_send_thread_cnt;
  port_id += send_thread_id * g_total_node_cnt * g_total_node_cnt;
  DEBUG("%ld\n",port_id);
  port_id += TPORT_PORT;
  DEBUG("%ld\n",port_id);
  printf("Port ID:  %ld, %ld -> %ld : %ld\n",send_thread_id,src_node_id,dest_node_id,port_id);
  return port_id;
}
#else

uint64_t Transport::get_port_id(uint64_t src_node_id, uint64_t dest_node_id,
								uint64_t send_thread_id) {
  uint64_t port_id = 0;
  DEBUG("Calc port id %ld %ld %ld\n",src_node_id,dest_node_id,send_thread_id);
  port_id += dest_node_id + src_node_id;
  DEBUG("%ld\n",port_id);
  port_id += send_thread_id * g_total_node_cnt * 2;
  DEBUG("%ld\n",port_id);
  port_id += TPORT_PORT;
  DEBUG("%ld\n",port_id);
  printf("Port ID:  %ld, %ld -> %ld : %ld\n",send_thread_id,src_node_id,dest_node_id,port_id);
  return port_id;
}
#endif



Socket * Transport::bind(uint64_t port_id) {
  Socket * socket = get_socket();
  char socket_name[MAX_TPORT_NAME];
#if TPORT_TYPE == IPC
  sprintf(socket_name,"ipc://node_%ld.ipc",port_id);
#else
/*#if ENVIRONMENT_EC2
  sprintf(socket_name,"tcp://eth0:%ld",port_id);
#else*/
  sprintf(socket_name,"tcp://%s:%ld",ifaddr[g_node_id],port_id);
//#endif
#endif
  printf("Sock Binding to %s %d\n",socket_name,g_node_id);
  int rc = socket->sock.bind(socket_name);
  if(rc < 0) {
	printf("Bind Error: %d %s\n",errno,strerror(errno));
	assert(false);
  }
  return socket;
}

Socket * Transport::connect(uint64_t dest_id,uint64_t port_id) {
  Socket * socket = get_socket();
  char socket_name[MAX_TPORT_NAME];
#if TPORT_TYPE == IPC
  sprintf(socket_name,"ipc://node_%ld.ipc",port_id);
#else
/*#if ENVIRONMENT_EC2
  sprintf(socket_name,"tcp://eth0;%s:%ld",ifaddr[dest_id],port_id);
#else*/
  sprintf(socket_name,"tcp://%s;%s:%ld",ifaddr[g_node_id],ifaddr[dest_id],port_id);
//#endif
#endif
  printf("Sock Connecting to %s %d -> %ld\n",socket_name,g_node_id,dest_id);


  int rc = socket->sock.connect(socket_name);
  if(rc < 0) {
	printf("Connect Error: %d %s\n",errno,strerror(errno));
	assert(false);
  }
  return socket;
}

#ifdef USE_RDMA
void Transport::create_server(uint64_t port, uint64_t dest_node_id) {
	RCtrl ctrl(port);
	RecvManager<RDMA_ENTRY_NUM, RDMA_BUFFER_ITEM_SIZE> manager(ctrl);
	//auto tport_man.nic = RNic::create(RNicInfo::query_dev_names().at(RDMA_USE_NIC_IDX)).value();
	RDMA_ASSERT(ctrl.opened_nics.reg(0, tport_man.nic));
	// RDMA_LOG(4) << g_node_id << " RDMA listens " << dest_node_id << " at " << port;

	//1,create receive cq
	auto recv_cq_res = ::rdmaio::qp::Impl::create_cq(tport_man.nic, RDMA_ENTRY_NUM);
	RDMA_ASSERT(recv_cq_res == IOCode::Ok);
	auto recv_cq = std::get<0>(recv_cq_res.desc);

	//2,prepare the message buffer with allocator
	auto mem = Arc<RMem>(new RMem(RDMA_BUFFER_SIZE)); // a memory with 4M bytes
	auto handler = RegHandler::create(mem, tport_man.nic).value();
	auto alloc = std::make_shared<SimpleAllocator>(mem, handler->get_reg_attr().value().key);

	//3,register receive cq
	manager.reg_recv_cqs.create_then_reg(RDMA_CQ_NAME+to_string(port), recv_cq, alloc);
	// RDMA_LOG(4) << "Register " << RDMA_CQ_NAME+to_string(port);
	int64_t reg_mem_name = RDMA_REG_MEM_NAME + int64_t(port) - TPORT_TWOSIDE_PORT;
	// RDMA_LOG(4) << "Reg_mem_name is " << reg_mem_name;
	ctrl.registered_mrs.reg(reg_mem_name, handler);
	ctrl.start_daemon();
	sleep(30);
	// RDMA_LOG(4) << "Recv qp: " << "rdma_qp"+to_string(port);
	auto recv_qp = ctrl.registered_qps.query("rdma_qp"+to_string(port)).value();
	auto recv_rs = manager.reg_recv_entries.query("rdma_qp"+to_string(port)).value();
	pthread_mutex_lock( tport_man.latch );

	tport_man.recv_handlers.push_back(handler);
	tport_man.recv_qps[port-TPORT_TWOSIDE_PORT] = recv_qp;
	tport_man.recv_rss[port-TPORT_TWOSIDE_PORT] = recv_rs;
	pthread_mutex_unlock( tport_man.latch );
	u64 recv_cnt = 0;

	// RDMA_LOG(2) << "a new recv";
	pthread_mutex_lock( tport_man.latch );
	auto recv_qpi = tport_man.recv_qps[port-TPORT_TWOSIDE_PORT];
	auto recv_rsi = tport_man.recv_rss[port-TPORT_TWOSIDE_PORT];
	pthread_mutex_unlock( tport_man.latch );
	for (RecvIter<Dummy, RDMA_ENTRY_NUM> iter(recv_qpi, recv_rsi); iter.has_msgs();
		iter.next()) {
		auto imm_msg = iter.cur_msg().value();

		auto buf = static_cast<char *>(std::get<1>(imm_msg));
		const std::string msg(buf, 2048);  // wrap the received msg

		u64 seq = std::atoi(msg.c_str());
		recv_cnt++;
		// RDMA_LOG(2) << "the message content is" << buf;
	}
}
void Transport::create_client(uint64_t port, uint64_t dest_node_id) {
	//1 create the local QP to send
	auto qp = RDMARC::create(tport_man.nic, QPConfig()).value();
	string ifaddr_temp = ifaddr[dest_node_id];
	string addr = ifaddr_temp + ":" + std::to_string(port);
	// cout << addr << endl;

	ConnectManager cm(addr);

	if (cm.wait_ready(10000000, 4) == IOCode::Timeout)
		RDMA_LOG(4) << "connect to the " << ifaddr[dest_node_id] << " timeout!";
	sleep(1);
	//2 create the remote QP and connect
	auto qp_res = cm.cc_rc_msg("rdma_qp"+to_string(port),
		RDMA_CQ_NAME+to_string(port), 4096, qp, RDMA_USE_NIC_IDX, QPConfig());
	// cout << "rdma_qp"+to_string(port) << "  " << RDMA_CQ_NAME+to_string(port);
	RDMA_ASSERT(qp_res == IOCode::Ok) << std::get<0>(qp_res.desc);
	auto fetch_res = cm.fetch_remote_mr(RDMA_REG_MEM_NAME + int64_t(port) - TPORT_TWOSIDE_PORT);
	rmem::RegAttr remote_attr = std::get<1>(fetch_res.desc);

	auto local_mr = RegHandler::create(Arc<RMem>(new RMem(RDMA_BUFFER_SIZE/*1024 * 1024*/)), tport_man.nic).value();
	char *buf = (char *)(local_mr->get_reg_attr().value().buf);
	qp->bind_remote_mr(remote_attr);
	qp->bind_local_mr(local_mr->get_reg_attr().value());
	pthread_mutex_lock( tport_man.latch_send );
	tport_man.cms.push_back(cm);
	tport_man.send_handlers.push_back(local_mr);
    rdma_send_qps sqp(qp);
	tport_man.send_qps[port-TPORT_TWOSIDE_PORT] = sqp;
	pthread_mutex_unlock( tport_man.latch_send );
	// RDMA_LOG(2) << "rc client ready to send message to the server!";
	pthread_mutex_lock( tport_man.latch_send );

	auto send_qpi = &tport_man.send_qps[port-TPORT_TWOSIDE_PORT];
	char *buf2 = (char *)(send_qpi->send_qps->local_mr->buf);
	pthread_mutex_unlock( tport_man.latch_send );
	// Timer timer;
	for (int i = 1; i <= 1; ++i) {
		std::string msg = std::to_string(i);
		memset(buf2, 0, msg.size() + 1);
		memcpy(buf2, msg.data(), msg.size());
		// RDMA_LOG(2) << "the message content is" << buf2;
		printf("qp buf ptr:%p\n", buf2);
		auto res_s = send_qpi->send_qps->send_normal(
			{.op = IBV_WR_SEND_WITH_IMM,
			.flags = IBV_SEND_SIGNALED,
			.len = (u32) msg.size() + 1,
			.wr_id = 0},
			{.local_addr = reinterpret_cast<RMem::raw_ptr_t>(buf2),
			.remote_addr = 0,
			.imm_data = 0});

		RDMA_ASSERT(res_s == IOCode::Ok);
		auto res_p = send_qpi->send_qps->wait_one_comp();
		RDMA_ASSERT(res_p == IOCode::Ok);
	}
}
#if USE_RDMA == CHANGE_TCP_ONLY
void Transport::init() {
	_sock_cnt = get_socket_count();
	rr = 0;
	printf("Tport Init %d: %ld\n",g_node_id,_sock_cnt);
	string path = get_path();
	read_ifconfig(path.c_str());


	latch = (pthread_mutex_t *)mem_allocator.alloc(sizeof(pthread_mutex_t));
	pthread_mutex_init( latch, NULL );
	latch_send = (pthread_mutex_t *)mem_allocator.alloc(sizeof(pthread_mutex_t));
	pthread_mutex_init( latch_send, NULL );
	tport_man.nic = RNic::create(RNicInfo::query_dev_names().at(RDMA_USE_NIC_IDX)).value();
	vector<std::thread> th_bind;
	vector<std::thread> th_connect;
	for(uint64_t node_id = 0; node_id < g_total_node_cnt; node_id++) {
		if (node_id == g_node_id) continue;
		// Listening ports
		if(ISCLIENTN(node_id)) {
		for (uint64_t client_thread_id = g_client_thread_cnt + g_client_rem_thread_cnt;
			client_thread_id <
				g_client_thread_cnt + g_client_rem_thread_cnt + g_client_send_thread_cnt;
			client_thread_id++) {
				th_bind.push_back(std::thread([=](){
					uint64_t port_id =
						get_twoside_port_id(node_id, g_node_id, client_thread_id % g_client_send_thread_cnt);
					create_server(port_id, node_id);
				}));
			}
		} else {
		for (uint64_t server_thread_id = g_thread_cnt + g_rem_thread_cnt;
			server_thread_id < g_thread_cnt + g_rem_thread_cnt + g_send_thread_cnt;
			server_thread_id++) {
				th_bind.push_back(std::thread([=](){
					uint64_t port_id = get_twoside_port_id(node_id,g_node_id,server_thread_id % g_send_thread_cnt);
					create_server(port_id, node_id);
				}));
			}
		}
		// Sending ports
		if(ISCLIENTN(g_node_id)) {
			for (uint64_t client_thread_id = g_client_thread_cnt + g_client_rem_thread_cnt;
				client_thread_id <
					g_client_thread_cnt + g_client_rem_thread_cnt + g_client_send_thread_cnt;
				client_thread_id++) {
				th_connect.push_back(std::thread([=](){
				sleep(1);
				// cout << g_node_id << node_id << "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" << endl;
					uint64_t port_id =
					get_twoside_port_id(g_node_id, node_id, client_thread_id % g_client_send_thread_cnt);
					create_client(port_id, node_id);
				}));
			}
		} else {
			for (uint64_t server_thread_id = g_thread_cnt + g_rem_thread_cnt;
				server_thread_id < g_thread_cnt + g_rem_thread_cnt + g_send_thread_cnt;
				server_thread_id++) {
				th_connect.push_back(std::thread([=](){
					sleep(1);
					// cout << g_node_id << node_id << "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" << endl;
					uint64_t port_id = get_twoside_port_id(g_node_id,node_id,server_thread_id % g_send_thread_cnt);
					create_client(port_id, node_id);
				}));
			}
		}
	}

	for (std::thread &th: th_connect) {
		th.join();
	}

	for (std::thread &th: th_bind) {
		th.join();
	}
		fflush(stdout);
}
#elif USE_RDMA == CHANGE_MSG_QUEUE
void Transport::init() {
	_sock_cnt = get_socket_count();
	rr = 0;
	printf("Tport Init %d: %ld\n",g_node_id,_sock_cnt);
	string path = get_path();
	read_ifconfig(path.c_str());

	buffer = (mbuf **) mem_allocator.align_alloc(sizeof(mbuf*) * g_total_node_cnt * g_thread_cnt);
	for(uint64_t n = 0; n < g_total_node_cnt * g_thread_cnt; n++) {
		DEBUG_M("MessageThread::init mbuf alloc\n");
		buffer[n] = (mbuf *)mem_allocator.align_alloc(sizeof(mbuf));
		buffer[n]->init(n / g_thread_cnt);
		buffer[n]->reset(n / g_thread_cnt);
	}
	latch = (pthread_mutex_t *)mem_allocator.alloc(sizeof(pthread_mutex_t));
	pthread_mutex_init( latch, NULL );
	latch_send = (pthread_mutex_t *)mem_allocator.alloc(sizeof(pthread_mutex_t));
	pthread_mutex_init( latch_send, NULL );
	tport_man.nic = RNic::create(RNicInfo::query_dev_names().at(RDMA_USE_NIC_IDX)).value();
	vector<std::thread> th_bind;
	vector<std::thread> th_connect;
	for(uint64_t node_id = 0; node_id < g_total_node_cnt; node_id++) {
		if (node_id == g_node_id) continue;
		// Listening ports
		if(ISCLIENTN(node_id)) {
		for (uint64_t client_thread_id = 0; client_thread_id < g_thread_cnt; client_thread_id++) {
				th_bind.push_back(std::thread([=](){
					uint64_t port_id = get_thd_port_id(node_id, g_node_id, client_thread_id);
					create_server(port_id, node_id);
				}));
			}
		} else {
		for (uint64_t server_thread_id = 0; server_thread_id < g_client_thread_cnt; server_thread_id++) {
				th_bind.push_back(std::thread([=](){
					uint64_t port_id = get_thd_port_id(node_id,g_node_id,server_thread_id);
					create_server(port_id, node_id);
				}));
			}
		}
		// Sending ports
		if(ISCLIENTN(g_node_id)) {
			for (uint64_t client_thread_id = 0; client_thread_id < g_client_thread_cnt; client_thread_id++) {
				th_connect.push_back(std::thread([=](){
				sleep(1);
				// cout << g_node_id << node_id << "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" << endl;
					uint64_t port_id =
					get_thd_port_id(g_node_id, node_id, client_thread_id);
					create_client(port_id, node_id);
				}));
			}
		} else {
			for (uint64_t server_thread_id = 0; server_thread_id < g_thread_cnt; server_thread_id++) {
				th_connect.push_back(std::thread([=](){
					sleep(1);
					// cout << g_node_id << node_id << "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" << endl;
					uint64_t port_id = get_thd_port_id(g_node_id,node_id,server_thread_id);
					create_client(port_id, node_id);
				}));
			}
		}
	}

	for (std::thread &th: th_connect) {
		th.join();
	}

	for (std::thread &th: th_bind) {
		th.join();
	}
		fflush(stdout);
}
#endif
#else
void Transport::init() {
	_sock_cnt = get_socket_count();

	rr = 0;
	printf("Tport Init %d: %ld\n",g_node_id,_sock_cnt);

 	string path = get_path();
	read_ifconfig(path.c_str());

  for(uint64_t node_id = 0; node_id < g_total_node_cnt; node_id++) {

    #if ONE_NODE_RECIEVE == 1 && defined(NO_REMOTE) && LESS_DIS_NUM == 10
    #else
    if (node_id == g_node_id) continue;
    #endif
    // Listening ports
    if(ISCLIENTN(node_id)) {
      for (uint64_t client_thread_id = g_client_thread_cnt + g_client_rem_thread_cnt;
           client_thread_id <
               g_client_thread_cnt + g_client_rem_thread_cnt + g_client_send_thread_cnt;
           client_thread_id++) {
        uint64_t port_id =
            get_port_id(node_id, g_node_id, client_thread_id % g_client_send_thread_cnt);
        Socket * sock = bind(port_id);
        recv_sockets.push_back(sock);
        DEBUG("Socket insert: {%ld}: %ld\n",node_id,(uint64_t)sock);
      }
    } else {
      for (uint64_t server_thread_id = g_thread_cnt + g_rem_thread_cnt;
           server_thread_id < g_thread_cnt + g_rem_thread_cnt + g_send_thread_cnt;
           server_thread_id++) {
        uint64_t port_id = get_port_id(node_id,g_node_id,server_thread_id % g_send_thread_cnt);
        Socket * sock = bind(port_id);
        recv_sockets.push_back(sock);
        DEBUG("Socket insert: {%ld}: %ld\n",node_id,(uint64_t)sock);
      }
    }
    // Sending ports
    if(ISCLIENTN(g_node_id)) {
      for (uint64_t client_thread_id = g_client_thread_cnt + g_client_rem_thread_cnt;
           client_thread_id <
               g_client_thread_cnt + g_client_rem_thread_cnt + g_client_send_thread_cnt;
           client_thread_id++) {
        uint64_t port_id =
            get_port_id(g_node_id, node_id, client_thread_id % g_client_send_thread_cnt);
        std::pair<uint64_t,uint64_t> sender = std::make_pair(node_id,client_thread_id);
        Socket * sock = connect(node_id,port_id);
        send_sockets.insert(std::make_pair(sender,sock));
        DEBUG("Socket insert: {%ld,%ld}: %ld\n",node_id,client_thread_id,(uint64_t)sock);
      }
    } else {
      for (uint64_t server_thread_id = g_thread_cnt + g_rem_thread_cnt;
           server_thread_id < g_thread_cnt + g_rem_thread_cnt + g_send_thread_cnt;
           server_thread_id++) {
        uint64_t port_id = get_port_id(g_node_id,node_id,server_thread_id % g_send_thread_cnt);
        std::pair<uint64_t,uint64_t> sender = std::make_pair(node_id,server_thread_id);
        Socket * sock = connect(node_id,port_id);
        send_sockets.insert(std::make_pair(sender,sock));
        DEBUG("Socket insert: {%ld,%ld}: %ld\n",node_id,server_thread_id,(uint64_t)sock);
      }
    }
  }


	fflush(stdout);
}

#endif
#ifdef USE_RDMA
#if 0
char* Transport::get_next_buf(rdma_send_qps* send_qpi) {
  char *buf = (char *)(send_qpi->send_qps->local_mr->buf);
  if (send_qpi->buffer_idx > send_qpi->buffer_size) send_qpi->buffer_idx = 0;
  buf = buf + MSG_SIZE_MAX * send_qpi->buffer_idx;
  send_qpi->buffer_idx ++;
  return buf;

}
#else
char* Transport::get_next_buf(rdma_send_qps* send_qpi) {
  char *buf = (char *)(send_qpi->send_qps->local_mr->buf);
  if (send_qpi->count > send_qpi->max_count) return NULL;
  else {
    buf = buf + MSG_SIZE_MAX * send_qpi->count;
    // send_qpi->count ++;
    return buf;
  }
}
#endif
void Transport::rdma_thd_send_msg(uint64_t send_thread_id, uint64_t dest_node_id, Message * msg) {
	mbuf * sbuf;
	//mbuf * buffer;
	// sbuf = (mbuf *)mem_allocator.align_alloc(sizeof(mbuf));

	// sbuf->init(dest_node_id);
	// sbuf->reset(dest_node_id);

	assert(msg);
	assert(dest_node_id < g_total_node_cnt);
	assert(dest_node_id != g_node_id);
	sbuf = buffer[dest_node_id * g_thread_cnt + send_thread_id];
	//sbuf = buffer;
	uint64_t copy_starttime = get_sys_clock();
	msg->copy_to_buf(&(sbuf->buffer[sbuf->ptr]));
	INC_STATS(send_thread_id, msg_copy_output_time,get_sys_clock() - copy_starttime);
	DEBUG("%ld Buffered Msg %d, (%ld,%ld) to %ld\n", send_thread_id, msg->rtype, msg->txn_id, msg->batch_id, dest_node_id);
	sbuf->cnt += 1;
    sbuf->ptr += msg->get_size();
	if(CC_ALG != CALVIN ) {
        Message::release_message(msg);
    }
	if (sbuf->starttime == 0) sbuf->starttime = get_sys_clock();
	uint64_t starttime = get_sys_clock();

	((uint32_t*)sbuf->buffer)[2] = sbuf->cnt;
	INC_STATS(send_thread_id,mbuf_send_intv_time,get_sys_clock() - sbuf->starttime);
	DEBUG("Send batch of %ld msgs to %ld\n",sbuf->cnt,dest_node_id);
    fflush(stdout);
    sbuf->set_send_time(get_sys_clock());
	//sleep(1);
	// printf("node %ld thread %ld send to remote node %ld\n", g_node_id, send_thread_id, dest_node_id);
    rdma_send_msg(send_thread_id, dest_node_id, sbuf->buffer, sbuf->ptr);
	sbuf->reset(dest_node_id);
	//free(sbuf->buffer);


	INC_STATS(send_thread_id,msg_batch_size_msgs,sbuf->cnt);
    INC_STATS(send_thread_id,msg_batch_size_bytes,sbuf->ptr);
    if(ISSERVERN(dest_node_id)) {
      INC_STATS(send_thread_id,msg_batch_size_bytes_to_server,sbuf->ptr);
    } else if (ISCLIENTN(dest_node_id)){
      INC_STATS(send_thread_id,msg_batch_size_bytes_to_client,sbuf->ptr);
    }
	//mem_allocator.free(sbuf,sizeof(mbuf));
    INC_STATS(send_thread_id,msg_batch_cnt,1);

	INC_STATS(send_thread_id,mtx[12],get_sys_clock() - starttime);
}
void Transport::rdma_send_msg(uint64_t send_thread_id, uint64_t dest_node_id, char * sbuf, int size) {
	uint64_t starttime = get_sys_clock();
	//g_node_id: local node_id
	//dest_node_id: remote node_id
	uint64_t port_id;
#if USE_RDMA == CHANGE_TCP_ONLY
	if(ISCLIENTN(g_node_id)) {
		port_id = get_twoside_port_id(g_node_id, dest_node_id, (g_client_thread_cnt + g_client_rem_thread_cnt) % g_client_send_thread_cnt);
	} else {
		port_id = get_twoside_port_id(g_node_id, dest_node_id, (g_thread_cnt + g_rem_thread_cnt) % g_send_thread_cnt);
	}
#elif USE_RDMA == CHANGE_MSG_QUEUE
		port_id = get_thd_port_id(g_node_id, dest_node_id, send_thread_id);
#endif

	pthread_mutex_lock( tport_man.latch );
	auto send_qpi = &tport_man.send_qps[port_id-TPORT_TWOSIDE_PORT];
	char* buf = get_next_buf(send_qpi);
	pthread_mutex_unlock( tport_man.latch );
	assert (buf != NULL);
	memset(buf, 0, size + sizeof(size) + 1);
	memcpy(buf, &size, 4);
	memcpy((buf+4), sbuf, size);

	DEBUG("%ld Sending batch of %d bytes to node %ld on qp\n", send_thread_id, size,
		dest_node_id, port_id);

	unsigned int send_flags = send_qpi->count + 1 > send_qpi->max_count ? IBV_SEND_SIGNALED : 0;
	// unsigned int send_flags = send_qpi->count == 0 ? IBV_SEND_SIGNALED : 0;
	auto res_s = send_qpi->send_qps->send_normal(
		{.op = IBV_WR_SEND_WITH_IMM,
		.flags = send_flags,
		.len = (u32) size + sizeof(size) + 1,
		.wr_id = 0},
	    {.local_addr = reinterpret_cast<RMem::raw_ptr_t>(buf),
    .remote_addr = 0,
		.imm_data = 0});
	RDMA_ASSERT(res_s == IOCode::Ok);
	// void* pVoid = buf;
	// RDMA_LOG(4) << port_id-TPORT_TWOSIDE_PORT << " RDMA send " << (send_qpi->count) << " buf size "<< (u32) size + sizeof(size) + 1 << " buf ptr " << pVoid << " flags " << send_flags;
	if (send_qpi->count >= send_qpi->max_count) {
		auto res_p = send_qpi->send_qps->wait_one_comp(10000000);
		// RDMA_LOG(4) << port_id-TPORT_TWOSIDE_PORT << " RDMA send wait ";
		// RDMA_ASSERT(res_p == IOCode::Ok);
		pthread_mutex_lock( tport_man.latch );
		send_qpi->count = 0;
		pthread_mutex_unlock( tport_man.latch );
	} else {
		send_qpi->count ++;
	}

	DEBUG("%ld Batch of %d bytes sent to node %ld\n",send_thread_id,size + sizeof(size) + 1,dest_node_id);

	INC_STATS(send_thread_id,msg_send_time,get_sys_clock() - starttime);
	INC_STATS(send_thread_id,msg_send_cnt,1);
}
#else
// rename sid to send thread id
void Transport::send_msg(uint64_t send_thread_id, uint64_t dest_node_id, void * sbuf,int size) {
	uint64_t starttime = get_sys_clock();

	Socket * socket = send_sockets.find(std::make_pair(dest_node_id,send_thread_id))->second;

	void * buf = nn_allocmsg(size,0);
	memcpy(buf,sbuf,size);
	DEBUG("%ld Sending batch of %d bytes to node %ld on socket %ld\n", send_thread_id, size,
			dest_node_id, (uint64_t)socket);

  	int rc = -1;
	// int start_time = get_sys_clock();
	// while (rc < 0 && (!simulation->is_setup_done() ||
						// (simulation->is_setup_done() && !simulation->is_done()))) {
	while (rc < 0 && (!simulation->is_done())) {

		rc= socket->sock.send(&buf,NN_MSG,NN_DONTWAIT);
		// int end_time = get_sys_clock();
		// if (end_time - start_time > MESSAGE_SEND_RETRY_TIME) {
		// 	printf("%ld send msg to node %ld failed\n",send_thread_id,dest_node_id);
		// 	break;
		// }
	}

  	DEBUG("%ld Batch of %d bytes sent to node %ld\n",send_thread_id,size,dest_node_id);

	INC_STATS(send_thread_id,msg_send_time,get_sys_clock() - starttime);
	INC_STATS(send_thread_id,msg_send_cnt,1);
}
#endif
#ifdef USE_RDMA
std::vector<Message*> * Transport::rdma_recv_msg(uint64_t thd_id) {
  	int bytes = 0;
	char * buf;
	uint64_t starttime = get_sys_clock();
// 	uint64_t starttime = get_sys_clock();
//   std::vector<Message*> * msgs = NULL;
//   //uint64_t ctr = starttime % recv_sockets.size();
    uint64_t rand = (starttime % (g_total_node_cnt)) / g_this_rem_thread_cnt;
//   // uint64_t ctr = ((thd_id % g_this_rem_thread_cnt) % recv_sockets.size()) + rand *
//   // g_this_rem_thread_cnt;
    uint64_t ctr = thd_id % g_this_rem_thread_cnt;
	assert(ctr < g_total_node_cnt);
	if(g_this_rem_thread_cnt < g_total_node_cnt) {
		ctr += rand * g_this_rem_thread_cnt;
		while(ctr >= g_total_node_cnt) {
			ctr -= g_this_rem_thread_cnt;
		}
	}
	if(ctr == g_node_id) ctr = (ctr + 1) % g_total_node_cnt;
	assert(ctr < g_total_node_cnt);
	uint64_t start_ctr = ctr;
	// uint64_t ctr;
	// ctr = starttime % (g_node_cnt + g_client_node_cnt);
	// while(ctr == g_node_id) {
	// 	starttime = get_sys_clock();
	// 	ctr = starttime % (g_node_cnt + g_client_node_cnt);
	// }
	std::vector<Message*> * msgs =new std::vector<Message*>;
	uint64_t port_id;
#if USE_RDMA == CHANGE_TCP_ONLY
	if(ISCLIENTN(g_node_id)) {
		port_id =
				get_twoside_port_id(ctr, g_node_id, (g_client_thread_cnt + g_client_rem_thread_cnt) % g_client_send_thread_cnt);
	} else {
		port_id = get_twoside_port_id(ctr, g_node_id, (g_thread_cnt + g_rem_thread_cnt) % g_send_thread_cnt);
	}
#elif USE_RDMA == CHANGE_MSG_QUEUE
    uint64_t send_thd_id;
	starttime = get_sys_clock();
	if(ISCLIENTN(g_node_id)) {
		send_thd_id = starttime % (g_thread_cnt);
	} else {
		send_thd_id = starttime % (g_client_thread_cnt);
	}
	
	port_id = get_thd_port_id(ctr, g_node_id, send_thd_id);
#endif
	//uint64_t start_ctr = ctr;
	//printf("%rdma will recv msg from node %ld\n", ctr);
	while (bytes <= 0 && (!simulation->is_setup_done() ||
							(simulation->is_setup_done() && !simulation->is_done()))) {

		//sleep(1);
		// starttime = get_sys_clock();
		// ctr = starttime % (g_node_cnt + g_client_node_cnt);
		// while(ctr == g_node_id) {
		// 	starttime = get_sys_clock();
		// 	ctr = starttime % (g_node_cnt + g_client_node_cnt);
		// }
		//printf("%rdma will recv msg from node %ld\n", ctr);


        //cout << "recv message from :" << ctr << " send_thd " << send_thd_id << endl;
		pthread_mutex_lock( tport_man.latch );
		auto recv_qpi = tport_man.recv_qps[port_id-TPORT_TWOSIDE_PORT];
		auto recv_rsi = tport_man.recv_rss[port_id-TPORT_TWOSIDE_PORT];
		pthread_mutex_unlock( tport_man.latch );
		for (RecvIter<Dummy, RDMA_ENTRY_NUM> iter(recv_qpi, recv_rsi); iter.has_msgs();
			iter.next()) {
			auto imm_msg = iter.cur_msg().value();
			char *tmp = static_cast<char *>(std::get<1>(imm_msg));
			bytes = *(int*)tmp;
			// printf("recv msg form remote %ld and thread %ld\n", ctr, send_thd_id);
			buf = (char*)malloc(bytes);
			memcpy(buf, tmp + 4, bytes);
			std::vector<Message*> * new_msgs = Message::create_messages((char*)buf);//new
			msgs->push_back(new_msgs->front());
			free(buf);
		}

		if (msgs->size() > 0) break;
	    
		
#if USE_RDMA == CHANGE_TCP_ONLY
		ctr = (ctr + g_this_rem_thread_cnt);

		if (ctr >= g_total_node_cnt) ctr = (thd_id % g_this_rem_thread_cnt) % g_total_node_cnt;
		if(ctr == g_node_id) ctr = (ctr + 1) % g_total_node_cnt;
		if (ctr == start_ctr) break;
		if(ISCLIENTN(g_node_id)) {
			port_id = get_twoside_port_id(ctr, g_node_id, (g_client_thread_cnt + g_client_rem_thread_cnt) % g_client_send_thread_cnt);
		} else {
			port_id = get_twoside_port_id(ctr, g_node_id, (g_thread_cnt + g_rem_thread_cnt) % g_send_thread_cnt);
		}
#elif USE_RDMA == CHANGE_MSG_QUEUE
		if (send_thd_id == g_thread_cnt - 1) {
			ctr = (ctr + g_this_rem_thread_cnt);

			if (ctr >= g_total_node_cnt) ctr = (thd_id % g_this_rem_thread_cnt) % g_total_node_cnt;
			if(ctr == g_node_id) ctr = (ctr + 1) % g_total_node_cnt;
			if (ctr == start_ctr) break;
			send_thd_id = 0;
		} else {
			send_thd_id += 1;
		}
		port_id = get_thd_port_id(ctr, g_node_id, send_thd_id);
#endif
	}
  // if (msgs->size() > 0) RDMA_LOG(4) << " RDMA recv node id "<< msgs->front()->return_node_id;
	
	if(bytes <= 0 ) {
		INC_STATS(thd_id,msg_recv_idle_time, get_sys_clock() - starttime);
		return msgs;
	}

    INC_STATS(thd_id,msg_recv_time, get_sys_clock() - starttime);
	INC_STATS(thd_id,msg_recv_cnt,1);

	starttime = get_sys_clock();

	//msgs = Message::create_messages((char*)buf);
	DEBUG("Batch of %d bytes recv from node %ld; Time: %f\n", bytes + 5, msgs->front()->return_node_id,
			simulation->seconds_from_start(get_sys_clock()));
	//free(buf);
	INC_STATS(thd_id,msg_unpack_time,get_sys_clock()-starttime);
	return msgs;
}
#else
// Listens to sockets for messages from other nodes
std::vector<Message*> * Transport::recv_msg(uint64_t thd_id) {
	int bytes = 0;
	void * buf;
	uint64_t starttime = get_sys_clock();
	std::vector<Message*> * msgs = NULL;
	//uint64_t ctr = starttime % recv_sockets.size();
	uint64_t rand = (starttime % recv_sockets.size()) / g_this_rem_thread_cnt;
	// uint64_t ctr = ((thd_id % g_this_rem_thread_cnt) % recv_sockets.size()) + rand *
	// g_this_rem_thread_cnt;
	uint64_t ctr = thd_id % g_this_rem_thread_cnt;
	if (ctr >= recv_sockets.size()) return msgs;
	if(g_this_rem_thread_cnt < g_total_node_cnt) {
		ctr += rand * g_this_rem_thread_cnt;
		while(ctr >= recv_sockets.size()) {
		ctr -= g_this_rem_thread_cnt;
		}
	}
	assert(ctr < recv_sockets.size());
	uint64_t start_ctr = ctr;

  	while (bytes <= 0 && (!simulation->is_setup_done() ||
						(simulation->is_setup_done() && !simulation->is_done()))) {
	Socket * socket = recv_sockets[ctr];
		bytes = socket->sock.recv(&buf, NN_MSG, NN_DONTWAIT);
	//printf("[recv buf] %s", buf);
	//sleep(1);
	//if (buf != NULL) {
	//  exit(1);
	//}
	//++ctr;
	ctr = (ctr + g_this_rem_thread_cnt);

	if (ctr >= recv_sockets.size()) ctr = (thd_id % g_this_rem_thread_cnt) % recv_sockets.size();
	if (ctr == start_ctr) break;
		if(bytes <= 0 && errno != 11) {
		  printf("Recv Error %d %s\n",errno,strerror(errno));
			nn::freemsg(buf);
		}

	if (bytes > 0) break;
	}

	if(bytes <= 0 ) {
	INC_STATS(thd_id,msg_recv_idle_time, get_sys_clock() - starttime);
	return msgs;
	}

  	INC_STATS(thd_id,msg_recv_time, get_sys_clock() - starttime);
	INC_STATS(thd_id,msg_recv_cnt,1);

	starttime = get_sys_clock();

  	msgs = Message::create_messages((char*)buf);
  	DEBUG_T("Batch of %d bytes recv from node %ld; Time: %f\n", bytes, msgs->front()->return_node_id,
		simulation->seconds_from_start(get_sys_clock()));

	nn::freemsg(buf);


	INC_STATS(thd_id,msg_unpack_time,get_sys_clock()-starttime);
  return msgs;
}
#endif
/*
void Transport::simple_send_msg(int size) {
	void * sbuf = nn_allocmsg(size,0);

	ts_t time = get_sys_clock();
	memcpy(&((char*)sbuf)[0],&time,sizeof(ts_t));

  int rc = -1;
  while(rc < 0 ) {
  if(g_node_id == 0)
	rc = socket->sock.send(&sbuf,NN_MSG,0);
  else
	rc = socket->sock.send(&sbuf,NN_MSG,0);
	}
}

uint64_t Transport::simple_recv_msg() {
	int bytes;
	void * buf;

  if(g_node_id == 0)
		bytes = socket->sock.recv(&buf, NN_MSG, NN_DONTWAIT);
  else
		bytes = socket->sock.recv(&buf, NN_MSG, NN_DONTWAIT);
	if(bytes <= 0 ) {
	  if(errno != 11)
		nn::freemsg(buf);
	  return 0;
	}

	ts_t time;
	memcpy(&time,&((char*)buf)[0],sizeof(ts_t));
	//printf("%d bytes, %f s\n",bytes,((float)(get_sys_clock()-time)) / BILLION);

	nn::freemsg(buf);
	return bytes;
}
*/
