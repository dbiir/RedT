#include "global.h"
#include "config.h"
#include "rdma.h"
#include <stdio.h>
//#include "rdma_ctrl.hpp"
#include "lib.hh"
#include <assert.h>
#include <string>
#include "src/allocator_master.hh"

char *Rdma::rdma_buffer; //= new char[RDMA_BUFFER_SIZE];
char ** Rdma::ifaddr = new char *[g_total_node_cnt];

uint64_t Rdma::get_socket_count() {
  uint64_t sock_cnt = 0;
  if(ISCLIENT)
    sock_cnt = (g_total_node_cnt)*2 + g_client_send_thread_cnt * g_servers_per_client;
  else
    sock_cnt = (g_total_node_cnt)*2 + g_client_send_thread_cnt;
  return sock_cnt;
}

void Rdma::read_ifconfig(const char * ifaddr_file) {
  // ifaddr = new char *[g_total_node_cnt];
	uint64_t cnt = 0;
    //从ifconfig.txt中读取IP信息
    printf("Reading ifconfig file: %s\n",ifaddr_file);
	ifstream fin(ifaddr_file);
	string line;
  while (getline(fin, line)) {
		//memcpy(ifaddr[cnt],&line[0],12);
        //初始化
		ifaddr[cnt] = new char[line.length()+1];
        //赋值
        strcpy(ifaddr[cnt],&line[0]);
        //输出显示
		printf("%ld: %s\n",cnt,ifaddr[cnt]);
		cnt++;
	}
  assert(cnt == g_total_node_cnt);
}

string Rdma::get_path() {
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

uint64_t Rdma::get_port_id(uint64_t src_node_id, uint64_t dest_node_id, uint64_t send_thread_id) {
  uint64_t port_id = 0;
  DEBUG("Calc port id %ld %ld %ld\n",src_node_id,dest_node_id,send_thread_id);
 // printf("Calc port id %ld %ld %ld\n",src_node_id,dest_node_id,send_thread_id);
  port_id += g_total_node_cnt * dest_node_id;
  DEBUG("%ld\n",port_id);
  port_id += src_node_id;
  DEBUG("%ld\n",port_id);
  port_id += send_thread_id * g_total_node_cnt * g_total_node_cnt;
  DEBUG("%ld\n",port_id);
  port_id += 30000;
  port_id = port_id + 1;
  DEBUG("%ld\n",port_id);

  printf("Port ID:  %ld, %ld -> %ld : %ld\n",send_thread_id,src_node_id,dest_node_id,port_id);
  return port_id;
}

uint64_t get_rm_id(uint64_t node_id,uint64_t thread_id){
  uint64_t rm_id = 0;
  rm_id = node_id * 10 + thread_id;
  return rm_id;
}

uint64_t Rdma::get_port(uint64_t node_id){
  uint64_t port_id = 0;
  port_id = 7344 + node_id;
  return port_id ;  
}

void * Rdma::client_qp(void *arg){

  printf("\n====client====");

  rdmaParameter *arg_tmp;
  arg_tmp = (rdmaParameter*)arg;
  uint64_t node_id = arg_tmp->node_id;
  uint64_t thread_num = arg_tmp->thread_num; 

  printf("\n node_id = %d \n",node_id);

  ConnectManager cm_(std::string(rdma_server_add[node_id]));

  printf("address = %s\n",rdma_server_add[node_id].c_str());
  
  if (cm_.wait_ready(1000000, 8) == IOCode::Timeout) RDMA_ASSERT(false) << "cm connect to server timeout";

  uint64_t reg_nic_name = node_id;
  uint64_t reg_mem_name = node_id;

  auto fetch_res = cm_.fetch_remote_mr(reg_mem_name);
  RDMA_ASSERT(fetch_res == IOCode::Ok) << std::get<0>(fetch_res.desc);
  rmem::RegAttr remote_attr = std::get<1>(fetch_res.desc);

  for(int thread_id = 0;thread_id < THREAD_CNT ; thread_id ++){
    rc_qp[node_id][thread_id] = RDMARC::create(nic, QPConfig()).value();
   // auto rcqp = RDMARC::create(nic, QPConfig()).value();
    ///auto qp_res = cm_.cc_rc("client-qp", rc_qp[node_id][thread_id], reg_nic_name, QPConfig());

    qp_name[node_id][thread_id] = "client-qp" + std::to_string(thread_id);
    //auto qp_res = cm_.cc_rc("client-qp", rcqp, reg_nic_name, QPConfig());
    auto qp_res = cm_.cc_rc(qp_name[node_id][thread_id], rc_qp[node_id][thread_id], reg_nic_name, QPConfig());
    RDMA_ASSERT(qp_res == IOCode::Ok) << std::get<0>(qp_res.desc);
    auto key = std::get<1>(qp_res.desc);
    RDMA_LOG(4) << "client fetch QP authentical key: " << key;


    //local_mr.push_back(local_mr_);

    rc_qp[node_id][thread_id]->bind_remote_mr(remote_attr);
    rc_qp[node_id][thread_id]->bind_local_mr(client_rm_handler->get_reg_attr().value());

  }
  cm.push_back(cm_);

  printf("server %d QP connect to server %d\n",g_node_id,node_id);
  
  return NULL;
}

void * Rdma::server_qp(void *){
  printf("\n====server====\n");
 // printf("port = %d\n",port);
  printf("rdma_server_port[g_node_id] = %d\n",rdma_server_port[g_node_id]);
  rm_ctrl = Arc<RCtrl>(new rdmaio::RCtrl(rdma_server_port[g_node_id]));
  //rm_ctrl = Arc<RCtrl>(new rdmaio::RCtrl(port));
  //  rm_ctrl(rdma_server_port);

  uint64_t reg_nic_name = g_node_id;
  uint64_t reg_mem_name = g_node_id;

  RDMA_ASSERT(rm_ctrl->opened_nics.reg(reg_nic_name, nic));

  RDMA_ASSERT(rm_ctrl->registered_mrs.create_then_reg(
        reg_mem_name, rdma_rm,
        rm_ctrl->opened_nics.query(reg_nic_name).value()));

  rdma_global_buffer = (char*)(rm_ctrl->registered_mrs.query(reg_mem_name)
                             .value()
                             ->get_reg_attr()
                             .value()
                             .buf);
  
  rm_ctrl->start_daemon();   

  //printf("server %d connect to server %d QP\n",g_node_id,node_id);
  
  return NULL;
}


void Rdma::init(){

  _sock_cnt = get_socket_count();

  printf("rdma Init %d: %ld\n",g_node_id,_sock_cnt);

  string path = get_path();
  read_ifconfig(path.c_str());
	
  nic =  RNic::create(RNicInfo::query_dev_names().at(0)).value();

  // rdma_global_buffer = (char *)malloc(rdma_buffer_size);
  // memset(rdma_global_buffer,0,rdma_buffer_size);
  // rdma_buffer = rdma_global_buffer;

 //as server
  rdma_rm = Arc<RMem>(new RMem(rdma_buffer_size));
  rm_handler = RegHandler::create(rdma_rm, nic).value();

  //as client
  client_rdma_rm = Arc<RMem>(new RMem(1024));
  client_rm_handler = RegHandler::create(client_rdma_rm, nic).value();

  uint64_t thread_num = 0;
  uint64_t node_id = 0;
  pthread_t *client_thread = new pthread_t[g_total_node_cnt * g_thread_cnt];
 // pthread_t *server_thread = new pthread_t[g_total_node_cnt * g_thread_cnt];
  pthread_t server_thread;
  printf("g_total_node_cnt = %d",g_total_node_cnt);

  //pthread_create(&server_thread,NULL,server_qp,NULL);
  // if(g_node_id == 0){port = 7344;server_port = 7345;}
  //  else{port = 7345;server_port = 7344;} 
  for(int i = 0;i < NODE_CNT ; i++){
    rdma_server_port[i] = get_port(i);
  }

  server_qp(NULL);
  printf("start wait\n");
  sleep(5);

  for(node_id = 0; node_id < g_total_node_cnt; node_id++) {
      
      if(node_id == g_node_id || ISCLIENTN(node_id))continue;                        //对自身节点之外的每个节点
       
        rdma_server_add[node_id] = ifaddr[node_id] + std::string(":") + std::to_string(rdma_server_port[node_id]);
        //rdma_server_add[node_id] = ifaddr[node_id] + std::string(":") + std::to_string(server_port);

        rdmaParameter *arg = (rdmaParameter*)malloc(sizeof(rdmaParameter));
        arg->node_id = node_id;
        arg->thread_num = thread_num;
       
        pthread_create(&client_thread[thread_num],NULL,client_qp,(void *)arg);
       
       thread_num ++;
  }

  
 // pthread_join(server_thread,NULL);

  for(int i = 0;i<thread_num;i++){
    pthread_join(client_thread[i],NULL);
  }

  //char* rheader = (char *)rdma_rm->raw_ptr + rdma_index_size;
  char* rheader = rdma_global_buffer + rdma_index_size;
  r2::AllocatorMaster<>::init(rheader,rdma_buffer_size-rdma_index_size);

}
