#include "rdma.h"

#include <stdio.h>
#include "global.h"
#include "rdma_ctrl.hpp"
#include <assert.h>
#include <string.h>

using namespace rdmaio;

char *Rdma::rdma_buffer = new char[RDMA_BUFFER_SIZE];
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
//  uint64_t max_send_thread_cnt = g_send_thread_cnt > g_client_send_thread_cnt ? g_send_thread_cnt : g_client_send_thread_cnt;
//  port_id *= max_send_thread_cnt;
  port_id += send_thread_id * g_total_node_cnt * g_total_node_cnt;
  DEBUG("%ld\n",port_id);
  port_id += 10000;
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

void * Rdma::create_qp(void *){

  printf("\n====create====");
  for(uint64_t node_id = g_node_id; node_id < g_total_node_cnt; node_id++) {
    if(node_id == g_node_id || ISCLIENTN(node_id))
     //如果是自身或客户节点
      continue;
    //对每个线程
     for(uint64_t thread_id = 0;thread_id < THREAD_CNT;thread_id++){
          //计算本机用到的端口
          uint64_t port_id = get_port_id(g_node_id,node_id,thread_id);
                                    //uint64_t src_node_id, uint64_t dest_node_id, uint64_t send_thread_id
       
//这里是建立了一个QP等待其他server连接过来
         //创建
          RdmaCtrl *c = new RdmaCtrl(g_node_id,port_id);
         //分配网卡
          RdmaCtrl::DevIdx idx {.dev_id = 0,.port_id = 1 }; 
          c->open_thread_local_device(idx);

         //注册内存
         Rdma::rdma_buffer = (char *)malloc(RDMA_BUFFER_SIZE);
         rdma_buffer = new char[RDMA_BUFFER_SIZE];
         memset(rdma_buffer, 0, RDMA_BUFFER_SIZE);
         
         //注册内存
         uint64_t rm_id = get_rm_id(node_id,thread_id);
         RDMA_ASSERT(c->register_memory(rm_id,rdma_buffer,4096,c->get_device()) == true);
         //����ע���ڴ���Ϣ����QP
         MemoryAttr local_mr = c->get_local_mr(rm_id);
         RCQP *qp = c->create_rc_qp(create_rc_idx(1,0),c->get_device(), &local_mr);
         //连接
         uint64_t client_port = get_port_id(node_id,g_node_id,thread_id);//计算连接端的端口
         while(qp->connect(ifaddr[node_id],port_id, create_rc_idx(1,0)) != SUCC){
            usleep(2000);
         }
        printf("server %d QP wait for server %d\n",g_node_id,node_id);
     }
  }
  
  return NULL;
}

void * Rdma::connect_qp(void *){
    printf("\n====connect====\n");
    uint64_t node_id = g_node_id;
 // for(uint64_t node_id = g_node_id; node_id > 0; node_id--) {
   while(node_id >= 0){
   //   printf("\nnode id = %d\n",node_id);
      
      if(node_id == -1){
        break;
      }
      if(node_id == g_node_id || ISCLIENTN(node_id)){
          node_id--;
          continue;
      }
     
     for(uint64_t thread_id = 0;thread_id < THREAD_CNT;thread_id++){
        printf("\nget port\n");
        uint64_t port_id = get_port_id(g_node_id,node_id,thread_id);
                                        //uint64_t src_node_id, uint64_t dest_node_id, uint64_t send_thread_id

          RdmaCtrl *c = new RdmaCtrl(node_id,port_id);
          RdmaCtrl::DevIdx idx {.dev_id = 0,.port_id = 1 }; 
          c->open_thread_local_device(idx);

        
          //rdma_buffer = (char *)malloc(RDMA_BUFFER_SIZE);
          //rdma_buffer = new char[RDMA_BUFFER_SIZE];
          memset(rdma_buffer, 0, RDMA_BUFFER_SIZE);
         
          uint64_t rm_id = get_rm_id(node_id,thread_id);
          RDMA_ASSERT(c->register_memory(rm_id,rdma_buffer,4096,c->get_device()) == true);

          MemoryAttr remote_mr;
          uint64_t remote_rm_id = get_rm_id(g_node_id,thread_id);
          //计算另一端使用的端口
          int server_port = get_port_id(node_id,g_node_id,thread_id);

          while(QP::get_remote_mr(ifaddr[node_id],server_port,remote_rm_id,&remote_mr) != SUCC) {
            usleep(2000);
          }

        MemoryAttr local_mr = c->get_local_mr(node_id);
        RCQP *qp = c->create_rc_qp(create_rc_idx(1,0),c->get_device(), &local_mr);
        qp->bind_remote_mr(remote_mr); // bind to the previous allocated mr

        while(qp->connect(ifaddr[node_id],server_port) != SUCC)  {
            usleep(2000);
        }
      
        printf("server %d connect to server %d QP\n",g_node_id,node_id);
     }

     node_id--;

  }
  
  return NULL;
}


void Rdma::init(){
  _sock_cnt = get_socket_count();

  rr = 0;
  printf("rdma Init %d: %ld\n",g_node_id,_sock_cnt);

  vector<std::thread> th_bind;
  vector<std::thread> th_connect;

  string path = get_path();
  read_ifconfig(path.c_str());
  //将IP地址存到ifaddr数组中

  pthread_t create_qp_thread,connect_qp_thread;
  pthread_create(&create_qp_thread,NULL,create_qp,NULL);
  pthread_create(&connect_qp_thread,NULL,connect_qp,NULL);

  pthread_join(create_qp_thread, NULL);
  pthread_join(connect_qp_thread, NULL);  

/*创建线程一*/
   // ret=pthread_create(&id_1,NULL,(void  *) thread_1,NULL);

  //建两个线程分别用来创建QP等待连接和连接QP
  // pthread_t *p_thds = (pthread_t *)malloc(sizeof(pthread_t) * 2);
  // worker_num_thds[0].init(0,g_node_id,m_wl);
  // pthread_create(&p_thds[id++], &attr, run_thread, (void *)&worker_num_thds[0]);

  // pthread_create(&id_1,NULL,(void  *) thread_1,NULL);
  // for (uint64_t i = 0; i < all_thd_cnt; i++) pthread_join(p_thds[i], NULL);

 

   

}
