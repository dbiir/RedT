#include "nn.hpp"
#include <string>
#include<string.h>

using namespace std;

class Rdma{
   public:
    uint64_t get_socket_count();
    string get_path();
    void read_ifconfig(const char * ifaddr_file);
    static uint64_t get_port_id(uint64_t src_node_id, uint64_t dest_node_id, uint64_t send_thread_id);
    static void * create_qp(void *);
    static void * connect_qp(void *);
    void * create();
    void * connect();
    void init();
   private:
    uint64_t _sock_cnt;
    uint64_t rr;
    static char *rdma_buffer;
    static char ** ifaddr;
    char *buffer;
    char ** addr;
};
