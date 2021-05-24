#include <string>

#include "global.h"
#include "lib.hh"
#include "nn.hpp"
//#include "rdma_ctrl.hpp"

// using namespace rdmaio;
using namespace rdmaio;
using namespace rdmaio::rmem;
using namespace rdmaio::qp;
using namespace std;

class Rdma {
 public:
  uint64_t get_socket_count();
  string get_path();
  void read_ifconfig(const char *ifaddr_file);
  static uint64_t get_port_id(uint64_t src_node_id, uint64_t dest_node_id, uint64_t send_thread_id);
  static uint64_t get_port(uint64_t node_id);
  static void *client_qp(void *);
  static void *server_qp(void *);
  void *create();
  void *connect();
  void init();
  static char *get_index_client_memory(uint64_t thd_id, int num = 1);
  static char *get_row_client_memory(uint64_t thd_id, int num = 1);
  //static char *get_table_client_memory(uint64_t thd_id);




 private:
  uint64_t _sock_cnt;
  uint64_t rr;
  static char *rdma_buffer;
  static char **ifaddr;
  char *buffer;
  char **addr;
};

typedef struct {
  uint64_t node_id;
  uint64_t thread_num;
} rdmaParameter;
