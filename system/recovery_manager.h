#include "global.h"
#include "message.h"
#include "thread.h"

#ifndef _RECOVERY_MANAGER_H_
#define _RECOVERY_MANAGER_H_

class HeartBeatThread : public Thread {
public:
    RC  run();
    RC  heartbeat_loop();
    RC  send_rdma_heart_beat(uint64_t dest_id);
    RC  send_tcp_heart_beat();
    RC  check_for_same_center();
    RC  check_for_other_center();
    bool  is_global_primary(uint64_t nid);
    RC  update_node_and_route(RouteAndStatus result, uint64_t origin_dest);
    void setup();
    vector<Replica> get_node_replica(uint64_t dest_id);
    RC generate_recovery_msg(uint64_t dest_id);
protected:
    bool write_remote_heartbeat(uint64_t target_server);
private:
    RouteAndStatus read_remote_status(uint64_t target_server);
    uint64_t caculate_suitable_node(Replica rep, uint64_t failed_id);
};

#endif