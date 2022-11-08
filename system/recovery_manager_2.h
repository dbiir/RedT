#include "global.h"
#include "message.h"
#include "thread.h"

#ifndef _RECOVERY_MANAGER_2_H_
#define _RECOVERY_MANAGER_2_H_

// class HeartBeatThread : public Thread {
// public:
//     RC  run();
//     RC  heartbeat_loop();
//     RC  send_rdma_heart_beat(uint64_t dest_id);
//     RC  check_for_same_center();
//     RC  update_node_and_route(Message* msg);
//     void setup();
// private:
//     RouteAndStatus read_remote_status(uint64_t target_server);
// };

class RecoveryThread : public Thread {
public:
    RC run();
    // uint64_t get_new_location(uint64_t partition, uint64_t old_location, uint64_t sid);
    RC secondary_recovery(uint64_t partition_id, uint64_t sid);
    // 1、创建新副本 2、追日志 3、广播路由地址
    void setup();
    RC primary_recovery(uint64_t partition_id);
    RC send_rdma_heart_beat(uint64_t dest_id);
    RC log_update_from_replica(uint64_t partition_id, uint64_t replica_id);
private:
    bool write_remote_heartbeat(uint64_t target_server);
    uint64_t caculate_new_secondary(uint64_t partition_id, uint64_t s_id);
};
#endif