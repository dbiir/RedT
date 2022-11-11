
#include "global.h"
#include "helper.h"
#include "manager.h"
#include "thread.h"
#include "recovery_manager_2.h"
#include "recovery_manager.h"
#include "query.h"
#include "mem_alloc.h"
#include "transport.h"
#include "math.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "message.h"
#include "work_queue.h"
#include "route_table.h"
#include "rdma.h"

void RecoveryThread::setup() {
}

RC RecoveryThread::run() {
    tsetup();
    printf("Running RecoveryThread %ld\n",_thd_id);

    while(!simulation->is_done()) {
        Message* msg;
        msg = recover_queue.dequeue(get_thd_id());
        if(!msg) {
            // sleep(100L);
            continue;
        }
        ReplicaRecoverMessage * rmsg = (ReplicaRecoverMessage*) msg;
        DEBUG_H("Node receive recover msg from %ld, recover part %ld:%ld\n",  rmsg->return_node_id, rmsg->partition_id, rmsg->replica_id);
        assert(rmsg->rtype == RECOVERY);
        if(rmsg->replica_id == 1 || rmsg->replica_id == 2) {
            secondary_recovery(rmsg->partition_id, rmsg->replica_id);
        } else if(rmsg->replica_id == 0) {
            primary_recovery(rmsg->partition_id);
        }

        delete msg;
    }

    return FINISH;
}

RC RecoveryThread::secondary_recovery(uint64_t partition_id, uint64_t sid) {
    route_node_ts primary = route_table.get_primary(partition_id);
    uint64_t primary_id = primary.node_id;
    DEBUG_H("Node recover part %ld secondary\n", partition_id);
    RC rc = log_update_from_replica(partition_id, primary_id);
    if(rc == RCOK) {
        if(sid == 1) {
            route_table.table[partition_id].secondary_1.node_id = g_node_id;
            route_table.table[partition_id].secondary_1.last_ts = get_wall_clock();
        } else { 
            route_table.table[partition_id].secondary_2.node_id = g_node_id;
            route_table.table[partition_id].secondary_2.last_ts = get_wall_clock();
        }
        if(is_center_primary(g_node_id)) {
            send_tcp_heart_beat();
        } else {
            uint64_t center_id = GET_CENTER_ID(g_node_id);
            uint64_t dest_id = get_center_primary(center_id);
            send_rdma_heart_beat(dest_id);
        }
        return rc;
    } else {
        return Abort;
    }
}

RC RecoveryThread::primary_recovery(uint64_t partition_id) {
    DEBUG_H("Node recover part %ld primary\n", partition_id);
    route_node_ts primary = route_table.get_primary(partition_id);
    uint64_t primary_id = primary.node_id;
    uint64_t sid1 = route_table.get_secondary_1(partition_id).node_id;
    uint64_t sid2 = route_table.get_secondary_2(partition_id).node_id;
    uint64_t new_sid;
    RC rc;
    if(sid1 == g_node_id) {
        rc = log_update_from_replica(partition_id, sid2);
        route_table.table[partition_id].secondary_1.node_id = -1;
        route_table.table[partition_id].secondary_1.last_ts = get_wall_clock();
        new_sid = 1;
    } else if (sid2 == g_node_id) {
        rc = log_update_from_replica(partition_id, sid1);
        route_table.table[partition_id].secondary_2.node_id = -1;
        route_table.table[partition_id].secondary_2.last_ts = get_wall_clock();
        new_sid = 2;
    }
    if(rc == RCOK) {
        route_table.table[partition_id].primary.node_id = g_node_id;
        route_table.table[partition_id].primary.last_ts = get_wall_clock();
        if(is_center_primary(g_node_id)) {
            send_tcp_heart_beat();
        } else {
            uint64_t center_id = GET_CENTER_ID(g_node_id);
            uint64_t dest_id = get_center_primary(center_id);
            send_rdma_heart_beat(dest_id);
        }
        uint64_t new_node_id = caculate_new_secondary(partition_id, new_sid);
        DEBUG_T("Node %ld send recover msg to %ld after primary recover\n", g_node_id, new_node_id);
        msg_queue.enqueue(get_thd_id(), 
            Message::create_message(partition_id, new_sid, node_status, RECOVERY),
            new_node_id); 
        return rc;
    } else {
        return Abort;
    }
}

RC RecoveryThread::send_rdma_heart_beat(uint64_t dest_id) {
    write_remote_heartbeat(dest_id);
    return RCOK;
}

uint64_t RecoveryThread::caculate_new_secondary(uint64_t partition_id, uint64_t s_id) {
    route_node_ts primary = route_table.get_primary(partition_id);
    uint64_t primary_id = primary.node_id;
    uint64_t sid1 = route_table.get_secondary_1(partition_id).node_id;
    uint64_t sid2 = route_table.get_secondary_2(partition_id).node_id;
    uint64_t p_center_id = GET_CENTER_ID(primary_id);
    uint64_t sid1_center_id;
    uint64_t sid2_center_id;
    uint64_t suitable_node = -1;
    if(s_id == 1) {
        sid2_center_id = GET_CENTER_ID(sid2);
        for(int i = 0; i < g_center_cnt; i++) {
            uint64_t center_id = (g_center_id + i) % g_center_cnt;
            if(center_id != p_center_id && center_id != sid2_center_id) {
                for(int j = 0; j < g_node_cnt/g_center_cnt; j++) {
                    uint64_t node_id = center_id + j * g_center_cnt;
                    if(node_status.get_node_status(node_id)->status == OnCall) {
                        suitable_node = node_id;
                        break;
                    }
                }
            }
            if(suitable_node != -1) break;
        }
    } else {
        sid1_center_id = GET_CENTER_ID(sid1);
        for(int i = 0; i < g_center_cnt; i++) {
            uint64_t center_id = (g_center_id + i) % g_center_cnt;
            if(center_id != p_center_id && center_id != sid1_center_id) {
                for(int j = 0; j < g_node_cnt/g_center_cnt; j++) {
                    uint64_t node_id = center_id + j * g_center_cnt;
                    if(node_status.get_node_status(node_id)->status == OnCall) {
                        suitable_node = node_id;
                        break;
                    }
                }
            }
            if(suitable_node != -1) break;
        }
    }
    if (suitable_node != -1) return suitable_node;
    else assert(false);
}
RC RecoveryThread::log_update_from_replica(uint64_t partition_id, uint64_t replica_id) {
    return RCOK;
}