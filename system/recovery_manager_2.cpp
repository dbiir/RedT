
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
            sleep(100L);
            continue;
        }
        ReplicaRecoverMessage * rmsg = (ReplicaRecoverMessage*) msg;
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

    RC rc = log_update_from_replica(partition_id, primary_id);
    if(rc == RCOK) {
        if(sid == 1) {
            route_table.table[partition_id].secondary_1.node_id = g_node_id;
            route_table.table[partition_id].secondary_1.last_ts = get_sys_clock();
        } else { 
            route_table.table[partition_id].secondary_2.node_id = g_node_id;
            route_table.table[partition_id].secondary_2.last_ts = get_sys_clock();
        }
        if(is_center_primary(g_node_id)) {
            for (int i = 0; i < g_center_cnt; i++) {
                uint64_t dest_id = get_center_primary(i);
                msg_queue.enqueue(get_thd_id(), 
                    Message::create_message(route_table.table, node_status.table, HEART_BEAT),
                    dest_id);
            }
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
    
    route_node_ts primary = route_table.get_primary(partition_id);
    uint64_t primary_id = primary.node_id;
    uint64_t sid1 = route_table.get_secondary_1(partition_id).node_id;
    uint64_t sid2 = route_table.get_secondary_2(partition_id).node_id;
    uint64_t new_sid;
    RC rc;
    if(sid1 == g_node_id) {
        rc = log_update_from_replica(partition_id, sid2);
        route_table.table[partition_id].secondary_1.node_id = -1;
        route_table.table[partition_id].secondary_1.last_ts = get_sys_clock();
        new_sid = 1;
    } else if (sid2 == g_node_id) {
        rc = log_update_from_replica(partition_id, sid1);
        route_table.table[partition_id].secondary_2.node_id = -1;
        route_table.table[partition_id].secondary_2.last_ts = get_sys_clock();
        new_sid = 2;
    }
    if(rc == RCOK) {
        route_table.table[partition_id].primary.node_id = g_node_id;
        route_table.table[partition_id].primary.last_ts = get_sys_clock();
        if(is_center_primary(g_node_id)) {
            for (int i = 0; i < g_center_cnt; i++) {
                uint64_t dest_id = get_center_primary(i);
                msg_queue.enqueue(get_thd_id(), 
                    Message::create_message(route_table.table, node_status.table, HEART_BEAT),
                    dest_id);
            }
        } else {
            uint64_t center_id = GET_CENTER_ID(g_node_id);
            uint64_t dest_id = get_center_primary(center_id);
            send_rdma_heart_beat(dest_id);
        }
        uint64_t new_node_id = caculate_new_secondary(partition_id, new_sid);
        DEBUG_T("116 Node %ld send recover msg to %ld\n", g_node_id, new_node_id);
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

bool RecoveryThread::write_remote_heartbeat(uint64_t target_server){
    uint64_t thd_id = get_thd_id();

    char *local_buf = Rdma::get_status_client_memory(thd_id);
    
    uint64_t time = get_sys_clock();
    uint64_t operate_size = sizeof(uint64_t);
    ::memset(local_buf, 0, operate_size);
    memcpy(local_buf, &time , operate_size);
    uint64_t remote_offset = rdma_buffer_size - rdma_log_size - rdma_routetable_size;
    remote_offset += SIZE_OF_ROUTE + sizeof(status_node) * g_node_id; 

    auto res_s = rc_qp[target_server][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_WRITE,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(local_buf),
		.remote_addr = remote_offset,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
	if (res_p != rdmaio::IOCode::Ok) {
        //todo: handle error.
        node_status.set_node_status(target_server, NS::Failure);
        return false;
    }

    return true;
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
                    if(node_status.get_node_status(node_id).status == OnCall) {
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
                    if(node_status.get_node_status(node_id).status == OnCall) {
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