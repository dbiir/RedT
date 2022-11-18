
#include "global.h"
#include "helper.h"
#include "manager.h"
#include "thread.h"
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

void HeartBeatThread::setup() {
}

RC HeartBeatThread::run() {
	tsetup();
	printf("Running HeartBeatThread %ld\n",_thd_id);
    heartbeat_loop();
	return FINISH;
}


RC HeartBeatThread::heartbeat_loop() {

	myrand rdm;
	rdm.init(get_thd_id());
	RC rc = RCOK;
	assert (rc == RCOK);
	uint64_t starttime, lastsendtime = 0;
    uint64_t now;
	Message* msg;
	while (!simulation->is_done()) {
        // Send heartbeat
        now = get_wall_clock();
        node_status.set_node_status(g_node_id,OnCall,get_thd_id());
        if (now - lastsendtime > HEARTBEAT_TIME ) {
            if (is_center_primary(g_node_id)) {
                // DEBUG_H("Node %ld is center %d primary\n", g_node_id, GET_CENTER_ID(g_node_id));
                send_tcp_heart_beat();
            } else {
                uint64_t center_id = GET_CENTER_ID(g_node_id);
                uint64_t dest_id = get_center_primary(center_id);
                send_rdma_heart_beat(dest_id);
            }   
            lastsendtime = get_wall_clock();
        }
        if (!is_center_primary(g_node_id)) continue;

        // Main RM check the status of other nodes in the same data center
        // Now, main RM can generate the failure recovery methods.
        // First, handle the failure in the same data center.
        check_for_same_center();

        // Main RM check the status of other nodes in the different data center
		msg = heartbeat_queue.dequeue(get_thd_id());
		if (msg == NULL) continue;
        HeartBeatMessage* hmsg = (HeartBeatMessage*)msg;
		update_node_and_route(hmsg->heartbeatmsg, hmsg->return_node_id);
        
        // Second, handle the failuer in the different data center.
        if (!is_global_primary(g_node_id)) continue;
        // DEBUG_H("Node %ld is global primary\n", g_node_id);
        check_for_other_center();
		delete msg;
	}
	// printf("FINISH %ld:%ld\n",_node_id,_thd_id);
	// fflush(stdout);
	return FINISH;
}

RC HeartBeatThread::send_rdma_heart_beat(uint64_t dest_id) {
    write_remote_heartbeat(dest_id);
    RouteAndStatus result = read_remote_status(dest_id);
    update_node_and_route(result, dest_id);
    return RCOK;
}

RC HeartBeatThread::send_tcp_heart_beat() {
    for (int i = 0; i < g_center_cnt; i++) {
        uint64_t dest_id = get_center_primary(i);
        if (dest_id == g_node_id) continue;
        if (dest_id == -1) continue;
        msg_queue.enqueue(get_thd_id(), 
            Message::create_message(route_table.table, node_status.table, HEART_BEAT),
            dest_id);
        DEBUG_H("Node %ld send TCP heartbeat to %ld\n", g_node_id, dest_id);
    }
    return RCOK;
}

RouteAndStatus HeartBeatThread::read_remote_status(uint64_t target_server){
    uint64_t operate_size = SIZE_OF_ROUTE + SIZE_OF_STATUS;
    // sizeof(route_table_node) + sizeof(status_node);
    uint64_t thd_id = get_thd_id();
	uint64_t remote_offset = rdma_buffer_size - rdma_log_size - rdma_routetable_size;
    char *local_buf = Rdma::get_status_client_memory(thd_id);

	uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();
    auto res_s = rc_qp[target_server][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_READ,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(local_buf),
		.remote_addr = remote_offset,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp(RDMA_CALLS_TIMEOUT);
	// RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
    if (res_p != rdmaio::IOCode::Ok) {
        //todo: handle error.
        node_status.set_node_status(target_server, NS::Failure, get_thd_id());
        DEBUG_T("Thd %ld send RDMA one-sided failed--read RDMA heartbeat %ld.\n", get_thd_id(),target_server);
        DEBUG_H("Center primary node %ld failed, because read RDMA heartbeat failed, result %d\n", target_server, res_p.code);
    }
    DEBUG_H("HEARTBEAT read primary node status\n");
	endtime = get_sys_clock();

	status_node* temp_status = (status_node *)mem_allocator.alloc(SIZE_OF_STATUS);
	route_table_node* temp_route = (route_table_node *)mem_allocator.alloc(SIZE_OF_ROUTE);
	memcpy(temp_route, local_buf, SIZE_OF_ROUTE);
	memcpy(temp_status, local_buf + SIZE_OF_ROUTE, SIZE_OF_STATUS);

	RouteAndStatus result;
	result._status = temp_status;
	result._route = temp_route;
    return result;
}

bool HeartBeatThread::write_remote_heartbeat(uint64_t target_server){
    uint64_t thd_id = get_thd_id();

    char *local_buf = Rdma::get_status_client_memory(thd_id);
    
    uint64_t time = get_wall_clock();
    uint64_t operate_size = sizeof(uint64_t);
    memset(local_buf, 0, operate_size);
    memcpy(local_buf, &time , operate_size);
    uint64_t remote_offset = rdma_buffer_size - rdma_log_size - rdma_routetable_size;
    remote_offset += SIZE_OF_ROUTE + sizeof(status_node) * g_node_id + sizeof(NS); 

    auto res_s = rc_qp[target_server][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_WRITE,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(local_buf),
		.remote_addr = remote_offset,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
    DEBUG_H("Node %ld issue rdma heartbeat to %ld\n", g_node_id, target_server);
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp(RDMA_CALLS_TIMEOUT);
	if (res_p != rdmaio::IOCode::Ok) {
        //todo: handle error.
        node_status.set_node_status(target_server, NS::Failure, get_thd_id());
        DEBUG_T("Thd %ld send RDMA one-sided failed--write RDMA heartbeat %ld.\n", get_thd_id(), target_server);
        DEBUG_H("Center primary node %ld failed, because write RDMA heartbeat failed, result %d\n", target_server, res_p);
        return false;
    }
    DEBUG_H("Node %ld issue rdma heartbeat to %ld success\n", g_node_id, target_server);
    return true;
}

RC HeartBeatThread::check_for_same_center() {
    uint64_t node_cnt_per_center = g_node_cnt / g_center_cnt;
    uint64_t center_id = GET_CENTER_ID(g_node_id);
    for (int i = 0; i < node_cnt_per_center; i++) {
        uint64_t dest_id = center_id + i * g_center_cnt;
        status_node* st = node_status.get_node_status(dest_id);
        bool is_alive = st->status == OnCall;
        if (dest_id == g_node_id) {
            assert(is_alive);
            continue;
        }

        uint64_t time = get_sys_clock();        
        if (time > st->last_ts && time - st->last_ts > SAME_CENTER_FAILED_TIME ) {
            DEBUG_H("Node %ld find in same data center node %ld status Failure due to timeout %lu : %lu\n", g_node_id, dest_id, time, st->last_ts);
            // update the status of failed node.
            node_status.set_node_status(dest_id, NS::Failure, get_thd_id());
            is_alive = false;
        }
        if (!is_alive) {
            // DEBUG_H("Node %ld find in same data center node %ld status Failure %d\n", g_node_id, dest_id, st->status);
            // recover the replica on failed node.
            generate_recovery_msg(dest_id);
        }
    }
}

RC HeartBeatThread::check_for_other_center() {
    uint64_t node_cnt_per_center = g_node_cnt / g_center_cnt;
    for (int j = 0; j < g_center_cnt; j++){
        uint64_t dest_id = get_center_primary(j);
        status_node* st = node_status.get_node_status(dest_id);
        bool is_alive = st->status == NS::OnCall;
        if (dest_id == g_node_id) {
            assert(is_alive);
            continue;
        } 
        // Check whether other data center is alive.
        if (dest_id != -1) {
            uint64_t time = get_wall_clock();
            // get_wall_clock();
            if (time > st->last_ts && time - st->last_ts > INTER_CENTER_FAILED_TIME) {
                is_alive = false;
            }
            DEBUG_H("Node primary check node %lu time %lu %lu\n", dest_id, time, st->last_ts);
        } else {
            is_alive = false;
            DEBUG_H("Node primary check no node alive in data center %d\n", j);
        }
        // if dead
        if (!is_alive) {
            // update the status of failed node.
            for (int i = 0; i < node_cnt_per_center; i++) {
                uint64_t dest_id2 = j + i * g_center_cnt;
                if (dest_id2 == g_node_id) continue;
                uint64_t time = get_wall_clock();
                status_node* st = node_status.get_node_status(dest_id2);
                DEBUG_H("Node %ld check node %ld in a failed data center\n", g_node_id, dest_id2);
                if (time > st->last_ts && time - st->last_ts > INTER_CENTER_FAILED_TIME ) {
                    DEBUG_H("Node %ld find in other data center node %ld status Failure\n", g_node_id, dest_id);
                    // update the status of failed node.
                    node_status.set_node_status(dest_id2, NS::Failure, get_thd_id());                   
                }
                if (st->status == NS::Failure) generate_recovery_msg(dest_id2);
            }
        }
    }
}

bool HeartBeatThread::is_global_primary(uint64_t nid) {
    uint64_t node_cnt_per_center = g_node_cnt / g_center_cnt;
    for (int i = 0; i < g_node_cnt; i++) {
        uint64_t node_id = i;
        status_node* st = node_status.get_node_status(node_id);
        // Skip the failed data node.
        if (st->status == NS::Failure) continue;
        // We assume the first active node is global primary
        // Now we check whether this node alive.
        if (node_id == nid) {
            DEBUG_H("Node %ld now is the primary\n", g_node_id);
            return true;
        }
        uint64_t time = get_wall_clock();
        if (time > st->last_ts && time - st->last_ts > INTER_CENTER_FAILED_TIME){
            DEBUG_H("Node %ld find global primary node %ld status Failure, now %lu last heartbeat time %lu\n", g_node_id, node_id, time, st->last_ts);
            // The origin global primary RM failed.
            node_status.set_node_status(node_id, NS::Failure, get_thd_id());
        } else {
            return false;
        }
    }
    return false;
}

RC HeartBeatThread::update_node_and_route(RouteAndStatus result, uint64_t origin_dest) {
    // node status
    DEBUG_H("Node %ld recieve heart beat from %ld in other data center\n", g_node_id, origin_dest);
    for (int i = 0; i < g_node_cnt; i++) {
        status_node* st = node_status.get_node_status(i);
        status_node tmp_st = result._status[i];
        DEBUG_H("Node %ld old ts %lu received ts %lu\n", i, st->last_ts, tmp_st.last_ts);
        if (tmp_st.last_ts > st->last_ts) {
            st->last_ts = tmp_st.last_ts;
            st->status = tmp_st.status;

            if ((i == g_node_id && st->status == Failure)||
                (i == origin_dest && st->status == Failure)) {
                assert(false);
            }
            DEBUG_H("Node %ld update ts %lu and status %s\n", i, st->last_ts, st->status == OnCall ? "OnCall" : "Failure");
        }
    }
    // route
    for (int i = 0; i < g_part_cnt; i++) {
        // primary
        route_node_ts p_rt = route_table.get_primary(i);
        route_node_ts tmp_p_rt = result._route[i].primary;
        if (tmp_p_rt.last_ts > p_rt.last_ts) {
            route_table.set_primary(i, tmp_p_rt.node_id, tmp_p_rt.last_ts);
            DEBUG_H("Route %ld update primary ts %lu and node %d\n", i, tmp_p_rt.last_ts, tmp_p_rt.node_id);
        }
        // secondary 1
        route_node_ts s1_rt = route_table.get_secondary_1(i);
        route_node_ts tmp_s1_rt = result._route[i].secondary_1;
        if (tmp_s1_rt.last_ts > s1_rt.last_ts) {
            route_table.set_secondary_1(i, tmp_s1_rt.node_id, tmp_s1_rt.last_ts);
            DEBUG_H("Route %ld update second1 ts %lu and node %d\n", i, tmp_s1_rt.last_ts, tmp_s1_rt.node_id);
        }
        // secondary 2
        route_node_ts s2_rt = route_table.get_secondary_2(i);
        route_node_ts tmp_s2_rt = result._route[i].secondary_2;
        if (tmp_s2_rt.last_ts > s2_rt.last_ts) {
            route_table.set_secondary_2(i, tmp_s2_rt.node_id, tmp_s2_rt.last_ts);
            DEBUG_H("Route %ld update second2 ts %lu and node %d\n", i, tmp_s2_rt.last_ts, tmp_s2_rt.node_id);
        }
    }
}

vector<Replica> HeartBeatThread::get_node_replica(uint64_t dest_id) {
    vector<Replica> replica_list;
    for (int i = 0; i < g_part_cnt; i++) {
        route_node_ts p_rt = route_table.get_primary(i);
        if (p_rt.node_id == dest_id) {
            replica_list.push_back(Replica(i, 0));
            DEBUG_H("Failed node %ld has primary replica %ld\n", dest_id, i);
        }
        // secondary 1
        route_node_ts s1_rt = route_table.get_secondary_1(i);
        if (s1_rt.node_id == dest_id) {
            replica_list.push_back(Replica(i, 1));
            DEBUG_H("Failed node %ld has second1 replica %ld\n", dest_id, i);
        }
        // secondary 2
        route_node_ts s2_rt = route_table.get_secondary_2(i);
        if (s2_rt.node_id == dest_id) {
            replica_list.push_back(Replica(i, 2));
            DEBUG_H("Failed node %ld has second2 replica %ld\n", dest_id, i);
        }
    }
    return replica_list;
}

uint64_t HeartBeatThread::caculate_suitable_node(Replica rep, uint64_t failed_id) {
    /* ------Check the same data center of failed node------- */
    uint64_t center_id = GET_CENTER_ID(failed_id);
    assert(g_node_cnt % CENTER_CNT == 0);
    uint64_t node_cnt_per_dc = g_node_cnt / CENTER_CNT;
    uint64_t suitable_node = -1;
    /* --Now we simply choose another node in the same data center.-- */
    for (int i = 0; i < node_cnt_per_dc; i++) {
        uint64_t node_id = i * CENTER_CNT + center_id;
        status_node* st = node_status.get_node_status(node_id);
        if (st->status == NS::Failure) continue;
        DEBUG_H("Generate recover msg, node %ld state %ld\n", node_id, st->status);
        suitable_node = node_id;
        break;
    }
    if (suitable_node != -1) return suitable_node;

    /* ------If all the node in the same data center fails ------- */
    for (int i = 0; i < g_node_cnt; i++) {
        uint64_t node_id = i;
        status_node* st = node_status.get_node_status(node_id);
        if (st->status == NS::Failure) continue;
        suitable_node = node_id;
        break;
    }
    if (suitable_node != -1) return suitable_node;
    else assert(false);
}

RC HeartBeatThread::generate_recovery_msg(uint64_t failed_id) {
    vector<Replica> replica_list = get_node_replica(failed_id);
    for (int i = 0; i < replica_list.size(); i ++) {
        Replica rep = replica_list[i];
        uint64_t new_dest_id = caculate_suitable_node(rep, failed_id);
        if (rep.replica_id == 0) {
            route_table.table[rep.partition_id].primary.node_id = -1;
            route_table.table[rep.partition_id].primary.last_ts = get_wall_clock();
        }
        if (rep.replica_id == 1) {
            route_table.table[rep.partition_id].secondary_1.node_id = -1;
            route_table.table[rep.partition_id].secondary_1.last_ts = get_wall_clock();
        }
        if (rep.replica_id == 2) {
            route_table.table[rep.partition_id].secondary_2.node_id = -1;
            route_table.table[rep.partition_id].secondary_2.last_ts = get_wall_clock();
        }
        
        DEBUG_H("Node %ld send recover msg to %ld, failed id %ld, failed part %ld, failed replica %ld\n", g_node_id, new_dest_id, failed_id, rep.replica_id, rep.replica_id);
        if (new_dest_id == g_node_id) {
            recover_queue.enqueue(get_thd_id(), 
                    Message::create_message(rep.partition_id, rep.replica_id, node_status, RECOVERY),
                    new_dest_id);
        }else {
            msg_queue.enqueue(get_thd_id(), 
                    Message::create_message(rep.partition_id, rep.replica_id, node_status, RECOVERY),
                    new_dest_id);
        }
    }
}