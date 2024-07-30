#include "route_table.h"

#include "global.h"
#include "manager.h"

#include "dbpa.hpp"
#include "routine.h"
#include "lib.hh"
#include "qps/op.hh"
#include "transport/rdma.h"
#include "src/rdma/sop.hh"

#include "mem_alloc.h"

void RouteTable::init() {
  uint64_t table_size = (PART_CNT) * sizeof(route_table_node);
  assert(table_size < rdma_routetable_size);
  table = (route_table_node*)rdma_routetable_buffer;

  for (int i = 0; i < PART_CNT; i++) {
    table[i].partition_id = i;
    table[i].replica_cnt = REPLICA_COUNT;
    // for (int j = 0; j < REPLICA_COUNT; j++) {
    //   table[i].new_secondary[j].node_id = (i / 2 + j + 1) % g_node_cnt;
    //   table[i].new_secondary[j].last_ts = get_wall_clock();
    //   table[i].new_secondary[j].watermark = 0;
    // }
    for (int j = 0; j < REPLICA_COUNT; j++) {
      table[i].new_secondary[j].node_id = (i + j) % g_node_cnt;
      table[i].new_secondary[j].last_ts = get_wall_clock();
      table[i].new_secondary[j].watermark = 0;
    }

    // for 5 replica
    // for (int j = 0; j < REPLICA_COUNT; j++) {
    //   if (j <= 3) {
    //     table[i].new_secondary[j].node_id = ((i / 4) * 2 + j) % g_node_cnt;
    //     table[i].new_secondary[j].last_ts = get_wall_clock();
    //   } else {
    //     table[i].new_secondary[j].node_id = ((i / 4) * 2 + 4 + (i % 2)) % g_node_cnt;
    //     table[i].new_secondary[j].last_ts = get_wall_clock();
    //   }
    // }
    // for 3 replica
    // for (int j = 0; j < REPLICA_COUNT; j++) {
    //   if (j <= 1) {
    //     table[i].new_secondary[j].node_id = (i / 4) * 2 + j;
    //     table[i].new_secondary[j].last_ts = get_wall_clock();
    //   } else {
    //     table[i].new_secondary[j].node_id = (i + 2) % g_node_cnt;
    //     table[i].new_secondary[j].last_ts = get_wall_clock();
    //   }
    // }
  }
}

auto RouteTable::get_route_node_new(int index, uint64_t partition_id) -> route_node_ts {
  return table[partition_id].new_secondary[index];
}

void RouteTable::set_route_node_new(int index, uint64_t partition_id, uint64_t node_id,
                                    uint64_t timestamp, uint64_t thd_id) {
  table[partition_id].new_secondary[index].node_id = node_id;
  if (timestamp == 0)
    table[partition_id].new_secondary[index].last_ts = get_wall_clock();
  else
    table[partition_id].new_secondary[index].last_ts = timestamp;
}


void RouteTable::set_route_node_watermark_new(uint64_t partition_id, uint64_t node_id,
                                    uint64_t watermark, uint64_t thd_id) {
  uint64_t index = 0;
  for (uint64_t i = 0; i < table[partition_id].replica_cnt; i++) {
    if (GET_CENTER_ID(table[partition_id].new_secondary[i].node_id) == GET_CENTER_ID(node_id)) {
      index = i;
      break;
    } 
    #if DEBUG_PRINTF
      printf("part %ld id %ld in node %ld target %ld\n", partition_id, i, table[partition_id].new_secondary[i].node_id,node_id);
    #endif
  }
  if (table[partition_id].new_secondary[index].watermark < watermark) {
    table[partition_id].new_secondary[index].watermark = watermark;
    #if DEBUG_PRINTF
      printf("part %ld id %ld's watermark is set to %lu, it has %ld replica\n", partition_id, index, watermark,table[partition_id].replica_cnt);
    #endif
  }
    
}


route_table_node* RouteTable::read_remote_route_node(yield_func_t &yield, uint64_t target_server, uint64_t partition_id, uint64_t thd_id,uint64_t cor_id) {
  uint64_t operate_size = sizeof(route_table_node);
  uint64_t remote_offset = rdma_buffer_size - rdma_log_size - rdma_routetable_size;
  remote_offset += sizeof(route_table_node) * partition_id;
  char* local_buf = Rdma::get_route_node_client_memory(thd_id);

  uint64_t starttime;
  uint64_t endtime;
  starttime = get_sys_clock();
  auto res_s = rc_qp[target_server][thd_id]->send_normal(
      {.op = IBV_WR_RDMA_READ, .flags = IBV_SEND_SIGNALED, .len = operate_size, .wr_id = 0},
      {.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(local_buf),
       .remote_addr = remote_offset,
       .imm_data = 0});
  RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);

  #if USE_COROUTINE
	// h_thd->un_res_p.push(std::make_pair(target_server, thd_id));
	uint64_t waitcomp_time;
	std::pair<int,ibv_wc> res_p;
	INC_STATS(thd_id, worker_process_time, get_sys_clock() - h_thd->cor_process_starttime[cor_id]);
	
	do {
		h_thd->start_wait_time = get_sys_clock();
		h_thd->last_yield_time = get_sys_clock();
		// printf("do\n");
		yield(h_thd->_routines[((cor_id) % COROUTINE_CNT) + 1]);
		uint64_t yield_endtime = get_sys_clock();
		INC_STATS(thd_id, worker_yield_cnt, 1);
		INC_STATS(thd_id, worker_yield_time, yield_endtime - h_thd->last_yield_time);
		INC_STATS(thd_id, worker_idle_time, yield_endtime - h_thd->last_yield_time);
		res_p = rc_qp[target_server][thd_id]->poll_send_comp();
		waitcomp_time = get_sys_clock();
		
		INC_STATS(thd_id, worker_idle_time, waitcomp_time - yield_endtime);
		INC_STATS(thd_id, worker_waitcomp_time, waitcomp_time - yield_endtime);
	} while (res_p.first == 0);
	h_thd->cor_process_starttime[cor_id] = get_sys_clock();
  #else
  auto res_p = rc_qp[target_server][thd_id]->wait_one_comp(RDMA_CALLS_TIMEOUT);
  // RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
  if (res_p != rdmaio::IOCode::Ok) {
    // todo: handle error.
    node_status.set_node_status(target_server, NS::Failure, thd_id);
    DEBUG_T("Thd %ld send RDMA one-sided failed--read remote route node %ld.\n", thd_id,
            target_server);
  }
  endtime = get_sys_clock();
  #endif
  route_table_node* temp_node = (route_table_node*)mem_allocator.alloc(sizeof(route_table_node));
  memcpy(temp_node, local_buf, sizeof(route_table_node));

  return temp_node;

}

RC RouteTable::cas_remote_route_node_watermark(yield_func_t &yield, uint64_t target_server,uint64_t partition_id,uint64_t index,uint64_t old_value,uint64_t new_value, uint64_t *try_lock, uint64_t thrd_id, uint64_t cor_id){
    
  rdmaio::qp::Op<> op;
  // 计算初始route table位置
  uint64_t remote_offset = rdma_buffer_size - rdma_log_size - rdma_routetable_size;
  // 计算对应part的route node位置
  remote_offset += sizeof(route_table_node) * partition_id;
  // 计算node里watermark的位置
  remote_offset += sizeof(uint64_t) + // partition_id
                  //  sizeof(route_node_ts) * 3 + 
                   sizeof(route_node_ts) * index + 
                   sizeof(uint64_t) * 2;

  uint64_t thd_id = thrd_id + cor_id * g_thread_cnt;
  uint64_t *local_buf = (uint64_t *)Rdma::get_row_client_memory(thd_id);
  auto mr = client_rm_handler->get_reg_attr().value();
  
  uint64_t starttime;
	uint64_t endtime;
	starttime = get_sys_clock();

  op.set_atomic_rbuf((uint64_t*)(remote_mr_attr[target_server].buf + remote_offset), remote_mr_attr[target_server].key).set_cas(old_value, new_value);
  assert(op.set_payload(local_buf, sizeof(uint64_t), mr.key) == true);
  auto res_s2 = op.execute(rc_qp[target_server][thd_id], IBV_SEND_SIGNALED);

  RDMA_ASSERT(res_s2 == IOCode::Ok);
	INC_STATS(thrd_id, worker_oneside_cnt, 1);
#if USE_COROUTINE
	// h_thd->un_res_p.push(std::make_pair(target_server, thd_id));
		
	uint64_t waitcomp_time;
	std::pair<int,ibv_wc> res_p;
	INC_STATS(thrd_id, worker_process_time, get_sys_clock() - h_thd->cor_process_starttime[cor_id]);
	do {
		h_thd->start_wait_time = get_sys_clock();
		h_thd->last_yield_time = get_sys_clock();
		// printf("do\n");
		yield(h_thd->_routines[((cor_id) % COROUTINE_CNT) + 1]);
		uint64_t yield_endtime = get_sys_clock();
		INC_STATS(thrd_id, worker_yield_cnt, 1);
		INC_STATS(thrd_id, worker_yield_time, yield_endtime - h_thd->last_yield_time);
		INC_STATS(thrd_id, worker_idle_time, yield_endtime - h_thd->last_yield_time);
		res_p = rc_qp[target_server][thd_id]->poll_send_comp();
		waitcomp_time = get_sys_clock();
		
		INC_STATS(thrd_id, worker_idle_time, waitcomp_time - yield_endtime);
		INC_STATS(thrd_id, worker_waitcomp_time, waitcomp_time - yield_endtime);
	} while (res_p.first == 0);
	h_thd->cor_process_starttime[cor_id] = get_sys_clock();

#else
	auto res_p = rc_qp[target_server][thd_id]->wait_one_comp(RDMA_CALLS_TIMEOUT);
  endtime = get_sys_clock();
	INC_STATS(thrd_id, worker_idle_time, endtime-starttime);
	DEL_STATS(thrd_id, worker_process_time, endtime-starttime);
	INC_STATS(thrd_id, worker_waitcomp_time, endtime-starttime);
	// assert(res_p == rdmaio::IOCode::Ok);
	if (res_p != rdmaio::IOCode::Ok) {
		node_status.set_node_status(target_server, NS::Failure, thrd_id);
		DEBUG_T("Thd %ld send RDMA one-sided failed--cas %ld.\n", thrd_id,target_server);
		return NODE_FAILED;
	}
#endif
	*try_lock = *local_buf;
  return RCOK;
}


void RouteTable::set_remote_route_node_watermark(yield_func_t &yield, uint64_t partition_id, uint64_t node_id,
                                    uint64_t watermark, uint64_t thd_id,uint64_t cor_id) {
  route_table_node* tmp_node = read_remote_route_node(yield, node_id, partition_id, thd_id, cor_id);

  uint64_t index = 0;
  for (uint64_t i = 0; i < tmp_node->replica_cnt; i++) {
    if (GET_CENTER_ID(tmp_node->new_secondary[i].node_id) == GET_CENTER_ID(node_id)) {
    // if (tmp_node->new_secondary[index].node_id == node_id) {
      index = i;
      break;
    } 
  }
  uint64_t orig_watermark = tmp_node->new_secondary[index].watermark;
  uint64_t remote_value = 0;
  RC rc;
  while (orig_watermark < watermark) {
    rc = cas_remote_route_node_watermark(yield, node_id, partition_id, index, orig_watermark, watermark, &remote_value, thd_id, cor_id);
    if (orig_watermark == remote_value) break; //此时修改成功
    orig_watermark = remote_value; //否则继续尝试
  }
  #if DEBUG_PRINTF
    printf("part %ld id %ld's remote watermark is set to %lu from %lu\n", partition_id, index, watermark, orig_watermark);
  #endif
}

void NodeStatus::init() {
  uint64_t node_table_size = (NODE_CNT) * sizeof(status_node);
  table = (status_node*)(rdma_routetable_buffer + (PART_CNT) * sizeof(route_table_node));
  for (int i = 0; i < NODE_CNT; i++) {
    table[i].status = OnCall;
    table[i].last_ts = get_wall_clock();
    DEBUG_H("Node Status init node %d ts %lu state %s\n", i, table[i].last_ts,
            table[i].status == OnCall ? "OnCall" : "Failure");
  }
}

status_node* NodeStatus::get_node_status(uint64_t node_id) { return &table[node_id]; }

void NodeStatus::set_node_status(uint64_t node_id, NS newStatus, uint64_t thd_id) {
  table[node_id].status = newStatus;
  table[node_id].last_ts = get_wall_clock();
}