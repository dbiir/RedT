#include "global.h"
#include "helper.h"
#include "array.h"
#include "mem_alloc.h"
#include "ycsb_query.h"
#include "message.h"
#include "rdma.h"
#include "src/rdma/sop.hh"
#include "src/sshed.hh"
#include "transport.h"
#include "qps/op.hh"
#include "rdma_calvin.h"
#include "index_rdma.h"
#include "storage/row.h"
#include "storage/table.h"

#if CC_ALG == RDMA_CALVIN

void sched_queue::add_txnentry(uint64_t thd_id, Message * msg) {
	assert(msg);
	uint64_t starttime = get_sys_clock();
	while ((rear + 1) % max_msg_cnt == front && !simulation->is_done()) {
		// usleep(5);
	}
	INC_STATS(thd_id,seq_waiting_push_time,get_sys_clock() - starttime);

	msg->send_time = get_sys_clock();
	msg->copy_to_buf(msgbuf + rear*g_msg_size);

	rear = (rear + 1) % max_msg_cnt;
}

void sched_queue::add_RDONE(uint64_t thd_id) {
	uint64_t starttime = get_sys_clock();
	while ((rear + 1) % max_msg_cnt == front && !simulation->is_done()) {
		// usleep(5);
	}
	INC_STATS(thd_id,seq_waiting_push_time,get_sys_clock() - starttime);
	
	Message * msg = Message::create_message(RDONE);
	msg->send_time = get_sys_clock();
	msg->copy_to_buf(msgbuf + rear*g_msg_size);
	rear = (rear + 1) % max_msg_cnt;
}

void sched_queue::write_entry(uint64_t thd_id, Message * msg, uint64_t loc) {
	assert(msg);
	uint64_t starttime = get_sys_clock();
	if ((rear + 1) % max_msg_cnt == front && !simulation->is_done()) {
		write_rear(thd_id, loc);
	}
	while ((rear + 1) % max_msg_cnt == front && !simulation->is_done()) {
		if (simulation->is_done()) return;
		// usleep(5);
		// calvin_man.read_remote_queue(thd_id, this, loc);
		front = calvin_man.read_remote_front(thd_id,loc);
	}
	INC_STATS(thd_id,seq_waiting_push_time,get_sys_clock() - starttime);
	
	char *test_buf = Rdma::get_queue_client_memory();

	uint64_t operate_size = g_msg_size;
	msg->send_time = get_sys_clock();
	msg->copy_to_buf(test_buf);
	
	uint64_t off = (char *)(calvin_man.rdma_sched_queue[g_node_id]) - rdma_global_buffer;
	off += sizeof(sched_queue); //跳过队头
	off += rear * g_msg_size; //跳到指定位置

	auto res_s = rc_qp[loc][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_WRITE,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(test_buf),
		.remote_addr = off,
		.imm_data = 0});
	INC_STATS(thd_id, worker_oneside_cnt, 1);
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
  	auto res_p = rc_qp[loc][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
	
	rear = (rear + 1) % max_msg_cnt;
}

void sched_queue::write_rear(uint64_t thd_id, uint64_t loc) {
	uint64_t off = (char *)(calvin_man.rdma_sched_queue[g_node_id]) - rdma_global_buffer;
	uint64_t operate_size = sizeof(uint64_t);
	char *test_buf = Rdma::get_rear_client_memory();
	uint64_t *tmp_rear = (uint64_t*) test_buf;
	*tmp_rear = rear;

	auto res_s = rc_qp[loc][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_WRITE,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(test_buf),
		.remote_addr = off,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
  	auto res_p = rc_qp[loc][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
	INC_STATS(thd_id, worker_oneside_cnt, 1);
}

void RDMA_calvin::init() {
	//check memory size
	queue_size = sizeof(sched_queue) + max_msg_cnt * g_msg_size;
    uint64_t used_size = g_node_cnt * queue_size;
	assert(used_size <= rdma_calvin_buffer_size);

	//check memory ptr
	uint64_t row_size = (g_synth_table_size/g_node_cnt)*(sizeof(row_t)+1);
	char* old_ptr = (char *)(rdma_global_buffer + rdma_index_size + row_size);
	assert(old_ptr < rdma_calvin_buffer);

    for (int i = 0; i < g_node_cnt; i++) {
		rdma_sched_queue[i] = (sched_queue *)(rdma_calvin_buffer + i * queue_size);
		rdma_sched_queue[i]->front = 0;
		rdma_sched_queue[i]->rear = 0;
        rdma_sched_queue[i]->msgbuf = (char *)rdma_sched_queue[i] + sizeof(sched_queue);
    }
	sched_ptr = 0;
}

void RDMA_calvin::read_remote_queue(uint64_t thd_id, sched_queue * temp_queue, uint64_t loc) {
	assert(loc != g_node_id);
    uint64_t off = (char *)(calvin_man.rdma_sched_queue[g_node_id]) - rdma_global_buffer;
	uint64_t operate_size = sizeof(sched_queue);
	char *test_buf = Rdma::get_queue_client_memory();

    auto res_s = rc_qp[loc][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_READ,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(test_buf),
		.remote_addr = off,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
  	auto res_p = rc_qp[loc][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
	INC_STATS(thd_id, worker_oneside_cnt, 1);
	memcpy(temp_queue, test_buf, operate_size);
}

uint64_t RDMA_calvin::read_remote_front(uint64_t thd_id, uint64_t loc) {
	assert(loc != g_node_id);
	uint64_t off = (char *)(calvin_man.rdma_sched_queue[g_node_id]) - rdma_global_buffer;
	off += sizeof(uint64_t);
	uint64_t operate_size = sizeof(uint64_t);

	uint64_t * test_buf = (uint64_t *)Rdma::get_queue_client_memory();
	auto res_s = rc_qp[loc][thd_id]->send_normal(
		{.op = IBV_WR_RDMA_READ,
		.flags = IBV_SEND_SIGNALED,
		.len = operate_size,
		.wr_id = 0},
		{.local_addr = reinterpret_cast<rdmaio::RMem::raw_ptr_t>(test_buf),
		.remote_addr = off,
		.imm_data = 0});
	RDMA_ASSERT(res_s == rdmaio::IOCode::Ok);
  	auto res_p = rc_qp[loc][thd_id]->wait_one_comp();
	RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
	INC_STATS(thd_id, worker_oneside_cnt, 1);
    return *test_buf;
}

Message * RDMA_calvin::sched_dequeue(uint64_t thd_id) {
	uint64_t starttime = get_sys_clock();

	assert(CC_ALG == RDMA_CALVIN);
	Message * msg = NULL;
	sched_queue * temp_queue = rdma_sched_queue[sched_ptr];
	uint64_t front = temp_queue->front;
	uint64_t rear = temp_queue->rear;
	bool valid = front == rear ? false : true;
	
	if(valid) {
		assert(front != rear);
		msg = Message::create_message(temp_queue->msgbuf + front*g_msg_size);
		uint64_t queue_time = get_sys_clock() - msg->send_time;
		INC_STATS(thd_id,sched_queue_wait_time,queue_time);
		INC_STATS(thd_id,sched_queue_cnt,1);

		assert(msg->rtype == RDONE || msg->rtype == CL_QRY);
		
		if(msg->rtype == RDONE) {
			DEBUG("[dequeue]loc:%lu, qid:%lu, front:%lu, rear:%lu RDNOE work_epoch:%ld\n"
				,g_node_id,sched_ptr,front,rear,simulation->get_worker_epoch());
			if(sched_ptr == g_node_cnt - 1) {
				INC_STATS(thd_id,sched_epoch_cnt,1);
				INC_STATS(thd_id,sched_epoch_diff,get_sys_clock()-simulation->last_worker_epoch_time);
				simulation->next_worker_epoch();
			}

			front = (front + 1) % max_msg_cnt;
			temp_queue->front = front;
			sched_ptr = (sched_ptr + 1) % g_node_cnt;
			return NULL;
		} else {
			if(sched_ptr != g_node_id)
				msg->return_node_id = sched_ptr;
			
			DEBUG("[dequeue]loc:%lu, qid:%lu, front:%lu, rear:%lu txn_id:%lu, batch_id:%lu, return_node_id:%lu\n"
				,g_node_id,sched_ptr,front,rear,msg->txn_id,msg->batch_id,msg->return_node_id);
			front = (front + 1) % max_msg_cnt;
			temp_queue->front = front;
		}
		INC_STATS(thd_id,sched_queue_dequeue_time,get_sys_clock() - starttime);
	}
	
	return msg;
}

#endif
