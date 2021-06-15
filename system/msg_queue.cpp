/*
   Copyright 2016 Massachusetts Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include "msg_queue.h"
#include "mem_alloc.h"
#include "query.h"
#include "pool.h"
#include "message.h"
#include <boost/lockfree/queue.hpp>

void MessageQueue::init() {
  //m_queue = new boost::lockfree::queue<msg_entry* > (0);
  m_queue = new boost::lockfree::queue<msg_entry* > * [g_this_send_thread_cnt];
#if NETWORK_DELAY_TEST
  cl_m_queue = new boost::lockfree::queue<msg_entry* > * [g_this_send_thread_cnt];
#endif
  for(uint64_t i = 0; i < g_this_send_thread_cnt; i++) {
    m_queue[i] = new boost::lockfree::queue<msg_entry* > (0);
#if NETWORK_DELAY_TEST
    cl_m_queue[i] = new boost::lockfree::queue<msg_entry* > (0);
#endif
  }
  ctr = new  uint64_t * [g_this_send_thread_cnt];
  for(uint64_t i = 0; i < g_this_send_thread_cnt; i++) {
    ctr[i] = (uint64_t*) mem_allocator.align_alloc(sizeof(uint64_t));
    *ctr[i] = i % g_thread_cnt;
  }
  msg_queue_size=0;
  sem_init(&_semaphore, 0, 1);
  for (uint64_t i = 0; i < g_this_send_thread_cnt; i++) sthd_m_cache.push_back(NULL);
}

void MessageQueue::statqueue(uint64_t thd_id, msg_entry * entry) {
  Message *msg = entry->msg;
  if (msg->rtype == CL_QRY || msg->rtype == CL_QRY_O || msg->rtype == RTXN_CONT ||
      msg->rtype == RQRY_RSP || msg->rtype == RACK_PREP  ||
      msg->rtype == RACK_FIN || msg->rtype == RTXN) {
    // these msg will send back to local node
    uint64_t queue_time = get_sys_clock() - entry->starttime;
		INC_STATS(thd_id,trans_msg_remote_wait,queue_time);
  } else if (msg->rtype == RQRY || msg->rtype == RQRY_CONT ||
             msg->rtype == RFIN || msg->rtype == RPREPARE ||
             msg->rtype == RFWD){
    // these msg will send to remote node
    uint64_t queue_time = get_sys_clock() - entry->starttime;
		INC_STATS(thd_id,trans_msg_local_wait,queue_time);
  } else if (msg->rtype == CL_RSP) {
    uint64_t queue_time = get_sys_clock() - entry->starttime;
		INC_STATS(thd_id,trans_return_client_wait,queue_time);
  }
}

void MessageQueue::enqueue(uint64_t thd_id, Message * msg,uint64_t dest) {
  // if(thd_id > 3)
  // printf("MQ Enqueue thread id: %ld\n", thd_id);
  //if(thd_id > 3) printf("MQ Enqueue thread id: %ld\n", thd_id);
  DEBUG("MQ Enqueue %ld\n",dest)
  assert(dest < g_total_node_cnt);
#if ONE_NODE_RECIEVE == 1 && defined(NO_REMOTE) && LESS_DIS_NUM == 10
#else
  assert(dest != g_node_id);
#endif
  DEBUG_M("MessageQueue::enqueue msg_entry alloc\n");
  msg_entry * entry = (msg_entry*) mem_allocator.alloc(sizeof(struct msg_entry));
  //msg_pool.get(entry);
  entry->msg = msg;
  entry->dest = dest;
  entry->starttime = get_sys_clock();
  assert(entry->dest < g_total_node_cnt);
  uint64_t mtx_time_start = get_sys_clock();
#if CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN
  // Need to have strict message ordering for sequencer thread
  uint64_t rand = thd_id % g_this_send_thread_cnt;
#elif WORKLOAD == DA
  uint64_t rand = 0;
#else
  uint64_t rand = mtx_time_start % g_this_send_thread_cnt;
#endif
#if NETWORK_DELAY_TEST
  if(ISCLIENTN(dest)) {
    while (!cl_m_queue[rand]->push(entry) && !simulation->is_done()) {
    }
    return;
  }
#endif
  while (!m_queue[rand]->push(entry) && !simulation->is_done()) {
  }
  INC_STATS(thd_id,mtx[3],get_sys_clock() - mtx_time_start);
  INC_STATS(thd_id,msg_queue_enq_cnt,1);
  sem_wait(&_semaphore);
  msg_queue_size++;
  sem_post(&_semaphore);
  INC_STATS(thd_id,trans_msg_queue_item_total,msg_queue_size);

}

uint64_t MessageQueue::dequeue(uint64_t thd_id, Message *& msg) {
  msg_entry * entry = NULL;
  uint64_t dest = UINT64_MAX;
  uint64_t mtx_time_start = get_sys_clock();
  bool valid = false;
#if NETWORK_DELAY_TEST
  valid = cl_m_queue[thd_id%g_this_send_thread_cnt]->pop(entry);
  if(!valid) {
    entry = sthd_m_cache[thd_id % g_this_send_thread_cnt];
    if(entry)
      valid = true;
    else
      valid = m_queue[thd_id%g_this_send_thread_cnt]->pop(entry);
  }
#elif WORKLOAD == DA
  valid = m_queue[0]->pop(entry);
#else
  //uint64_t ctr_id = thd_id % g_this_send_thread_cnt;
  //uint64_t start_ctr = *ctr[ctr_id];
  valid = m_queue[thd_id%g_this_send_thread_cnt]->pop(entry);
#endif
  /*
  while(!valid && !simulation->is_done()) {
    ++(*ctr[ctr_id]);
    if(*ctr[ctr_id] >= g_this_thread_cnt)
      *ctr[ctr_id] = 0;
    valid = m_queue[*ctr[ctr_id]]->pop(entry);
    if(*ctr[ctr_id] == start_ctr)
      break;
  }
  */
  INC_STATS(thd_id,mtx[4],get_sys_clock() - mtx_time_start);
  uint64_t curr_time = get_sys_clock();
  if(valid) {
    assert(entry);
#if NETWORK_DELAY_TEST
    if(!ISCLIENTN(entry->dest)) {
      if(ISSERVER && (get_sys_clock() - entry->starttime) < g_network_delay) {
        sthd_m_cache[thd_id%g_this_send_thread_cnt] = entry;
        INC_STATS(thd_id,mtx[5],get_sys_clock() - curr_time);
        return UINT64_MAX;
      } else {
        sthd_m_cache[thd_id%g_this_send_thread_cnt] = NULL;
      }
      if(ISSERVER) {
        INC_STATS(thd_id,mtx[38],1);
        INC_STATS(thd_id,mtx[39],curr_time - entry->starttime);
      }
    }

#endif
    dest = entry->dest;
    assert(dest < g_total_node_cnt);
    msg = entry->msg;
    DEBUG("MQ Dequeue %ld\n",dest)
    statqueue(thd_id, entry);
    INC_STATS(thd_id,msg_queue_delay_time,curr_time - entry->starttime);
    INC_STATS(thd_id,msg_queue_cnt,1);
    msg->mq_time = curr_time - entry->starttime;
    //msg_pool.put(entry);
    DEBUG_M("MessageQueue::enqueue msg_entry free\n");
    mem_allocator.free(entry,sizeof(struct msg_entry));
    sem_wait(&_semaphore);
    msg_queue_size--;
    sem_post(&_semaphore);
  } else {
    msg = NULL;
    dest = UINT64_MAX;
  }
  INC_STATS(thd_id,mtx[5],get_sys_clock() - curr_time);
  return dest;
}
