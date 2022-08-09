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

#include "msg_thread.h"
#include "msg_queue.h"
#include "message.h"
#include "mem_alloc.h"
#include "transport.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "pool.h"
#include "global.h"
#include "work_queue.h"

void MessageThread::init(uint64_t thd_id) {
  buffer_cnt = g_total_node_cnt;
#if CC_ALG == CALVIN
  buffer_cnt++;
#endif
  DEBUG_M("MessageThread::init buffer[] alloc\n");
  buffer = (mbuf **) mem_allocator.align_alloc(sizeof(mbuf*) * buffer_cnt);
  for(uint64_t n = 0; n < buffer_cnt; n++) {
    DEBUG_M("MessageThread::init mbuf alloc\n");
    buffer[n] = (mbuf *)mem_allocator.align_alloc(sizeof(mbuf));
    buffer[n]->init(n);
    buffer[n]->reset(n);
  }
  _thd_id = thd_id;
}

void MessageThread::check_and_send_batches() {
  uint64_t starttime = get_sys_clock();
  for(uint64_t dest_node_id = 0; dest_node_id < buffer_cnt; dest_node_id++) {
#if SEND_TO_SELF_PAHSE == 0
    if (dest_node_id == g_node_id) continue;
#endif
    if(buffer[dest_node_id]->ready()) {
      send_batch(dest_node_id);
    }
  }
  INC_STATS(_thd_id,mtx[11],get_sys_clock() - starttime);
}

void MessageThread::send_batch(uint64_t dest_node_id) {
  uint64_t starttime = get_sys_clock();
    mbuf * sbuf = buffer[dest_node_id];
    assert(sbuf->cnt > 0);
	  ((uint32_t*)sbuf->buffer)[2] = sbuf->cnt;
    INC_STATS(_thd_id,mbuf_send_intv_time,get_sys_clock() - sbuf->starttime);

    DEBUG("Send batch of %ld msgs to %ld\n",sbuf->cnt,dest_node_id);
    fflush(stdout);
    sbuf->set_send_time(get_sys_clock());
    tport_man.send_msg(_thd_id,dest_node_id,sbuf->buffer,sbuf->ptr);

    INC_STATS(_thd_id,msg_batch_size_msgs,sbuf->cnt);
    INC_STATS(_thd_id,msg_batch_size_bytes,sbuf->ptr);
    if(ISSERVERN(dest_node_id)) {
      INC_STATS(_thd_id,msg_batch_size_bytes_to_server,sbuf->ptr);
    } else if (ISCLIENTN(dest_node_id)){
      INC_STATS(_thd_id,msg_batch_size_bytes_to_client,sbuf->ptr);
    }
    INC_STATS(_thd_id,msg_batch_cnt,1);
    sbuf->reset(dest_node_id);
  INC_STATS(_thd_id,mtx[12],get_sys_clock() - starttime);
}
char type2char1(DATxnType txn_type)
{
  switch (txn_type)
  {
    case DA_READ:
      return 'R';
    case DA_WRITE:
      return 'W';
    case DA_COMMIT:
      return 'C';
    case DA_ABORT:
      return 'A';
    case DA_SCAN:
      return 'S';
    default:
      return 'U';
  }
}

static uint64_t mget_size() {
  uint64_t size = 0;
  size += sizeof(RemReqType);
  size += sizeof(uint64_t);
#if CC_ALG == CALVIN
  size += sizeof(uint64_t);
#endif
  // for stats, send message queue time
  size += sizeof(uint64_t);

  // for stats, latency
  size += sizeof(uint64_t) * 7;
  return size;
}

uint64_t cget_size(QueryMessage * msg) {
  uint64_t size = mget_size();
#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC || CC_ALG == WOOKONG || CC_ALG == DTA || CC_ALG == CICADA || CC_ALG == WOUND_WAIT
  size += sizeof(msg->ts);
#endif
#if CC_ALG == OCC || CC_ALG == FOCC || CC_ALG == BOCC || CC_ALG == SSI || CC_ALG == WSI || \
    CC_ALG == DLI_BASE || CC_ALG == DLI_OCC || CC_ALG == DLI_MVCC_OCC || \
    CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3 || CC_ALG == DLI_MVCC
  size += sizeof(msg->start_ts);
#endif
  return size;
}
#if 0
void fake_copy_to_buf(Message * msg, char* buf) {
  // !mcopy_to_buf
  
  uint64_t ptr = 0;
  COPY_BUF(buf,msg->rtype,ptr);
  COPY_BUF(buf,msg->txn_id,ptr);
#if CC_ALG == CALVIN
  COPY_BUF(buf,msg->batch_id,ptr);
#endif
  COPY_BUF(buf,msg->mq_time,ptr);

  COPY_BUF(buf,msg->lat_work_queue_time,ptr);
  COPY_BUF(buf,msg->lat_msg_queue_time,ptr);
  COPY_BUF(buf,msg->lat_cc_block_time,ptr);
  COPY_BUF(buf,msg->lat_cc_time,ptr);
  COPY_BUF(buf,msg->lat_process_time,ptr);
  if ((CC_ALG == CALVIN && (msg->rtype == CL_QRY||msg->rtype == CL_QRY_O) && msg->txn_id % g_node_cnt == g_node_id) ||
      (CC_ALG != CALVIN && IS_LOCAL(msg->txn_id))) {
    msg->lat_network_time = get_sys_clock();
  } else {
    msg->lat_other_time = get_sys_clock() - msg->lat_other_time;
  }
  //printf("mtobuf %ld: %f, %f\n",txn_id,lat_network_time,lat_other_time);
  COPY_BUF(buf,msg->lat_network_time,ptr);
  COPY_BUF(buf,msg->lat_other_time,ptr);
  if (msg->rtype == RQRY) {
    // !copy_to_buf
    ptr = mget_size();
    QueryMessage* cmsg = (QueryMessage*) msg;
    #if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC || CC_ALG == WOOKONG || CC_ALG == DTA
    COPY_BUF(buf,cmsg->ts,ptr);
      assert(ts != 0);
    #endif
    #if CC_ALG == OCC || CC_ALG == FOCC || CC_ALG == BOCC || CC_ALG == SSI || CC_ALG == WSI || \
        CC_ALG == DLI_BASE || CC_ALG == DLI_OCC || CC_ALG == DLI_MVCC_OCC || \
        CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3 || CC_ALG == DLI_MVCC
    COPY_BUF(buf,cmsg->start_ts,ptr);
    #endif
    // !YCSBClientQueryMessage:copy_to_buf
    ptr = cget_size(cmsg);
    YCSBQueryMessage* ymsg = (YCSBQueryMessage*) msg;
    size_t size = ymsg->requests.size();
    COPY_BUF(buf,size,ptr);
    for(uint64_t i = 0; i < ymsg->requests.size(); i++) {
      ycsb_request * req = ymsg->requests[i];
      assert(req->key < g_synth_table_size);
      COPY_BUF(buf,*req,ptr);
    }
    assert(ptr == ymsg->get_size());
  } else if (msg->rtype == RQRY_RSP) {
    ptr = mget_size();
    QueryResponseMessage* qmsg = (QueryResponseMessage*)msg;
    COPY_BUF(buf,qmsg->rc,ptr);
    #if CC_ALG == TICTOC
      COPY_BUF(buf,_min_commit_ts,ptr);
    #endif
    assert(ptr == qmsg->get_size());
  }
  
}

Message* fake_create_message(char * buf) {
  RemReqType rtype = NO_MSG;
  uint64_t ptr = 0;
  COPY_VAL(rtype,buf,ptr);
  Message * msg = Message::create_message(rtype);
  
  // !mcopy_from_buf(txn);
  ptr = 0;
  COPY_VAL(msg->rtype,buf,ptr);
  COPY_VAL(msg->txn_id,buf,ptr);
#if CC_ALG == CALVIN
  COPY_VAL(msg->batch_id,buf,ptr);
#endif
  COPY_VAL(msg->mq_time,buf,ptr);

  COPY_VAL(msg->lat_work_queue_time,buf,ptr);
  COPY_VAL(msg->lat_msg_queue_time,buf,ptr);
  COPY_VAL(msg->lat_cc_block_time,buf,ptr);
  COPY_VAL(msg->lat_cc_time,buf,ptr);
  COPY_VAL(msg->lat_process_time,buf,ptr);
  COPY_VAL(msg->lat_network_time,buf,ptr);
  COPY_VAL(msg->lat_other_time,buf,ptr);
  if ((CC_ALG == CALVIN && rtype == CALVIN_ACK && msg->txn_id % g_node_cnt == g_node_id) ||
      (CC_ALG != CALVIN && IS_LOCAL(msg->txn_id))) {
    msg->lat_network_time = (get_sys_clock() - msg->lat_network_time) - msg->lat_other_time;
  } else {
    msg->lat_other_time = get_sys_clock();
  }
  if (msg->rtype == RQRY) {
    // !QueryMessage::copy_from_buf(txn);
    ptr = mget_size();
    QueryMessage* cmsg = (QueryMessage*) msg;
    #if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC || CC_ALG == WOOKONG || CC_ALG == DTA
    COPY_VAL(cmsg->ts,buf,ptr);
      assert(cmsg->ts != 0);
    #endif
    #if CC_ALG == OCC || CC_ALG == FOCC || CC_ALG == BOCC || CC_ALG == SSI || CC_ALG == WSI || \
        CC_ALG == DLI_BASE || CC_ALG == DLI_OCC || CC_ALG == DLI_MVCC_OCC || \
        CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3 || CC_ALG == DLI_MVCC
    COPY_VAL(cmsg->start_ts,buf,ptr);
    #endif
    YCSBQueryMessage* ymsg = (YCSBQueryMessage*) msg;
    ptr = cget_size(cmsg);
    size_t size;
    COPY_VAL(size,buf,ptr);
    ymsg->requests.init(size);
    for(uint64_t i = 0 ; i < size;i++) {
      DEBUG_M("YCSBQueryMessage::copy ycsb_request alloc\n");
      ycsb_request * req = (ycsb_request*)mem_allocator.alloc(sizeof(ycsb_request));
      COPY_VAL(*req,buf,ptr);
      //DEBUG("3YCSBClientQuery %ld\n",ptr);
      assert(req->key < g_synth_table_size);
      ymsg->requests.add(req);
    }
    assert(ptr == ymsg->get_size());
  } else if (msg->rtype == RQRY_RSP) {
    QueryResponseMessage *qmsg = (QueryResponseMessage*)msg;
    ptr = mget_size();
    COPY_VAL(qmsg->rc,buf,ptr);
    #if CC_ALG == TICTOC
    COPY_VAL(qmsg->_min_commit_ts,buf,ptr);
    #endif
    assert(ptr == qmsg->get_size());
  }
  
  return msg;
}
#else
void fake_copy_to_buf(Message * msg, char* buf) {
  // !mcopy_to_buf
  
  uint64_t ptr = 0;
  *(RemReqType*)(buf+ptr) = msg->rtype;
  ptr += sizeof(RemReqType);
  *(uint64_t*)(buf+ptr) = msg->txn_id;
  ptr += sizeof(uint64_t);
  *(uint64_t*)(buf+ptr) = msg->mq_time;
  ptr += sizeof(uint64_t);
  *(double*)(buf+ptr) = msg->lat_work_queue_time;
  ptr += sizeof(double);
  *(double*)(buf+ptr) = msg->lat_msg_queue_time;
  ptr += sizeof(double);
  *(double*)(buf+ptr) = msg->lat_cc_block_time;
  ptr += sizeof(double);
  *(double*)(buf+ptr) = msg->lat_cc_time;
  ptr += sizeof(double);
  *(double*)(buf+ptr) = msg->lat_process_time;
  ptr += sizeof(double);

  if (((CC_ALG == CALVIN) && (msg->rtype == CL_QRY||msg->rtype == CL_QRY_O) && msg->txn_id % g_node_cnt == g_node_id) ||
      ((CC_ALG != CALVIN) && IS_LOCAL(msg->txn_id))) {
    msg->lat_network_time = get_sys_clock();
  } else {
    msg->lat_other_time = get_sys_clock() - msg->lat_other_time;
  }

  *(double*)(buf+ptr) = msg->lat_network_time;
  ptr += sizeof(double);
  *(double*)(buf+ptr) = msg->lat_other_time;
  ptr += sizeof(double);

  if (msg->rtype == RQRY) {
    // !copy_to_buf
    ptr = mget_size();
    QueryMessage* cmsg = (QueryMessage*) msg;

    // !YCSBClientQueryMessage:copy_to_buf
    ptr = cget_size(cmsg);
    YCSBQueryMessage* ymsg = (YCSBQueryMessage*) msg;
    size_t size = ymsg->requests.size();

    *(size_t*)(buf+ptr) = size;
    ptr += sizeof(size_t);

    for(uint64_t i = 0; i < ymsg->requests.size(); i++) {
      ycsb_request * req = ymsg->requests[i];
      assert(req->key < g_synth_table_size);
      COPY_BUF(buf,*req,ptr);
    }
    assert(ptr == ymsg->get_size());
  } else if (msg->rtype == RQRY_RSP) {
    ptr = mget_size();
    QueryResponseMessage* qmsg = (QueryResponseMessage*)msg;
    *(RC*)(buf+ptr) = qmsg->rc;
    ptr += sizeof(RC);  

    assert(ptr == qmsg->get_size());
  }  
}

Message* fake_create_message(char * buf) {
  RemReqType rtype = NO_MSG;
  uint64_t ptr = 0;
  COPY_VAL(rtype,buf,ptr);
  Message * msg = Message::create_message(rtype);
  
  // !mcopy_from_buf(txn);
  ptr = 0;
  msg->rtype = *(RemReqType*)(buf+ptr);
  ptr += sizeof(RemReqType);
  msg->txn_id = *(uint64_t*)(buf+ptr);
  ptr += sizeof(uint64_t);
  msg->mq_time += *(uint64_t*)(buf+ptr);
  ptr += sizeof(uint64_t);
  msg->lat_work_queue_time = *(double*)(buf+ptr);
  ptr += sizeof(double);
  msg->lat_msg_queue_time = *(double*)(buf+ptr);
  ptr += sizeof(double);
  msg->lat_cc_block_time = *(double*)(buf+ptr);
  ptr += sizeof(double);
  msg->lat_cc_time += *(double*)(buf+ptr);
  ptr += sizeof(double);
  msg->lat_process_time = *(double*)(buf+ptr);
  ptr += sizeof(double);
  
  msg->lat_network_time = *(double*)(buf+ptr);
  ptr += sizeof(double);
  msg->lat_other_time = *(double*)(buf+ptr);
  ptr += sizeof(double);
  if (((CC_ALG == CALVIN) && rtype == CALVIN_ACK && msg->txn_id % g_node_cnt == g_node_id) ||
      ((CC_ALG != CALVIN) && IS_LOCAL(msg->txn_id))) {
    msg->lat_network_time = (get_sys_clock() - msg->lat_network_time) - msg->lat_other_time;
  } else {
    msg->lat_other_time = get_sys_clock();
  }
  if (msg->rtype == RQRY) {
    // !QueryMessage::copy_from_buf(txn);
    ptr = mget_size();
    QueryMessage* cmsg = (QueryMessage*) msg;

    YCSBQueryMessage* ymsg = (YCSBQueryMessage*) msg;
    ptr = cget_size(cmsg);
    size_t size;
    size = *(size_t*)(buf+ptr);
    ptr += sizeof(size_t);

    ymsg->requests.init(size);
    for(uint64_t i = 0 ; i < size;i++) {
      DEBUG_M("YCSBQueryMessage::copy ycsb_request alloc\n");
      ycsb_request * req = (ycsb_request*)mem_allocator.alloc(sizeof(ycsb_request));
      COPY_VAL(*req,buf,ptr);
      //DEBUG("3YCSBClientQuery %ld\n",ptr);
      assert(req->key < g_synth_table_size);
      ymsg->requests.add(req);
    }
    assert(ptr == ymsg->get_size());
  } else if (msg->rtype == RQRY_RSP) {
    QueryResponseMessage *qmsg = (QueryResponseMessage*)msg;
    ptr = mget_size();
    qmsg->rc = *(RC*)(buf+ptr);
    ptr += sizeof(RC);  

    assert(ptr == qmsg->get_size());
  }
  
  return msg;
}
#endif
void MessageThread::run() {

  uint64_t starttime = get_sys_clock();
  Message * msg = NULL;
  uint64_t dest_node_id;
  mbuf * sbuf;

  dest_node_id = msg_queue.dequeue(get_thd_id(), msg);
  if(!msg) {
    INC_STATS(_thd_id,mtx[9],get_sys_clock() - starttime);
    check_and_send_batches();
    return;
  }
  assert(msg);
  assert(dest_node_id < g_total_node_cnt);
// #if SEND_TO_SELF_PAHSE == 0
//   if (dest_node_id == g_node_id) {// && 
// #elif SEND_TO_SELF_PAHSE == 1
//   if (dest_node_id == g_node_id &&
//      (msg->get_rtype() == RQRY_RSP) {
// #elif SEND_TO_SELF_PAHSE == 2
//   if (dest_node_id == g_node_id &&
//      (msg->get_rtype() == RQRY)) {
// #elif SEND_TO_SELF_PAHSE == 3
//   if (false) {
// #endif
// #if SEND_STAGE == 1
//     DEBUG("try to %ld enqueue Msg into workqueue %d, (%ld,%ld) to %ld\n", _thd_id, msg->rtype, msg->txn_id, msg->batch_id,
//         dest_node_id);
//     uint64_t starttime = get_sys_clock();
//     sbuf = buffer[dest_node_id];

//     if(!sbuf->fits(msg->get_size())) {
//       assert(sbuf->cnt > 0);
//       sbuf->reset(dest_node_id);
//     }
//     uint64_t old_ptr = sbuf->ptr;
//     ASSERT(msg->get_rtype() == RQRY ||
//         msg->get_rtype() == RQRY_RSP);
//     if (msg->get_rtype() == RQRY ||
//         msg->get_rtype() == RQRY_RSP) {
//       fake_copy_to_buf(msg, &(sbuf->buffer[sbuf->ptr]));
//     } else {
//       msg->copy_to_buf(&(sbuf->buffer[sbuf->ptr]));
//     }
//     sbuf->cnt += 1;
//     sbuf->ptr += msg->get_size();
//     // if(CC_ALG != CALVIN) {
//     //   Message::release_message(msg);
//     // }
//     Message *new_msg;
//     if (msg->get_rtype() == RQRY ||
//         msg->get_rtype() == RQRY_RSP) {
//       new_msg = fake_create_message(&sbuf->buffer[old_ptr]);
//     } else {
//       new_msg = Message::create_message(&sbuf->buffer[old_ptr]);
//     }
    
//     new_msg->return_node_id = g_node_id;
//     DEBUG("%ld enqueue Msg into workqueue %d, (%ld,%ld) to %ld\n", _thd_id, new_msg->rtype, new_msg->txn_id, new_msg->batch_id,
//         dest_node_id);
//     if (buffer[dest_node_id]->ready()) {
//       assert(sbuf->cnt > 0);
//       sbuf->reset(dest_node_id);   
//     }
//     INC_STATS(0,trans_msgsend_stage_one,get_sys_clock()-starttime);
//     work_queue.enqueue(get_thd_id(),msg,false);
// #elif SEND_STAGE == 2
//     DEBUG("try to %ld enqueue Msg into workqueue %d, (%ld,%ld) to %ld\n", _thd_id, msg->rtype, msg->txn_id, msg->batch_id,
//         dest_node_id);
//     uint64_t starttime = get_sys_clock();
//     uint64_t stage3_span1 = 0, stage3_span2 = 0;
//     sbuf = buffer[dest_node_id];

//     if(!sbuf->fits(msg->get_size())) {
//       assert(sbuf->cnt > 0);
//       // stage 3 parse msg
//       uint64_t stage3_starttime = get_sys_clock();
//       ((uint32_t*)sbuf->buffer)[2] = sbuf->cnt;
//       sbuf->set_send_time(get_sys_clock());
//       Message::create_messages((char*)sbuf->buffer);
//       stage3_span1 = get_sys_clock() - stage3_starttime;
//       // stage 3 parse msg
//       sbuf->reset(dest_node_id);
//     }
//     uint64_t old_ptr = sbuf->ptr;
//     msg->copy_to_buf(&(sbuf->buffer[sbuf->ptr]));
//     sbuf->cnt += 1;
//     sbuf->ptr += msg->get_size();
//     if(CC_ALG != CALVIN) {
//       Message::release_message(msg);
//     }
//     Message *new_msg = Message::create_message(&sbuf->buffer[old_ptr]);
//     new_msg->return_node_id = g_node_id;
//     if (buffer[dest_node_id]->ready()) {
//       assert(sbuf->cnt > 0);
//       // stage 3 parse msg
//       uint64_t stage3_starttime = get_sys_clock();
//       ((uint32_t*)sbuf->buffer)[2] = sbuf->cnt;
//       sbuf->set_send_time(get_sys_clock());
//       Message::create_messages((char*)sbuf->buffer);
//       stage3_span2 = get_sys_clock() - stage3_starttime;
//       // stage 3 parse msg
//       sbuf->reset(dest_node_id);      
//     }
//     DEBUG("%ld enqueue Msg into workqueue %d, (%ld,%ld) to %ld\n", _thd_id, new_msg->rtype, new_msg->txn_id, new_msg->batch_id,
//         dest_node_id);
//     INC_STATS(0,trans_msgsend_stage_one,get_sys_clock()-starttime - stage3_span1 - stage3_span2);
//     INC_STATS(0,trans_msgsend_stage_three,stage3_span1 + stage3_span2);
//     work_queue.enqueue(get_thd_id(),msg,false);
// #else
//     DEBUG("try to %ld enqueue Msg into workqueue %d, (%ld,%ld) to %ld\n", _thd_id, msg->rtype, msg->txn_id, msg->batch_id,
//         dest_node_id);
//     work_queue.enqueue(get_thd_id(),msg,false);
// #endif
//     return;
//   }

#if ONE_NODE_RECIEVE == 1 && defined(NO_REMOTE) && LESS_DIS_NUM == 10
#else
  assert(dest_node_id != g_node_id);
#endif

  sbuf = buffer[dest_node_id];

  if(!sbuf->fits(msg->get_size())) {
    assert(sbuf->cnt > 0);
    send_batch(dest_node_id);
  }

  #if WORKLOAD == DA
  if(!is_server&&true)
  printf("cl seq_id:%lu type:%c trans_id:%lu item:%c state:%lu next_state:%lu write_version:%lu\n",
      ((DAClientQueryMessage*)msg)->seq_id,
      type2char1(((DAClientQueryMessage*)msg)->txn_type),
      ((DAClientQueryMessage*)msg)->trans_id,
      static_cast<char>('x'+((DAClientQueryMessage*)msg)->item_id),
      ((DAClientQueryMessage*)msg)->state,
      (((DAClientQueryMessage*)msg)->next_state),
      ((DAClientQueryMessage*)msg)->write_version);
      fflush(stdout);
  #endif

  uint64_t copy_starttime = get_sys_clock();
  msg->copy_to_buf(&(sbuf->buffer[sbuf->ptr]));
  INC_STATS(_thd_id,msg_copy_output_time,get_sys_clock() - copy_starttime);
  DEBUG("%ld Buffered Msg %d, (%ld,%ld) to %ld\n", _thd_id, msg->rtype, msg->txn_id, msg->batch_id,
        dest_node_id);
  sbuf->cnt += 1;
  sbuf->ptr += msg->get_size();
  // Free message here, no longer needed unless CALVIN sequencer
  if(CC_ALG != CALVIN) {
    Message::release_message(msg);
  }
  if (sbuf->starttime == 0) sbuf->starttime = get_sys_clock();

  check_and_send_batches();
  INC_STATS(_thd_id,mtx[10],get_sys_clock() - starttime);

}

