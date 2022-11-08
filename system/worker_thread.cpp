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

#include "worker_thread.h"

#include "abort_queue.h"
#include "global.h"
#include "helper.h"
#include "logger.h"
#include "maat.h"
#include "manager.h"
#include "math.h"
#include "message.h"
#include "msg_queue.h"
#include "msg_thread.h"
#include "query.h"
#include "tpcc_query.h"
#include "txn.h"
#include "wl.h"
#include "work_queue.h"
#include "ycsb_query.h"
#include "maat.h"
#include "transport.h"
#include "routine.h"
#include "log_rdma.h"
#include "index_rdma.h"
#include <boost/bind.hpp>
#include <unordered_map>


void WorkerThread::setup() {
	if( get_thd_id() == 0) {
    send_init_done_to_all_nodes();
  }
#if USE_COROUTINE
  for(uint64_t i = 0; i <= COROUTINE_CNT; ++i) {
    _cor_txn_id[i] = 0;
  }
#else
  _thd_txn_id = 0;
#endif
}

void WorkerThread::statqueue(uint64_t thd_id, Message * msg, uint64_t starttime) {
  if (msg->rtype == RTXN_CONT ||
      msg->rtype == RQRY_RSP || msg->rtype == RACK_PREP  ||
      msg->rtype == RACK_FIN || msg->rtype == RTXN  ||
      msg->rtype == CL_RSP) {
    uint64_t queue_time = get_sys_clock() - starttime;
		INC_STATS(thd_id,trans_local_process,queue_time);
  } else if (msg->rtype == RQRY || msg->rtype == RQRY_CONT ||
             msg->rtype == RFIN || msg->rtype == RPREPARE ||
             msg->rtype == RFWD){
    uint64_t queue_time = get_sys_clock() - starttime;
		INC_STATS(thd_id,trans_remote_process,queue_time);
  } else if (msg->rtype == CL_QRY || msg->rtype == CL_QRY_O) {
    uint64_t queue_time = get_sys_clock() - starttime;
    INC_STATS(thd_id,trans_process_client,queue_time);
  }
}

void WorkerThread::process(yield_func_t &yield, Message * msg, uint64_t cor_id) {
  RC rc __attribute__ ((unused));

  DEBUG("%ld Processing %ld %d\n",get_thd_id(),msg->get_txn_id(),msg->get_rtype());
  assert(msg->get_rtype() == CL_QRY || msg->get_rtype() == CL_QRY_O || msg->get_txn_id() != UINT64_MAX);
  uint64_t starttime = get_sys_clock();
#if USE_COROUTINE
  cor_process_starttime[cor_id] = get_sys_clock();
#endif
		switch(msg->get_rtype()) {
			case RPASS:
        //rc = process_rpass(msg);
				break;
			case RPREPARE:
        rc = process_rprepare(yield, msg, cor_id);
				break;
			case RFWD:
        rc = process_rfwd(yield, msg, cor_id);
				break;
			case RQRY:
        rc = process_rqry(yield, msg, cor_id);
				break;
			case RQRY_CONT:
        rc = process_rqry_cont(yield, msg, cor_id);
				break;
			case RQRY_RSP:
        rc = process_rqry_rsp(yield, msg, cor_id);
				break;
			case RFIN:
        rc = process_rfin(yield, msg, cor_id);
				break;
			case RACK_PREP:
        rc = process_rack_prep(yield, msg, cor_id);
				break;
			case RACK_FIN:
        rc = process_rack_rfin(msg);
				break;
			case RTXN_CONT:
        rc = process_rtxn_cont(yield, msg, cor_id);
				break;
      case CL_QRY:
      case CL_QRY_O:
			case RTXN:
#if CC_ALG == CALVIN
        rc = process_calvin_rtxn(yield, msg, cor_id);
#else
        rc = process_rtxn(yield, msg, cor_id);
#endif
				break;
			case LOG_FLUSHED:
        rc = process_log_flushed(msg);
				break;
			case LOG_MSG:
        rc = process_log_msg(msg);
				break;
			case LOG_MSG_RSP:
        rc = process_log_msg_rsp(msg);
				break;
			default:
        printf("Msg: %d\n",msg->get_rtype());
        fflush(stdout);
				assert(false);
				break;
		}
  statqueue(get_thd_id(), msg, starttime);
#if USE_COROUTINE
  uint64_t timespan = get_sys_clock() - cor_process_starttime[cor_id];
#else
  uint64_t timespan = get_sys_clock() - starttime;
#endif
  INC_STATS(get_thd_id(),worker_process_cnt,1);

  INC_STATS(get_thd_id(),worker_process_time,timespan);
  INC_STATS(get_thd_id(),worker_process_cnt_by_type[msg->rtype],1);
  INC_STATS(get_thd_id(),worker_process_time_by_type[msg->rtype],timespan);
  DEBUG("%ld EndProcessing %d %ld\n",get_thd_id(),msg->get_rtype(),msg->get_txn_id());
}

#if USE_COROUTINE

void WorkerThread::co_check_if_done(RC rc, uint64_t cor_id) {
  if (cor_txn_man[cor_id]->waiting_for_response()) return;
  if (rc == Commit) {
    cor_txn_man[cor_id]->txn_stats.finish_start_time = get_sys_clock();
    co_commit(cor_id);
  }
  if (rc == Abort) {
    cor_txn_man[cor_id]->txn_stats.finish_start_time = get_sys_clock();
    co_abort(cor_id);
  }
}

void WorkerThread::co_release_txn_man(uint64_t cor_id) {
  txn_table.release_transaction_manager(get_thd_id(), cor_txn_man[cor_id]->get_txn_id(),
                                        cor_txn_man[cor_id]->get_batch_id());
  cor_txn_man[cor_id] = NULL;
}


// Can't use txn_man after this function
void WorkerThread::co_commit(uint64_t cor_id) {
  assert(cor_txn_man[cor_id]);
  assert(IS_LOCAL(cor_txn_man[cor_id]->get_txn_id()));

  uint64_t timespan = get_sys_clock() - cor_txn_man[cor_id]->txn_stats.starttime;
  DEBUG("COMMIT %ld %f -- %f\n", cor_txn_man[cor_id]->get_txn_id(),
        simulation->seconds_from_start(get_sys_clock()), (double)timespan / BILLION);

  // ! trans total time
  uint64_t end_time = get_sys_clock();
  uint64_t timespan_short  = end_time - cor_txn_man[cor_id]->txn_stats.restart_starttime;
  uint64_t two_pc_timespan  = end_time - cor_txn_man[cor_id]->txn_stats.prepare_start_time;
  uint64_t finish_timespan  = end_time - cor_txn_man[cor_id]->txn_stats.finish_start_time;
  uint64_t prepare_timespan = cor_txn_man[cor_id]->txn_stats.finish_start_time - cor_txn_man[cor_id]->txn_stats.restart_starttime;
  INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
  INC_STATS(get_thd_id(), trans_prepare_count, 1);

  INC_STATS(get_thd_id(), trans_2pc_time, two_pc_timespan);
  INC_STATS(get_thd_id(), trans_finish_time, finish_timespan);
  INC_STATS(get_thd_id(), trans_commit_time, finish_timespan);
  INC_STATS(get_thd_id(), trans_total_run_time, timespan_short);

  INC_STATS(get_thd_id(), trans_2pc_count, 1);
  INC_STATS(get_thd_id(), trans_finish_count, 1);
  INC_STATS(get_thd_id(), trans_commit_count, 1);
  INC_STATS(get_thd_id(), trans_total_count, 1);

  // Send result back to client
#if !SERVER_GENERATE_QUERIES
#if USE_RDMA == CHANGE_MSG_QUEUE
    tport_man.rdma_thd_send_msg(get_thd_id(), cor_txn_man[cor_id]->client_id, Message::create_message(cor_txn_man[cor_id],CL_RSP));
#else
    msg_queue.enqueue(get_thd_id(),Message::create_message(cor_txn_man[cor_id],CL_RSP),cor_txn_man[cor_id]->client_id);
#endif
#endif
  // remove txn from pool
  co_release_txn_man(cor_id);
  // Do not use txn_man after this
}

void WorkerThread::co_abort(uint64_t cor_id) {
  DEBUG("ABORT %ld -- %f\n", cor_txn_man[cor_id]->get_txn_id(),
        (double)get_sys_clock() - run_starttime / BILLION);
  // TODO: TPCC Rollback here

  ++cor_txn_man[cor_id]->abort_cnt;
  cor_txn_man[cor_id]->reset();

  uint64_t end_time = get_sys_clock();
  uint64_t timespan_short  = end_time - cor_txn_man[cor_id]->txn_stats.restart_starttime;
  uint64_t two_pc_timespan  = end_time - cor_txn_man[cor_id]->txn_stats.prepare_start_time;
  uint64_t finish_timespan  = end_time - cor_txn_man[cor_id]->txn_stats.finish_start_time;
  uint64_t prepare_timespan = cor_txn_man[cor_id]->txn_stats.finish_start_time - cor_txn_man[cor_id]->txn_stats.restart_starttime;
  INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
  INC_STATS(get_thd_id(), trans_prepare_count, 1);

  INC_STATS(get_thd_id(), trans_2pc_time, two_pc_timespan);
  INC_STATS(get_thd_id(), trans_finish_time, finish_timespan);
  INC_STATS(get_thd_id(), trans_abort_time, finish_timespan);
  INC_STATS(get_thd_id(), trans_total_run_time, timespan_short);

  INC_STATS(get_thd_id(), trans_2pc_count, 1);
  INC_STATS(get_thd_id(), trans_finish_count, 1);
  INC_STATS(get_thd_id(), trans_abort_count, 1);
  INC_STATS(get_thd_id(), trans_total_count, 1);
  #if WORKLOAD != DA //actually DA do not need real abort. Just count it and do not send real abort msg.
  uint64_t penalty =
      abort_queue.enqueue(get_thd_id(), cor_txn_man[cor_id]->get_txn_id(), cor_txn_man[cor_id]->get_abort_cnt());

  cor_txn_man[cor_id]->txn_stats.total_abort_time += penalty;
  #endif
}


uint64_t WorkerThread::co_get_next_txn_id(uint64_t cor_id) {
  uint64_t txn_id =
      (get_node_id() + (get_thd_id() + (cor_id-1) * g_thread_cnt) * g_node_cnt) + (COROUTINE_CNT * g_thread_cnt * g_node_cnt * _cor_txn_id[cor_id]);
  ++_cor_txn_id[cor_id];
  return txn_id;
}


#endif
void WorkerThread::check_if_done(RC rc) {
  if (txn_man->waiting_for_response()) return;
  if (rc == Commit) {
    txn_man->txn_stats.finish_start_time = get_sys_clock();
    commit();
  }
  if (rc == Abort) {
    txn_man->txn_stats.finish_start_time = get_sys_clock();
    abort();
  }
}

void WorkerThread::release_txn_man() {
  txn_table.release_transaction_manager(get_thd_id(), txn_man->get_txn_id(),
                                        txn_man->get_batch_id());
  txn_man = NULL;
}

void WorkerThread::calvin_wrapup(yield_func_t &yield, uint64_t cor_id) {
  txn_man->release_locks(yield, RCOK, cor_id);
  txn_man->commit_stats();
  DEBUG("(%ld,%ld) calvin ack to %ld\n", txn_man->get_txn_id(), txn_man->get_batch_id(),
        txn_man->return_id);
  if(txn_man->return_id == g_node_id) {
    work_queue.sequencer_enqueue(_thd_id,Message::create_message(txn_man,CALVIN_ACK));
  } else {
#if USE_RDMA == CHANGE_MSG_QUEUE
    tport_man.rdma_thd_send_msg(get_thd_id(), txn_man->return_id, Message::create_message(txn_man, CALVIN_ACK));
#else
    msg_queue.enqueue(get_thd_id(), Message::create_message(txn_man, CALVIN_ACK),
                      txn_man->return_id);
#endif
  }
  release_txn_man();
}

// Can't use txn_man after this function
void WorkerThread::commit() {
  txn_man->txn_stats.current_states = DONE_PHASE;
  total_local_txn_commit++;
  total_num_msgs_rw_prep += txn_man->num_msgs_rw_prep;
  total_num_msgs_commit += txn_man->num_msgs_commit;
  if(txn_man->num_msgs_rw_prep > max_num_msgs_rw_prep) max_num_msgs_rw_prep = txn_man->num_msgs_rw_prep;
  if(txn_man->num_msgs_commit > max_num_msgs_commit) max_num_msgs_commit = txn_man->num_msgs_commit;

  assert(txn_man);
  assert(IS_LOCAL(txn_man->get_txn_id()));

  uint64_t timespan = get_sys_clock() - txn_man->txn_stats.starttime;
  DEBUG_T("COMMIT %ld %f -- %f\n", txn_man->get_txn_id(),
        simulation->seconds_from_start(get_sys_clock()), (double)timespan / BILLION);

  // ! trans total time
  uint64_t end_time = get_sys_clock();
  uint64_t timespan_short  = end_time - txn_man->txn_stats.restart_starttime;
  uint64_t two_pc_timespan  = end_time - txn_man->txn_stats.prepare_start_time;
  uint64_t finish_timespan  = end_time - txn_man->txn_stats.finish_start_time;
  uint64_t prepare_timespan = txn_man->txn_stats.finish_start_time - txn_man->txn_stats.restart_starttime;
  INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
  INC_STATS(get_thd_id(), trans_prepare_count, 1);

  INC_STATS(get_thd_id(), trans_2pc_time, two_pc_timespan);
  INC_STATS(get_thd_id(), trans_finish_time, finish_timespan);
  INC_STATS(get_thd_id(), trans_commit_time, finish_timespan);
  INC_STATS(get_thd_id(), trans_total_run_time, timespan_short);

  INC_STATS(get_thd_id(), trans_2pc_count, 1);
  INC_STATS(get_thd_id(), trans_finish_count, 1);
  INC_STATS(get_thd_id(), trans_commit_count, 1);
  INC_STATS(get_thd_id(), trans_total_count, 1);

  // Send result back to client
#if !SERVER_GENERATE_QUERIES
#if USE_RDMA == CHANGE_MSG_QUEUE
    tport_man.rdma_thd_send_msg(get_thd_id(), txn_man->client_id, Message::create_message(txn_man,CL_RSP));
#else
    msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,CL_RSP),txn_man->client_id);
#endif
#endif
  // remove txn from pool  
  release_txn_man();
  // Do not use txn_man after this
}

void WorkerThread::abort() {
  DEBUG_T("ABORT %ld -- %f\n", txn_man->get_txn_id(),
        (double)get_sys_clock() - run_starttime / BILLION);
  // TODO: TPCC Rollback here
  txn_man->txn_stats.current_states = DONE_PHASE;
  ++txn_man->abort_cnt;
  txn_man->reset();

  uint64_t end_time = get_sys_clock();
  uint64_t timespan_short  = end_time - txn_man->txn_stats.restart_starttime;
  uint64_t two_pc_timespan  = end_time - txn_man->txn_stats.prepare_start_time;
  uint64_t finish_timespan  = end_time - txn_man->txn_stats.finish_start_time;
  uint64_t prepare_timespan = txn_man->txn_stats.finish_start_time - txn_man->txn_stats.restart_starttime;
  INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
  INC_STATS(get_thd_id(), trans_prepare_count, 1);

  INC_STATS(get_thd_id(), trans_2pc_time, two_pc_timespan);
  INC_STATS(get_thd_id(), trans_finish_time, finish_timespan);
  INC_STATS(get_thd_id(), trans_abort_time, finish_timespan);
  INC_STATS(get_thd_id(), trans_total_run_time, timespan_short);

  INC_STATS(get_thd_id(), trans_2pc_count, 1);
  INC_STATS(get_thd_id(), trans_finish_count, 1);
  INC_STATS(get_thd_id(), trans_abort_count, 1);
  INC_STATS(get_thd_id(), trans_total_count, 1);
  #if WORKLOAD != DA //actually DA do not need real abort. Just count it and do not send real abort msg.
  uint64_t penalty =
      abort_queue.enqueue(get_thd_id(), txn_man->get_txn_id(), txn_man->get_abort_cnt());

  txn_man->txn_stats.total_abort_time += penalty;
  #endif
  DEBUG_T("Txn %ld status is %ld\n", txn_man->get_txn_id(),
      txn_man->txn_stats.current_states);
}

TxnManager * WorkerThread::get_transaction_manager(Message * msg) {
#if CC_ALG == CALVIN
  TxnManager* local_txn_man =
      txn_table.get_transaction_manager(get_thd_id(), msg->get_txn_id(), msg->get_batch_id());
#else
  TxnManager * local_txn_man = txn_table.get_transaction_manager(get_thd_id(),msg->get_txn_id(),0);
#endif
  return local_txn_man;
}

char type2char(DATxnType txn_type)
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
#if USE_COROUTINE
RC WorkerThread::co_run(yield_func_t &yield, uint64_t cor_id) {
  //tsetup();
  printf("Running WorkerThread %ld:%ld\n",_thd_id, cor_id);

  uint64_t ready_starttime;
  uint64_t idle_starttime = 0;
  uint64_t get_msg_starttime = 0;

	while(!simulation->is_done()) {
    get_msg_starttime = get_sys_clock();
    cor_txn_man[cor_id] = NULL;
    heartbeat();

    progress_stats();
    Message* msg;

  // DA takes msg logic

  // #define TEST_MSG_order
  #ifdef TEST_MSG_order
    while(1)
    {
      msg = work_queue.dequeue(get_thd_id());
      if (!msg) {
        if (idle_starttime == 0) idle_starttime = get_sys_clock();
        continue;
      }
      printf("s seq_id:%lu type:%c trans_id:%lu item:%c state:%lu next_state:%lu\n",
      ((DAClientQueryMessage*)msg)->seq_id,
      type2char(((DAClientQueryMessage*)msg)->txn_type),
      ((DAClientQueryMessage*)msg)->trans_id,
      static_cast<char>('x'+((DAClientQueryMessage*)msg)->item_id),
      ((DAClientQueryMessage*)msg)->state,
      (((DAClientQueryMessage*)msg)->next_state));
      fflush(stdout);
    }
  #endif

    msg = work_queue.dequeue(get_thd_id());
    uint64_t get_msg_endtime = get_sys_clock();
    INC_STATS(_thd_id,worker_idle_time, get_msg_endtime - get_msg_starttime);
    INC_STATS(_thd_id,worker_msg_time, get_msg_endtime - get_msg_starttime);
    if(!msg) {

      last_yield_time = get_sys_clock();
		  // printf("do\n");
      // INC_STATS(get_thd_id(), worker_process_time, get_sys_clock() - cor_process_starttime[cor_id]);
      yield(_routines[((cor_id) % COROUTINE_CNT) + 1]);
      uint64_t yield_endtime = get_sys_clock();
      INC_STATS(get_thd_id(), worker_yield_cnt, 1);
      INC_STATS(get_thd_id(), worker_yield_time, yield_endtime - last_yield_time);
      INC_STATS(get_thd_id(), worker_idle_time, yield_endtime - last_yield_time);
      continue;
    }
    simulation->last_da_query_time = get_sys_clock();

    if((msg->rtype != CL_QRY && msg->rtype != CL_QRY_O) || CC_ALG == CALVIN) {
      cor_txn_man[cor_id] = get_transaction_manager(msg);

      if ((CC_ALG != CALVIN ) && IS_LOCAL(cor_txn_man[cor_id]->get_txn_id())) {
        if (msg->rtype != RTXN_CONT &&
            ((msg->rtype != RACK_PREP) || (cor_txn_man[cor_id]->get_rsp_cnt() == 1))) {
          cor_txn_man[cor_id]->txn_stats.work_queue_time_short += msg->lat_work_queue_time;
          cor_txn_man[cor_id]->txn_stats.cc_block_time_short += msg->lat_cc_block_time;
          cor_txn_man[cor_id]->txn_stats.cc_time_short += msg->lat_cc_time;
          cor_txn_man[cor_id]->txn_stats.msg_queue_time_short += msg->lat_msg_queue_time;
          cor_txn_man[cor_id]->txn_stats.process_time_short += msg->lat_process_time;
          /*
          if (msg->lat_network_time/BILLION > 1.0) {
            printf("%ld %d %ld -> %ld: %f %f\n",msg->txn_id, msg->rtype,
          msg->return_node_id,get_node_id() ,msg->lat_network_time/BILLION,
          msg->lat_other_time/BILLION);
          }
          */
          cor_txn_man[cor_id]->txn_stats.network_time_short += msg->lat_network_time;
        }

      } else {
          cor_txn_man[cor_id]->txn_stats.clear_short();
      }
      if (CC_ALG != CALVIN ) {
        cor_txn_man[cor_id]->txn_stats.lat_network_time_start = msg->lat_network_time;
        cor_txn_man[cor_id]->txn_stats.lat_other_time_start = msg->lat_other_time;
      }
      cor_txn_man[cor_id]->txn_stats.msg_queue_time += msg->mq_time;
      cor_txn_man[cor_id]->txn_stats.msg_queue_time_short += msg->mq_time;
      msg->mq_time = 0;
      cor_txn_man[cor_id]->txn_stats.work_queue_time += msg->wq_time;
      cor_txn_man[cor_id]->txn_stats.work_queue_time_short += msg->wq_time;
      //cor_txn_man[cor_id]->txn_stats.network_time += msg->ntwk_time;
      msg->wq_time = 0;
      cor_txn_man[cor_id]->txn_stats.work_queue_cnt += 1;


      ready_starttime = get_sys_clock();
      bool ready = cor_txn_man[cor_id]->unset_ready();
      INC_STATS(get_thd_id(),worker_activate_txn_time,get_sys_clock() - ready_starttime);
      if(!ready) {
        // Return to work queue, end processing
        work_queue.enqueue(get_thd_id(),msg,true);
        continue;
      }
      cor_txn_man[cor_id]->register_thread(this, cor_id);
    }

    process(yield, msg, cor_id);
    // yield(_routines[0]);

    ready_starttime = get_sys_clock();
    if(cor_txn_man[cor_id]) {
      bool ready = cor_txn_man[cor_id]->set_ready();
      assert(ready);
    }
    INC_STATS(get_thd_id(),worker_deactivate_txn_time,get_sys_clock() - ready_starttime);

    // delete message
    ready_starttime = get_sys_clock();
#if CC_ALG != CALVIN 
    msg->release();
#endif
    INC_STATS(get_thd_id(),worker_release_msg_time,get_sys_clock() - ready_starttime);
    
	}
  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
  return FINISH;
}

void WorkerThread::create_routines(int coroutines) {
    _routines = new coroutine_func_t[1 + coroutines];

    _routines[0] = coroutine_func_t(bind(&WorkerThread::master_routine, this, _1, 0));
    for(uint64_t i = 1; i <= coroutines; ++i) {
    _routines[i] = coroutine_func_t(bind(&WorkerThread::co_run, this, _1, i));
    }
    total_worker_coroutine = coroutines;
    printf("Init coroutine succ\n");
}

void WorkerThread::master_routine(yield_func_t &yield, int cor_id) {
    tsetup();
    printf("Running WorkerThread %ld:%ld\n",_thd_id, cor_id);
    printf("This is new master routine\n");
    //yield(_routines[1]);
    for(uint64_t i = 0; i <= COROUTINE_CNT; i++) {
      pendings[i] = 0;
    }
    while(!simulation->is_done()) {
        last_yield_time = get_sys_clock();
        yield(_routines[1]);
        uint64_t yield_endtime = get_sys_clock();
        INC_STATS(get_thd_id(), worker_yield_time, yield_endtime - last_yield_time);
        INC_STATS(get_thd_id(), worker_idle_time, yield_endtime - last_yield_time);
        INC_STATS(get_thd_id(), worker_yield_cnt, 1);
        // uint64_t starttime;
        // uint64_t endtime;
        // starttime = get_sys_clock();
        // for(uint64_t i = 1; i <= COROUTINE_CNT; i++)
        {
            // yield(_routines[i]);
            
            
            // uint64_t target_server = un_res_p.front().first;
            // uint64_t thd_id = un_res_p.front().second;
            // // printf("sever:%ld thd_id:%ld", target_server, thd_id);
            // un_res_p.pop();
            // auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
            // RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
            

            
        // auto res_p = rc_qp[target_server][thd_id]->wait_one_comp();
            // RDMA_ASSERT(res_p == rdmaio::IOCode::Ok);
        }
        // endtime = get_sys_clock();
        // INC_STATS(get_thd_id(), worker_idle_time, endtime-starttime);
    }
    printf("FINISH %ld:%ld:%ld\n",_node_id,_thd_id, cor_id);
    fflush(stdout);
}
#endif
void WorkerThread::no_routines() {
     _routines = new coroutine_func_t[1];

     _routines[0] = coroutine_func_t(bind(&WorkerThread::run, this, _1, 0));
     printf("Init coroutine succ\n");
}
RC WorkerThread::run(yield_func_t &yield, uint64_t cor_id) {

  tsetup();
  printf("Running WorkerThread %ld\n",_thd_id);

  uint64_t ready_starttime;
  uint64_t idle_starttime = 0;
  uint64_t get_msg_starttime = 0;

	while(!simulation->is_done()) {
    get_msg_starttime = get_sys_clock();
    txn_man = NULL;
    heartbeat();

    progress_stats();
    Message* msg;

  // DA takes msg logic

  // #define TEST_MSG_order
  #ifdef TEST_MSG_order
    while(1)
    {
      msg = work_queue.dequeue(get_thd_id());
      if (!msg) {
        if (idle_starttime == 0) idle_starttime = get_sys_clock();
        continue;
      }
      printf("s seq_id:%lu type:%c trans_id:%lu item:%c state:%lu next_state:%lu\n",
      ((DAClientQueryMessage*)msg)->seq_id,
      type2char(((DAClientQueryMessage*)msg)->txn_type),
      ((DAClientQueryMessage*)msg)->trans_id,
      static_cast<char>('x'+((DAClientQueryMessage*)msg)->item_id),
      ((DAClientQueryMessage*)msg)->state,
      (((DAClientQueryMessage*)msg)->next_state));
      fflush(stdout);
    }
  #endif

    msg = work_queue.dequeue(get_thd_id());
    uint64_t get_msg_endtime = get_sys_clock();
    INC_STATS(_thd_id,worker_idle_time, get_msg_endtime - get_msg_starttime);
    INC_STATS(_thd_id,worker_msg_time, get_msg_endtime - get_msg_starttime);
    if(!msg) {
      if (idle_starttime == 0) idle_starttime = get_sys_clock();
      //todo: add sleep 0.01ms
      continue;
    }
    simulation->last_da_query_time = get_sys_clock();
    if(idle_starttime > 0) {
      // INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
      idle_starttime = 0;
    }
    //uint64_t starttime = get_sys_clock();

    if((msg->rtype != CL_QRY && msg->rtype != CL_QRY_O) || CC_ALG == CALVIN) {
      txn_man = get_transaction_manager(msg);

      ready_starttime = get_sys_clock();
      bool ready;
      if((msg->rtype == RQRY || msg->rtype == RFIN) && msg->current_abort_cnt > txn_man->txn_stats.abort_cnt){
        // assert(msg->current_abort_cnt > txn_man->txn_stats.abort_cnt);
        ready = false;
      }
      else if(msg->rtype == RFIN && !txn_man->finish_logging) ready = false;
      else ready = txn_man->unset_ready(); 
      
      INC_STATS(get_thd_id(),worker_activate_txn_time,get_sys_clock() - ready_starttime);
      if(!ready) {
        // Return to work queue, end processing
        work_queue.enqueue(get_thd_id(),msg,true);
        continue;
      }

      if(!txn_man->txn){
        assert(msg->rtype == RACK_PREP || msg->rtype == RACK_FIN);
        //   printf("---\n");        
          bool ready = txn_man->set_ready();
          assert(ready);
        continue; // in majority, txn_man may has been destroyed when receive msgs
      } 

      if ((CC_ALG != CALVIN ) && IS_LOCAL(txn_man->get_txn_id())) {
        if (msg->rtype != RTXN_CONT &&
            ((msg->rtype != RACK_PREP) || (txn_man->get_rsp_cnt() == 1))) {
          txn_man->txn_stats.work_queue_time_short += msg->lat_work_queue_time;
          txn_man->txn_stats.cc_block_time_short += msg->lat_cc_block_time;
          txn_man->txn_stats.cc_time_short += msg->lat_cc_time;
          txn_man->txn_stats.msg_queue_time_short += msg->lat_msg_queue_time;
          txn_man->txn_stats.process_time_short += msg->lat_process_time;
          /*
          if (msg->lat_network_time/BILLION > 1.0) {
            printf("%ld %d %ld -> %ld: %f %f\n",msg->txn_id, msg->rtype,
          msg->return_node_id,get_node_id() ,msg->lat_network_time/BILLION,
          msg->lat_other_time/BILLION);
          }
          */
          txn_man->txn_stats.network_time_short += msg->lat_network_time;
        }

      } else {
          txn_man->txn_stats.clear_short();
      }
      if (CC_ALG != CALVIN ) {
        txn_man->txn_stats.lat_network_time_start = msg->lat_network_time;
        txn_man->txn_stats.lat_other_time_start = msg->lat_other_time;
      }
      txn_man->txn_stats.msg_queue_time += msg->mq_time;
      txn_man->txn_stats.msg_queue_time_short += msg->mq_time;
      msg->mq_time = 0;
      txn_man->txn_stats.work_queue_time += msg->wq_time;
      txn_man->txn_stats.work_queue_time_short += msg->wq_time;
      //txn_man->txn_stats.network_time += msg->ntwk_time;
      msg->wq_time = 0;
      txn_man->txn_stats.work_queue_cnt += 1;

#if !USE_COROUTINE
      txn_man->register_thread(this);
#endif
    }
    process(yield, msg, cor_id);

    ready_starttime = get_sys_clock();
    if(txn_man) {
      bool ready = txn_man->set_ready();
      assert(ready);
    }
    INC_STATS(get_thd_id(),worker_deactivate_txn_time,get_sys_clock() - ready_starttime);

    // delete message
    ready_starttime = get_sys_clock();
#if CC_ALG != CALVIN 
    msg->release();
#endif
    INC_STATS(get_thd_id(),worker_release_msg_time,get_sys_clock() - ready_starttime);
	}
	printf("FINISH %ld:%ld, extra wait time: %lu(ns)\n",_node_id,_thd_id,extra_wait_time);
  fflush(stdout);
  return FINISH;
}

RC WorkerThread::process_rfin(yield_func_t &yield, Message * msg, uint64_t cor_id) {
  DEBUG_T("RFIN %ld from %ld\n",msg->get_txn_id(), msg->get_return_id());
  assert(CC_ALG != CALVIN );
  
  M_ASSERT_V(!IS_LOCAL(msg->get_txn_id()), "RFIN local: %ld %ld/%d\n", msg->get_txn_id(),
             msg->get_txn_id() % g_node_cnt, g_node_id);
#if CC_ALG == MAAT || USE_REPLICA
  txn_man->set_commit_timestamp(((FinishMessage*)msg)->commit_timestamp);
#endif

  if(((FinishMessage*)msg)->rc == Abort) {
    txn_man->abort(yield, cor_id);
    txn_man->reset();
    txn_man->reset_query();
    txn_man->abort_cnt = msg->current_abort_cnt;
#if USE_RDMA == CHANGE_MSG_QUEUE
    tport_man.rdma_thd_send_msg(get_thd_id(), GET_NODE_ID(msg->get_txn_id()), Message::create_message(txn_man, RACK_FIN));
#else
    msg_queue.enqueue(get_thd_id(), Message::create_message(txn_man, RACK_FIN),
                      GET_NODE_ID(msg->get_txn_id()));
#endif
    return Abort;
  }
  txn_man->txn_stats.current_states = FINISH_PHASE;
  txn_man->commit(yield, cor_id);
  //if(!txn_man->query->readonly() || CC_ALG == OCC)
  if (!((FinishMessage*)msg)->readonly || CC_ALG == MAAT || CC_ALG == OCC || USE_REPLICA)
    txn_man->abort_cnt = msg->current_abort_cnt;
#if USE_RDMA == CHANGE_MSG_QUEUE
    tport_man.rdma_thd_send_msg(get_thd_id(), GET_NODE_ID(msg->get_txn_id()), Message::create_message(txn_man, RACK_FIN));
#else
  msg_queue.enqueue(get_thd_id(), Message::create_message(txn_man, RACK_FIN),
                    GET_NODE_ID(msg->get_txn_id()));
#endif
  release_txn_man();

  return RCOK;
}

RC WorkerThread::process_rack_prep(yield_func_t &yield, Message * msg, uint64_t cor_id) {
  DEBUG_T("RPREP_ACK %ld from %ld\n",msg->get_txn_id(), msg->get_return_id());
  RC rc = RCOK;
  uint64_t starttime = get_sys_clock();
  uint64_t center_from = GET_CENTER_ID(msg->return_node_id);
  msg->copy_to_txn(txn_man);

#if CC_ALG == MAAT
  // Integrate bounds
  uint64_t lower = ((AckMessage*)msg)->lower;
  uint64_t upper = ((AckMessage*)msg)->upper;
  if(lower > time_table.get_lower(get_thd_id(),msg->get_txn_id())) {
    time_table.set_lower(get_thd_id(),msg->get_txn_id(),lower);
  }
  if(upper < time_table.get_upper(get_thd_id(),msg->get_txn_id())) {
    time_table.set_upper(get_thd_id(),msg->get_txn_id(),upper);
  }
  DEBUG("%ld bound set: [%ld,%ld] -> [%ld,%ld]\n", msg->get_txn_id(), lower, upper,
        time_table.get_lower(get_thd_id(), msg->get_txn_id()),
        time_table.get_upper(get_thd_id(), msg->get_txn_id()));
  if(((AckMessage*)msg)->rc != RCOK) {
    time_table.set_state(get_thd_id(),msg->get_txn_id(),MAAT_ABORTED);
  }
#endif
  if(!txn_man->query || txn_man->query->partitions_touched.size() == 0 || txn_man->abort_cnt != msg->current_abort_cnt || txn_man->get_commit_timestamp() != 0) {
    return RCOK;
  }
  if (txn_man->txn_stats.current_states >= FINISH_PHASE) {
    return RCOK;
  }

  // int responses_left = txn_man->received_response(((AckMessage*)msg)->rc);
  int responses_left = txn_man->received_response((AckMessage*)msg, OpStatus::PREPARE);
  if (responses_left > 0) return WAIT;
  // Done waiting
  txn_man->set_commit_timestamp(get_next_ts());

  uint64_t curr_time = get_sys_clock();
  INC_STATS(get_thd_id(),trans_wait_for_rsp_time, curr_time - txn_man->txn_stats.wait_for_rsp_time);
  INC_STATS(get_thd_id(),trans_wait_for_rsp_count, 1);
  txn_man->txn_stats.wait_for_rsp_time = curr_time;

  if(txn_man->get_rc() == RCOK) {
    rc = txn_man->validate(yield, cor_id); 
#if USE_REPLICA
    // if(rc != Abort) 
    rc = txn_man->redo_log(yield,rc,cor_id);
    if (rc != RCOK) {
      rc = rc == NODE_FAILED ? Abort : rc;
    }
#endif
  }
  uint64_t finish_start_time = get_sys_clock();
  txn_man->txn_stats.finish_start_time = finish_start_time;
  // uint64_t prepare_timespan  = finish_start_time - txn_man->txn_stats.prepare_start_time;
  // INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
  // INC_STATS(get_thd_id(), trans_prepare_count, 1);
  if(rc == Abort || txn_man->get_rc() == Abort) {
    txn_man->txn->rc = Abort;
    rc = Abort;
  }
  //commit phase: now commit or abort
  txn_man->send_finish_messages();
  if(rc == Abort) {
    txn_man->abort(yield, cor_id);
    txn_man->update_query_status(g_node_id,OpStatus::PREP_ABORT);
  } else {
    txn_man->commit(yield, cor_id);
    txn_man->update_query_status(g_node_id,OpStatus::COMMIT);
  }

  return rc;
}

RC WorkerThread::process_rack_rfin(Message * msg) {
  DEBUG_T("RFIN_ACK %ld from %ld\n",msg->get_txn_id(), msg->get_return_id());
  RC rc = RCOK;
  uint64_t starttime = get_sys_clock();
  // txn_man->txn_stats.current_states = FINISH_PHASE;
  // if(txn_man->abort_cnt > 0) printf("acnt:%lu\n",txn_man->abort_cnt);
  uint64_t center_from = GET_CENTER_ID(msg->return_node_id);
  if(!txn_man->query || txn_man->query->partitions_touched.size() == 0 || txn_man->abort_cnt != msg->current_abort_cnt || txn_man->get_commit_timestamp() == 0) 
  {
    DEBUG_T("Txn %ld rack fin skip, because %p, part_touch %ld, abort %ld:%ld, ts %lu\n",msg->get_txn_id(), txn_man->query, txn_man->query->partitions_touched.size(), txn_man->abort_cnt, msg->current_abort_cnt, txn_man->get_commit_timestamp());
    return RCOK;
  }

  // int responses_left = txn_man->received_response(((AckMessage*)msg)->rc);
  int responses_left = txn_man->received_response((AckMessage*)msg, OpStatus::COMMIT);
  if (responses_left > 0) return WAIT;
  #if WORKLOAD == YCSB
  for(int i=0;i<REQ_PER_QUERY;i++){
    if(txn_man->req_need_wait[i]) return WAIT;
  }
  #else 
  if (txn_man->wh_need_wait || txn_man->cus_need_wait) {
    return WAIT;
  }
  for(int i=0;i<MAX_ITEMS_PER_TXN;i++){
    if(txn_man->req_need_wait[i]) return WAIT;
  }
  #endif
  
  // Done waiting
  uint64_t curr_time = get_sys_clock();
  txn_man->txn_stats.twopc_time += curr_time - txn_man->txn_stats.wait_starttime;
  
  INC_STATS(get_thd_id(),trans_wait_for_commit_rsp_time, curr_time - txn_man->txn_stats.wait_for_rsp_time);
  INC_STATS(get_thd_id(),trans_wait_for_commit_rsp_count, 1);
  if(txn_man->get_rc() == RCOK) {
    //txn_man->commit();
    commit();
  } else {
    //txn_man->abort();
    abort();
  }
  return rc;
}


RC WorkerThread::process_rqry_rsp(yield_func_t &yield, Message * msg, uint64_t cor_id) {
  DEBUG_T("RQRY_RSP %ld from %ld\n",msg->get_txn_id(),msg->get_return_id());
  assert(IS_LOCAL(msg->get_txn_id()));
#if PARAL_SUBTXN == true
  if (!txn_man->query || txn_man->abort_cnt != msg->current_abort_cnt ||
    (txn_man->query->partitions_touched.size() == 0)) return RCOK;
  txn_man->txn_stats.remote_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;
  RC rc = RCOK;
  int responses_left = txn_man->received_response(((AckMessage*)msg)->rc);
  assert(responses_left >=0);
  if (responses_left > 0) return WAIT;

  //for redt
  uint64_t curr_time = get_sys_clock();
  INC_STATS(get_thd_id(),trans_wait_for_rsp_time, curr_time - txn_man->txn_stats.wait_for_rsp_time);

  if(((QueryResponseMessage*)msg)->rc == Abort) {
    txn_man->start_abort(yield, cor_id);
    return Abort;
  }
  txn_man->send_RQRY_RSP = false;
  rc = txn_man->run_txn(yield, cor_id);
  check_if_done(rc);
#else
  txn_man->txn_stats.remote_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;

  if(((QueryResponseMessage*)msg)->rc == Abort) {
    txn_man->start_abort(yield, cor_id);
    return Abort;
  }
  txn_man->send_RQRY_RSP = false;
  RC rc = txn_man->run_txn(yield, cor_id);
  check_if_done(rc);
#endif
  return rc;
}

RC WorkerThread::process_rqry(yield_func_t &yield, Message * msg, uint64_t cor_id) { 
  DEBUG_T("RQRY %ld from %ld\n",msg->get_txn_id(), msg->get_return_id());
#if ONE_NODE_RECIEVE == 1 && defined(NO_REMOTE) && LESS_DIS_NUM == 10
#else
  M_ASSERT_V(!IS_LOCAL(msg->get_txn_id()), "RQRY local: %ld %ld/%d\n", msg->get_txn_id(),
             msg->get_txn_id() % g_node_cnt, g_node_id);
  assert(!IS_LOCAL(msg->get_txn_id()));
#endif
  RC rc = RCOK;
  msg->copy_to_txn(txn_man);

#if CC_ALG == MVCC
  txn_table.update_min_ts(get_thd_id(),txn_man->get_txn_id(),0,txn_man->get_timestamp());
#endif
#if CC_ALG == MAAT
    time_table.init(get_thd_id(),txn_man->get_txn_id());
#endif
#if USE_REPLICA
	for(int i=0;i<g_node_cnt;i++) txn_man->log_idx[i] = redo_log_buf.get_size();
#endif
  txn_man->send_RQRY_RSP = true;
  rc = txn_man->run_txn(yield, cor_id);

  // Send response
#if USE_REPLICA && (CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT3)
  assert(rc != WAIT);
#endif 

  if(rc != WAIT) {
#if USE_RDMA == CHANGE_MSG_QUEUE
    tport_man.rdma_thd_send_msg(get_thd_id(), txn_man->return_id, Message::create_message(txn_man,RQRY_RSP));
#else
#if PARAL_SUBTXN == true && CENTER_MASTER == true
#if CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT3
#if USE_REPLICA
    if(rc != Abort) {
      rc = txn_man->redo_log(yield,rc,cor_id);
      if (rc != RCOK) {
        rc = rc == NODE_FAILED ? Abort : rc;
      }
    }
    txn_man->finish_logging = true;
    txn_man->abort_cnt = msg->current_abort_cnt;
#endif
    msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,RACK_PREP),msg->return_node_id);
    DEBUG_T("Txn %ld send RACK_PREP to %ld, state %ld\n",msg->get_txn_id(), msg->get_return_id(), rc);
#endif
#else
    msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,RQRY_RSP),txn_man->return_id);
#endif
#endif
  }
  return rc;
}

RC WorkerThread::process_rqry_cont(yield_func_t &yield, Message * msg, uint64_t cor_id) {
  DEBUG("RQRY_CONT %ld\n",msg->get_txn_id());
  assert(!IS_LOCAL(msg->get_txn_id()));
  RC rc = RCOK;

  txn_man->run_txn_post_wait();
  txn_man->send_RQRY_RSP = false;
  rc = txn_man->run_txn(yield, cor_id);

  // Send response
  if(rc != WAIT) {
#if USE_RDMA == CHANGE_MSG_QUEUE
    tport_man.rdma_thd_send_msg(get_thd_id(), txn_man->return_id, Message::create_message(txn_man,RQRY_RSP));
#else
    msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,RQRY_RSP),txn_man->return_id);
#endif
  }
  return rc;
}

RC WorkerThread::process_rtxn_cont(yield_func_t &yield, Message * msg, uint64_t cor_id) {
  DEBUG("RTXN_CONT %ld\n",msg->get_txn_id());
  assert(IS_LOCAL(msg->get_txn_id()));

  txn_man->txn_stats.local_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;

  txn_man->run_txn_post_wait();
  txn_man->send_RQRY_RSP = false;
  RC rc = txn_man->run_txn(yield, cor_id);
  check_if_done(rc);
  return RCOK;
}

RC WorkerThread::process_rprepare(yield_func_t &yield, Message * msg, uint64_t cor_id) {
    DEBUG("RPREP %ld\n",msg->get_txn_id());
    RC rc = RCOK;

    // Validate transaction
    rc  = txn_man->validate(yield, cor_id);
    txn_man->set_rc(rc);
#if USE_RDMA == CHANGE_MSG_QUEUE
    tport_man.rdma_thd_send_msg(get_thd_id(), msg->return_node_id, Message::create_message(txn_man,RACK_PREP));
#else
    msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,RACK_PREP),msg->return_node_id);
#endif
    // Clean up as soon as abort is possible
    if(rc == Abort) {
      txn_man->abort(yield, cor_id);
    }

    return rc;
}

uint64_t WorkerThread::get_next_txn_id() {
  uint64_t txn_id =
      (get_node_id() + get_thd_id() * g_node_cnt) + (g_thread_cnt * g_node_cnt * _thd_txn_id);
  ++_thd_txn_id;
  return txn_id;
}

RC WorkerThread::process_rtxn( yield_func_t &yield, Message * msg, uint64_t cor_id) {
  RC rc = RCOK;
  uint64_t txn_id = UINT64_MAX;
  bool is_cl_o = msg->get_rtype() == CL_QRY_O;
#if USE_COROUTINE
  if(msg->get_rtype() == CL_QRY || msg->get_rtype() == CL_QRY_O) {
    // This is a new transaction
    // Only set new txn_id when txn first starts
    #if WORKLOAD == DA
      msg->txn_id=((DAClientQueryMessage*)msg)->trans_id;
      txn_id=((DAClientQueryMessage*)msg)->trans_id;
    #else
      txn_id = co_get_next_txn_id(cor_id);
      msg->txn_id = txn_id;
    #endif
    // Put txn in txn_table
    cor_txn_man[cor_id] = txn_table.get_transaction_manager(get_thd_id(),txn_id,0);
    
    cor_txn_man[cor_id]->register_thread(this, cor_id);
    // printf("txn_id:%ld cor_id:%ld\n", cor_txn_man[cor_id]->get_txn_id(), cor_id);
    uint64_t ready_starttime = get_sys_clock();
    bool ready = cor_txn_man[cor_id]->unset_ready();
    INC_STATS(get_thd_id(),worker_activate_txn_time,get_sys_clock() - ready_starttime);
    assert(ready);
    if (CC_ALG == WAIT_DIE) {
      #if WORKLOAD == DA //mvcc use timestamp
        if (da_stamp_tab.count(cor_txn_man[cor_id]->get_txn_id())==0)
        {
          da_stamp_tab[cor_txn_man[cor_id]->get_txn_id()]=co_get_next_ts(cor_id);
          cor_txn_man[cor_id]->set_timestamp(da_stamp_tab[cor_txn_man[cor_id]->get_txn_id()]);
        }
        else
        cor_txn_man[cor_id]->set_timestamp(da_stamp_tab[cor_txn_man[cor_id]->get_txn_id()]);
      #else
      cor_txn_man[cor_id]->set_timestamp(co_get_next_ts(cor_id));
      #endif
    }
    cor_txn_man[cor_id]->txn_stats.starttime = get_sys_clock();
    cor_txn_man[cor_id]->txn_stats.restart_starttime = cor_txn_man[cor_id]->txn_stats.starttime;
    msg->copy_to_txn(cor_txn_man[cor_id]);
    DEBUG("START %ld %f %lu\n", cor_txn_man[cor_id]->get_txn_id(),
          simulation->seconds_from_start(get_sys_clock()), cor_txn_man[cor_id]->txn_stats.starttime);
    #if WORKLOAD==DA
      if(da_start_trans_tab.count(cor_txn_man[cor_id]->get_txn_id())==0)
      {
        da_start_trans_tab.insert(cor_txn_man[cor_id]->get_txn_id());
          INC_STATS(get_thd_id(),local_txn_start_cnt,1);
      }
    #else
      INC_STATS(get_thd_id(), local_txn_start_cnt, 1);
    #endif

  } else {
    cor_txn_man[cor_id]->txn_stats.restart_starttime = get_sys_clock();
    DEBUG("RESTART %ld %f %lu\n", cor_txn_man[cor_id]->get_txn_id(),
        simulation->seconds_from_start(get_sys_clock()), cor_txn_man[cor_id]->txn_stats.starttime);
  }
    // Get new timestamps
  if(is_cc_new_timestamp()) {
    #if WORKLOAD==DA //mvcc use timestamp
      if(da_stamp_tab.count(cor_txn_man[cor_id]->get_txn_id())==0)
      {
        da_stamp_tab[cor_txn_man[cor_id]->get_txn_id()]=co_get_next_ts(cor_id);
        cor_txn_man[cor_id]->set_timestamp(da_stamp_tab[cor_txn_man[cor_id]->get_txn_id()]);
      }
      else
        cor_txn_man[cor_id]->set_timestamp(da_stamp_tab[cor_txn_man[cor_id]->get_txn_id()]);
    #else
      cor_txn_man[cor_id]->set_timestamp(co_get_next_ts(cor_id));
    #endif
  }

  #if CC_ALG == MVCC
      txn_table.update_min_ts(get_thd_id(),txn_id,0,cor_txn_man[cor_id]->get_timestamp());
  #endif

  #if CC_ALG == OCC
    #if WORKLOAD==DA
      if(da_start_stamp_tab.count(cor_txn_man[cor_id]->get_txn_id())==0)
      {
        da_start_stamp_tab[cor_txn_man[cor_id]->get_txn_id()]=co_get_next_ts(cor_id);
        cor_txn_man[cor_id]->set_start_timestamp(da_start_stamp_tab[cor_txn_man[cor_id]->get_txn_id()]);
      }
      else cor_txn_man[cor_id]->set_start_timestamp(da_start_stamp_tab[cor_txn_man[cor_id]->get_txn_id()]);
    #else
        cor_txn_man[cor_id]->set_start_timestamp(co_get_next_ts(cor_id));
    #endif
  #endif
  #if CC_ALG == MAAT
    #if WORKLOAD==DA
    if(da_start_stamp_tab.count(cor_txn_man[cor_id]->get_txn_id())==0)
    {
      da_start_stamp_tab[cor_txn_man[cor_id]->get_txn_id()]=1;
      time_table.init(get_thd_id(), cor_txn_man[cor_id]->get_txn_id());
      assert(time_table.get_lower(get_thd_id(), cor_txn_man[cor_id]->get_txn_id()) == 0);
      assert(time_table.get_upper(get_thd_id(), cor_txn_man[cor_id]->get_txn_id()) == UINT64_MAX);
      assert(time_table.get_state(get_thd_id(), cor_txn_man[cor_id]->get_txn_id()) == MAAT_RUNNING);
    }
    #else
    time_table.init(get_thd_id(),cor_txn_man[cor_id]->get_txn_id());
    assert(time_table.get_lower(get_thd_id(),cor_txn_man[cor_id]->get_txn_id()) == 0);
    assert(time_table.get_upper(get_thd_id(),cor_txn_man[cor_id]->get_txn_id()) == UINT64_MAX);
    assert(time_table.get_state(get_thd_id(),cor_txn_man[cor_id]->get_txn_id()) == MAAT_RUNNING);
    #endif
  #endif
  rc = init_phase();

  cor_txn_man[cor_id]->txn_stats.init_complete_time = get_sys_clock();
  INC_STATS(get_thd_id(),trans_init_time, cor_txn_man[cor_id]->txn_stats.init_complete_time - cor_txn_man[cor_id]->txn_stats.restart_starttime);
  INC_STATS(get_thd_id(),trans_init_count, 1);
  if (rc != RCOK) return rc;
  #if WORKLOAD == DA
    printf("thd_id:%lu stxn_id:%lu batch_id:%lu seq_id:%lu type:%c rtype:%d trans_id:%lu item:%c laststate:%lu state:%lu next_state:%lu\n",
      this->_thd_id,
      ((DAClientQueryMessage*)msg)->txn_id,
      ((DAClientQueryMessage*)msg)->batch_id,
      ((DAClientQueryMessage*)msg)->seq_id,
      type2char(((DAClientQueryMessage*)msg)->txn_type),
      ((DAClientQueryMessage*)msg)->rtype,
      ((DAClientQueryMessage*)msg)->trans_id,
      static_cast<char>('x'+((DAClientQueryMessage*)msg)->item_id),
      ((DAClientQueryMessage*)msg)->last_state,
      ((DAClientQueryMessage*)msg)->state,
      ((DAClientQueryMessage*)msg)->next_state);
    fflush(stdout);
  #endif
  // Execute transaction
  cor_txn_man[cor_id]->send_RQRY_RSP = false;
  if (is_cl_o) {
    rc = cor_txn_man[cor_id]->send_remote_request();
  } else {
    rc = cor_txn_man[cor_id]->run_txn(yield, cor_id);
  }
  co_check_if_done(rc, cor_id); 
#else
  if(msg->get_rtype() == CL_QRY || msg->get_rtype() == CL_QRY_O) {
    // This is a new transaction
    // Only set new txn_id when txn first starts
    #if WORKLOAD == DA
      msg->txn_id=((DAClientQueryMessage*)msg)->trans_id;
      txn_id=((DAClientQueryMessage*)msg)->trans_id;
    #else
      txn_id = get_next_txn_id();
      msg->txn_id = txn_id;
    #endif
    // Put txn in txn_table
    txn_man = txn_table.get_transaction_manager(get_thd_id(),txn_id,0);
#if !USE_COROUTINE
    txn_man->register_thread(this);
#endif
    uint64_t ready_starttime = get_sys_clock();
    bool ready = txn_man->unset_ready();
    INC_STATS(get_thd_id(),worker_activate_txn_time,get_sys_clock() - ready_starttime);
    assert(ready);
    if (CC_ALG == WAIT_DIE || CC_ALG == WOUND_WAIT) {
      #if WORKLOAD == DA //mvcc use timestamp
        if (da_stamp_tab.count(txn_man->get_txn_id())==0)
        {
          da_stamp_tab[txn_man->get_txn_id()]=get_next_ts();
          txn_man->set_timestamp(da_stamp_tab[txn_man->get_txn_id()]);
        }
        else
        txn_man->set_timestamp(da_stamp_tab[txn_man->get_txn_id()]);
      #else
      txn_man->set_timestamp(get_next_ts());
      #endif
    }
    txn_man->txn_stats.starttime = get_sys_clock();
    txn_man->txn_stats.restart_starttime = txn_man->txn_stats.starttime;
    msg->copy_to_txn(txn_man);
    DEBUG("START %ld %f %lu\n", txn_man->get_txn_id(),
          simulation->seconds_from_start(get_sys_clock()), txn_man->txn_stats.starttime);
    #if WORKLOAD==DA
      if(da_start_trans_tab.count(txn_man->get_txn_id())==0)
      {
        da_start_trans_tab.insert(txn_man->get_txn_id());
          INC_STATS(get_thd_id(),local_txn_start_cnt,1);
      }
    #else
      INC_STATS(get_thd_id(), local_txn_start_cnt, 1);
    #endif
  } else {
    txn_man->txn_stats.restart_starttime = get_sys_clock();
    DEBUG("RESTART %ld %f %lu\n", txn_man->get_txn_id(),
        simulation->seconds_from_start(get_sys_clock()), txn_man->txn_stats.starttime);
  }
    // Get new timestamps
  if(is_cc_new_timestamp()) {
  #if WORKLOAD==DA //mvcc use timestamp
    if(da_stamp_tab.count(txn_man->get_txn_id())==0)
    {
      da_stamp_tab[txn_man->get_txn_id()]=get_next_ts();
      txn_man->set_timestamp(da_stamp_tab[txn_man->get_txn_id()]);
    }
    else
      txn_man->set_timestamp(da_stamp_tab[txn_man->get_txn_id()]);
  #else
    txn_man->set_timestamp(get_next_ts());
  #endif
  }

#if USE_REPLICA
      txn_man->set_start_timestamp(get_next_ts());
#endif

#if CC_ALG == MVCC
    txn_table.update_min_ts(get_thd_id(),txn_id,0,txn_man->get_timestamp());
#endif

#if CC_ALG == OCC
  #if WORKLOAD==DA
    if(da_start_stamp_tab.count(txn_man->get_txn_id())==0)
    {
      da_start_stamp_tab[txn_man->get_txn_id()]=get_next_ts();
      txn_man->set_start_timestamp(da_start_stamp_tab[txn_man->get_txn_id()]);
    }
    else
      txn_man->set_start_timestamp(da_start_stamp_tab[txn_man->get_txn_id()]);
  #else
      txn_man->set_start_timestamp(get_next_ts());
  #endif
#endif
#if CC_ALG == MAAT
  #if WORKLOAD==DA
  if(da_start_stamp_tab.count(txn_man->get_txn_id())==0)
  {
    da_start_stamp_tab[txn_man->get_txn_id()]=1;
    time_table.init(get_thd_id(), txn_man->get_txn_id());
    assert(time_table.get_lower(get_thd_id(), txn_man->get_txn_id()) == 0);
    assert(time_table.get_upper(get_thd_id(), txn_man->get_txn_id()) == UINT64_MAX);
    assert(time_table.get_state(get_thd_id(), txn_man->get_txn_id()) == MAAT_RUNNING);
  }
  #else
  time_table.init(get_thd_id(),txn_man->get_txn_id());
  assert(time_table.get_lower(get_thd_id(),txn_man->get_txn_id()) == 0);
  assert(time_table.get_upper(get_thd_id(),txn_man->get_txn_id()) == UINT64_MAX);
  assert(time_table.get_state(get_thd_id(),txn_man->get_txn_id()) == MAAT_RUNNING);
  #endif
#endif
  rc = init_phase();

  txn_man->txn_stats.init_complete_time = get_sys_clock();
  INC_STATS(get_thd_id(),trans_init_time, txn_man->txn_stats.init_complete_time - txn_man->txn_stats.restart_starttime);
  INC_STATS(get_thd_id(),trans_init_count, 1);
  if (rc != RCOK) return rc;
  #if WORKLOAD == DA
    printf("thd_id:%lu stxn_id:%lu batch_id:%lu seq_id:%lu type:%c rtype:%d trans_id:%lu item:%c laststate:%lu state:%lu next_state:%lu\n",
      this->_thd_id,
      ((DAClientQueryMessage*)msg)->txn_id,
      ((DAClientQueryMessage*)msg)->batch_id,
      ((DAClientQueryMessage*)msg)->seq_id,
      type2char(((DAClientQueryMessage*)msg)->txn_type),
      ((DAClientQueryMessage*)msg)->rtype,
      ((DAClientQueryMessage*)msg)->trans_id,
      static_cast<char>('x'+((DAClientQueryMessage*)msg)->item_id),
      ((DAClientQueryMessage*)msg)->last_state,
      ((DAClientQueryMessage*)msg)->state,
      ((DAClientQueryMessage*)msg)->next_state);
    fflush(stdout);
  #endif
  // Execute transaction
#if USE_REPLICA
	for(int i=0;i<g_node_cnt;i++) txn_man->log_idx[i] = redo_log_buf.get_size();
#endif
  txn_man->send_RQRY_RSP = false;

  if (is_cl_o) {
    rc = txn_man->send_remote_request();
  } else {
    rc = txn_man->run_txn(yield, cor_id);
  }

#if USE_REPLICA && (CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT3)
  if(txn_man->get_rsp_cnt() == 0 && !txn_man->need_extra_wait()){
    // assert(false);
		assert(IS_LOCAL(txn_man->get_txn_id()));
    if(txn_man->get_rc() != Abort) {
      rc = txn_man->redo_log(yield,rc,cor_id);
      if (rc != RCOK) {
        rc = rc == NODE_FAILED ? Abort : rc;
        // return rc;
      }
    }

    //commit phase: now commit or abort
    txn_man->set_commit_timestamp(get_next_ts());
    txn_man->send_finish_messages();
    // assert(txn_man->get_rsp_cnt() == 0 && !txn_man->need_extra_wait());

    if(rc == Abort) {
      txn_man->abort(yield, cor_id);
      txn_man->update_query_status(g_node_id,OpStatus::PREP_ABORT);
      abort();
    } else {
      txn_man->commit(yield, cor_id);
      txn_man->update_query_status(g_node_id,OpStatus::COMMIT);
      commit();
    }
    return rc;
	}
#endif	

  // check_if_done(rc);
  #endif
  return rc;
}

RC WorkerThread::init_phase() {
  RC rc = RCOK;
  //m_query->part_touched[m_query->part_touched_cnt++] = m_query->part_to_access[0];
  return rc;
}

RC WorkerThread::process_log_msg(Message * msg) {
  assert(ISREPLICA);
  DEBUG("REPLICA PROCESS %ld\n",msg->get_txn_id());
  LogRecord * record = logger.createRecord(&((LogMessage*)msg)->record);
  logger.enqueueRecord(record);
  return RCOK;
}

RC WorkerThread::process_log_msg_rsp(Message * msg) {
  DEBUG("REPLICA RSP %ld\n",msg->get_txn_id());
  txn_man->repl_finished = true;
  if (txn_man->log_flushed) commit();
  return RCOK;
}

RC WorkerThread::process_log_flushed(Message * msg) {
  DEBUG("LOG FLUSHED %ld\n",msg->get_txn_id());
  if(ISREPLICA) {
#if USE_RDMA == CHANGE_MSG_QUEUE
    tport_man.rdma_thd_send_msg(get_thd_id(), GET_NODE_ID(msg->txn_id), Message::create_message(msg->txn_id, LOG_MSG_RSP));
#else
    msg_queue.enqueue(get_thd_id(), Message::create_message(msg->txn_id, LOG_MSG_RSP),
                      GET_NODE_ID(msg->txn_id));
#endif
    return RCOK;
  }

  txn_man->log_flushed = true;
  if (g_repl_cnt == 0 || txn_man->repl_finished) commit();
  return RCOK;
}

RC WorkerThread::process_rfwd(yield_func_t &yield, Message * msg, uint64_t cor_id) {
  DEBUG("RFWD (%ld,%ld)\n",msg->get_txn_id(),msg->get_batch_id());
  txn_man->txn_stats.remote_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;
  assert(CC_ALG == CALVIN);
  int responses_left = txn_man->received_response(((ForwardMessage*)msg)->rc);
  assert(responses_left >=0);
  if(txn_man->calvin_collect_phase_done()) {
    assert(ISSERVERN(txn_man->return_id));
    RC rc = txn_man->run_calvin_txn(yield, cor_id);
    if(rc == RCOK && txn_man->calvin_exec_phase_done()) {
      calvin_wrapup(yield, cor_id);
      return RCOK;
    }
  }
  return WAIT;
}

RC WorkerThread::process_calvin_rtxn(yield_func_t &yield, Message * msg, uint64_t cor_id) {
  DEBUG("START %ld %f %lu\n", txn_man->get_txn_id(),
        simulation->seconds_from_start(get_sys_clock()), txn_man->txn_stats.starttime);
  assert(ISSERVERN(txn_man->return_id));
  txn_man->txn_stats.local_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;
  // Execute
  RC rc = txn_man->run_calvin_txn(yield, cor_id);
  // if((txn_man->phase==6 && rc == RCOK) || txn_man->active_cnt == 0 || txn_man->participant_cnt ==
  // 1) {
  if(rc == RCOK && txn_man->calvin_exec_phase_done()) {
    calvin_wrapup(yield, cor_id);
  }
  return RCOK;
}

bool WorkerThread::is_cc_new_timestamp() {
  return (CC_ALG == MVCC || CC_ALG == TIMESTAMP);
}

ts_t WorkerThread::get_next_ts() {
	if (g_ts_batch_alloc) {
		if (_curr_ts % g_ts_batch_num == 0) {
			_curr_ts = glob_manager.get_ts(get_thd_id());
			_curr_ts ++;
		} else {
			_curr_ts ++;
		}
		return _curr_ts - 1;
	} else {
		_curr_ts = glob_manager.get_ts(get_thd_id());
		return _curr_ts;
	}
}

ts_t WorkerThread::co_get_next_ts(uint64_t cor_id) {
	if (g_ts_batch_alloc) {
		if (_curr_ts % g_ts_batch_num == 0) {
			_curr_ts = glob_manager.get_ts(get_thd_id());
			_curr_ts ++;
		} else {
			_curr_ts ++;
		}
		return _curr_ts - 1;
	} else {
		_curr_ts = glob_manager.get_ts(get_thd_id());
    _curr_ts = _curr_ts * COROUTINE_CNT + cor_id;
		return _curr_ts;
	}
}

bool WorkerThread::is_mine(Message* msg) {  //TODO:have some problems!
  if (((DAQueryMessage*)msg)->trans_id == get_thd_id()) {
    return true;
  }
  return false;
}

void WorkerNumThread::setup() {
}

RC WorkerNumThread::run() {
  tsetup();
  printf("Running WorkerNumThread %ld\n",_thd_id);

  // uint64_t idle_starttime = 0;
  int i = 0;
	while(!simulation->is_done()) {
    progress_stats();

    uint64_t wq_size = work_queue.get_wq_cnt();
    uint64_t tx_size = work_queue.get_txn_cnt();
    uint64_t ewq_size = work_queue.get_enwq_cnt();
    uint64_t dwq_size = work_queue.get_dewq_cnt();

    uint64_t etx_size = work_queue.get_entxn_cnt();
    uint64_t dtx_size = work_queue.get_detxn_cnt();

    work_queue.set_detxn_cnt();
    work_queue.set_dewq_cnt();
    work_queue.set_entxn_cnt();
    work_queue.set_enwq_cnt();

    INC_STATS(_thd_id,work_queue_wq_cnt[i],wq_size);
    INC_STATS(_thd_id,work_queue_tx_cnt[i],tx_size);

    INC_STATS(_thd_id,work_queue_ewq_cnt[i],ewq_size);
    INC_STATS(_thd_id,work_queue_dwq_cnt[i],dwq_size);

    INC_STATS(_thd_id,work_queue_etx_cnt[i],etx_size);
    INC_STATS(_thd_id,work_queue_dtx_cnt[i],dtx_size);
    i++;
    sleep(1);
    printf("------------%d s-----------%s--\n", i, simulation->is_warmup_done()?"exec":"warm");
    // if(idle_starttime ==0)
    //   idle_starttime = get_sys_clock();

    // if(get_sys_clock() - idle_starttime > 1000000000) {
    //   i++;
    //   idle_starttime = 0;
    // }
    //uint64_t starttime = get_sys_clock();

	}
  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
  return FINISH;
}


#if USE_REPLICA
void AsyncRedoThread::setup() {
}

RC AsyncRedoThread::run() {
  tsetup();
  printf("Running AsyncRedoThread %ld\n",_thd_id);

  uint64_t wait_time = 0;
  uint64_t start_time = get_sys_clock();

	while(!simulation->is_done()) {
    uint64_t ch = *(redo_log_buf.get_head());
    uint64_t ct = *(redo_log_buf.get_tail());
    uint64_t buf_size = redo_log_buf.get_size();
    if(ct-ch> buf_size) ct = ch+buf_size;
    if(ct > ULONG_MAX-100000){ //clear head and tail to prevent ct bigger than ULONG_MAX
      assert(false); //no need to clean, uint64_t is big enough for experiment purpose
      int max_to_subtract = min(ch/buf_size,ct/buf_size);
      uint64_t cleaned_head = ch - max_to_subtract*buf_size;
      uint64_t cleaned_tail = ct - max_to_subtract*buf_size;
      //todo: rdma cas to clean tail...
      redo_log_buf.set_head(cleaned_head);
    }
    if(ch < ct){
#if THOMAS_WRITE
      for(uint64_t i=ch;i<ct;i++){ 
        LogEntry* cur_entry = redo_log_buf.get_entry(i % buf_size);
        if(cur_entry->state == LE_ABORTED){
          cur_entry->set_flushed();
        }else if(cur_entry->state == LE_COMMITTED){
          for(int j=0;j<cur_entry->change_cnt;j++){
            if(cur_entry->change[j].is_primary) continue;
            IndexInfo* idx_info = (IndexInfo *)(rdma_global_buffer + (cur_entry->change[j].index_key) * sizeof(IndexInfo));
            row_t * tar_row = idx_info->address;
            uint64_t tar_wts= *(uint64_t*)(tar_row + sizeof(tar_row->_tid_word));
            if(cur_entry->c_ts > tar_wts){ //update data and cts
              memcpy((char *)tar_row,cur_entry->change[j].content,cur_entry->change[j].size);
              tar_row->wts = cur_entry->c_ts;
            }else {
              //do nothing 
            }
          }
          cur_entry->set_flushed();
        }else continue;
      }
#else
      //scan from head to tail, get committed_entries, logged_entries and ikey_to_cts
      LogEntry* committed_entries[ct-ch];
      LogEntry* logged_entries[ct-ch];
      uint64_t c_size = 0;
      uint64_t l_size = 0;
      unordered_map<uint64_t,uint64_t> ikey_to_cts; //index key of changies in COMMITTED entries to max cts

      for(uint64_t i=ch;i<ct;i++){ 
        LogEntry* cur_entry = redo_log_buf.get_entry(i % buf_size);
        entry_status cur_state = cur_entry->state;
        if(cur_state == LOGGED){
          logged_entries[l_size++] = cur_entry;
        }else if(cur_state == LE_COMMITTED){
          committed_entries[c_size++] = cur_entry;
          for(int j=0;j<cur_entry->change_cnt;j++){
            if(cur_entry->change[j].is_primary) continue;
            auto ret = ikey_to_cts.insert ({cur_entry->change[j].index_key,cur_entry->c_ts});
            if(!ret.second){
              uint64_t ikey = cur_entry->change[j].index_key;
              if(ikey_to_cts[ikey] < cur_entry->c_ts) ikey_to_cts[ikey] = cur_entry->c_ts;
            }
          }
        }else if(cur_state == LE_ABORTED){
          cur_entry->set_flushed();
        }
      }

      //scan logged_entries, get entries_to_wait
      LogEntry* entries_to_wait[l_size];
      uint64_t w_size = 0;
      for(int i=0;i<l_size;i++){
        for(int j=0;j<logged_entries[i]->change_cnt;j++){
          if(logged_entries[i]->change[j].is_primary) continue;
          uint64_t ikey = logged_entries[i]->change[j].index_key;
          if(ikey_to_cts.find(ikey) == ikey_to_cts.end()) continue;
          if(logged_entries[i]->s_ts >= ikey_to_cts[ikey]) continue;
          entries_to_wait[w_size++] = logged_entries[i];
          break;
        }
      }

      //wait 
      bool need_wait[w_size];
      for(int i=0;i<w_size;i++){
        need_wait[i] = true;
      }

      uint64_t need_wait_size = w_size;
      while(need_wait_size != 0 && !simulation->is_done()){
        for(int i=0;i<w_size;i++){
          if(!need_wait[i]) continue;
          entry_status cur_state = entries_to_wait[i]->state;
          if(cur_state == LOGGED) continue;
          else if(cur_state == LE_COMMITTED){
            committed_entries[c_size++] = entries_to_wait[i];
            --need_wait_size;
            need_wait[i] = false;
          }else if(cur_state == LE_ABORTED){
            entries_to_wait[i]->set_flushed();
            --need_wait_size;
            need_wait[i] = false;
          }
        }
      }

      //bubble sort committed_entries
      if(c_size>=2){
        for(uint64_t i=c_size-1;i>=1;i--){
          for(uint64_t j=0;j<i;j++){
            if(committed_entries[j]->c_ts > committed_entries[j+1]->c_ts){
              LogEntry* temp_le = committed_entries[j];
              committed_entries[j] = committed_entries[j+1];
              committed_entries[j+1] = temp_le;            
            }
          }
        }
      }

      //apply committed_entries and reset LogEntries afterwards
      for(int j=0;j<c_size;j++){
        LogEntry* entry_to_apply = committed_entries[j];
        for(int i=0;i<entry_to_apply->change_cnt;i++){
          if(entry_to_apply->change[i].is_primary) continue;
          IndexInfo* idx_info = (IndexInfo *)(rdma_global_buffer + (entry_to_apply->change[i].index_key) * sizeof(IndexInfo));
          char * tar_addr = (char *) idx_info->address;
          memcpy(tar_addr,entry_to_apply->change[i].content,entry_to_apply->change[i].size);
        }
        entry_to_apply->set_flushed();
      }
#endif

      //move head as far as able to
      for(int i=ch;i<ct;i++){
        LogEntry* cur_entry = redo_log_buf.get_entry(i % buf_size);
        if(cur_entry->state == FLUSHED){
          cur_entry->reset();
          redo_log_buf.set_head(*(redo_log_buf.get_head())+1);
        }else break;
      }     
    }
  }

  uint64_t total_time = get_sys_clock() - start_time;

  printf("FINISH %ld:%ld, final head: %ld, final tail: %ld, spend %lf time on waiting\n",_node_id,_thd_id,*(redo_log_buf.get_head()),*(redo_log_buf.get_tail()), (double)wait_time/(double)total_time);
  for(int i=0;i<g_node_cnt;i++){
    if(i!=g_node_id) printf("log_head[%d]: %lu \n", i, log_head[i]);
  }
  
  fflush(stdout);
  return FINISH;
}
#endif
