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

#ifndef _WORKERTHREAD_H_
#define _WORKERTHREAD_H_

#include "global.h"
#include "thread.h"
class Workload;
class Message;

class WorkerThread : public Thread {
public:
    RC run() {}
    RC run(yield_func_t &yield, uint64_t cor_id);
    RC co_run(yield_func_t &yield, uint64_t cor_id);
    void setup();
    void statqueue(uint64_t thd_id, Message * msg, uint64_t starttime);
    void process(yield_func_t &yield, Message * msg, uint64_t cor_id);
    void fakeprocess(yield_func_t &yield, Message * msg, uint64_t cor_id);
    void check_if_done(RC rc);
    void release_txn_man();
    void commit();
    void abort();
    TxnManager * get_transaction_manager(Message * msg);
    void calvin_wrapup(yield_func_t &yield, uint64_t cor_id);
    RC process_rfin(yield_func_t &yield, Message * msg, uint64_t cor_id);
    RC process_rfwd(yield_func_t &yield, Message * msg, uint64_t cor_id);
    RC process_rack_rfin(Message * msg);
    RC process_rack_prep(yield_func_t &yield, Message * msg, uint64_t cor_id);
    RC process_rqry_rsp(yield_func_t &yield, Message * msg, uint64_t cor_id);
    RC process_rqry(yield_func_t &yield, Message * msg, uint64_t cor_id);
    RC process_rqry_cont(yield_func_t &yield, Message * msg, uint64_t cor_id);
    RC process_rinit(Message * msg);
    RC process_rprepare(yield_func_t &yield, Message * msg, uint64_t cor_id);
    RC process_rpass(Message * msg);
    // RC process_rtxn(Message * msg);
    RC process_rtxn(yield_func_t &yield, Message * msg, uint64_t cor_id);
    RC process_calvin_rtxn(yield_func_t &yield, Message * msg, uint64_t cor_id);
    RC process_rtxn_cont(yield_func_t &yield, Message * msg, uint64_t cor_id);
    RC process_log_msg(Message * msg);
    RC process_log_msg_rsp(Message * msg);
    RC process_log_flushed(Message * msg);
    RC init_phase();
    uint64_t get_next_txn_id();
    bool is_cc_new_timestamp();
    bool is_mine(Message* msg);
    void no_routines();

    void create_routines(int coroutines);
    void master_routine(yield_func_t &yield, int cor_id);
    void co_check_if_done(RC rc, uint64_t cor_id);
    void co_release_txn_man(uint64_t cor_id);
    void co_commit(uint64_t cor_id);
    void co_abort(uint64_t cor_id);
    uint64_t co_get_next_txn_id(uint64_t cor_id);
    void start_routine() {
        printf("start routine 0\n");
        _routines[0]();
    }
    u_int64_t _cor_id = 0;
    
    int total_worker_coroutine = 0;


private:
#if USE_COROUTINE
    uint64_t _cor_txn_id[COROUTINE_CNT + 1];
    TxnManager * cor_txn_man[COROUTINE_CNT + 1];
// #else
#endif
    uint64_t _thd_txn_id;
    TxnManager * txn_man;
// #endif
    ts_t        _curr_ts;
    ts_t        get_next_ts();
};

class WorkerNumThread : public Thread {
public:
    RC run();
    void setup();

};
#endif
