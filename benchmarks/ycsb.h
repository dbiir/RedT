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

#ifndef _SYNTH_BM_H_
#define _SYNTH_BM_H_

#include "wl.h"
#include "txn.h"
#include "global.h"
#include "helper.h"

class YCSBQuery;
class YCSBQueryMessage;
class ycsb_request;

enum YCSBRemTxnType {
  YCSB_0,
  YCSB_1,
  YCSB_FIN,
  YCSB_RDONE
};

class YCSBWorkload : public Workload {
public :
	RC init();
	RC init_table();
  table_t* get_table(const std::string& tbl_name);
  table_t* get_table(int tbl_idx);
	RC init_schema(const char * schema_file);
	RC get_txn_man(TxnManager *& txn_manager);
	int key_to_part(uint64_t key);
	INDEX * the_index;
	table_t * the_table;
private:
	void init_table_parallel();
	void * init_table_slice();
	static void * threadInitTable(void * This) {
		((YCSBWorkload *)This)->init_table_slice();
		return NULL;
	}
	pthread_mutex_t insert_lock;
	//  For parallel initialization
	static int next_tid;
};

class YCSBTxnManager : public TxnManager {
public:
	void init(uint64_t thd_id, Workload * h_wl);
	void reset();
	void partial_reset();
  RC acquire_locks();
	RC run_txn(yield_func_t &yield, uint64_t cor_id);
#if USE_REPLICA
	RC redo_log(yield_func_t &yield, RC status, uint64_t cor_id);
  RC redo_commit_log(yield_func_t &yield, RC status, uint64_t cor_id);
  RC redo_local_log(yield_func_t &yield,RC status, uint64_t cor_id);
  RC redo_commit_local_log(yield_func_t &yield,RC status, uint64_t cor_id);
#endif
  // RC run_co_txn(yield_func_t &yield, uint64_t cor_id);
  RC run_txn_post_wait();
	RC run_calvin_txn(yield_func_t &yield,uint64_t cor_id);
  void copy_remote_requests(YCSBQueryMessage * msg);
  void insert_failed_partition(uint64_t key) {
    failed_partition.add(key);
  }
  void update_query_status(uint64_t return_id, OpStatus status);
  void update_query_status(bool timeout_check);
  RC check_query_status(OpStatus status);

  RC resend_remote_subtxn();
private:
  void next_ycsb_state();
  RC run_txn_state(yield_func_t &yield, uint64_t cor_id);
  // RC run_co_txn_state(yield_func_t &yield, uint64_t cor_id);
  RC send_remote_one_side_request(yield_func_t &yield, ycsb_request * req,row_t *& row_local, uint64_t cor_id);
  // RC co_send_remote_one_side_request(yield_func_t &yield, ycsb_request * reqs, row_t *& row_local, uint64_t cor_id);
  RC mvcc_remote_one_side_request(ycsb_request * req,row_t *& row_local);
  RC send_maat_remote_one_side_request(ycsb_request * req,row_t *& row_local);
  RC send_timestamp_remote_one_side_request(ycsb_request * req,row_t *& row_local);
  RC ycsb_read_remote_index(yield_func_t &yield, ycsb_request * req,  itemid_t* &item, uint64_t cor_id);

  RC run_ycsb_0(yield_func_t &yield,ycsb_request * req,row_t *& row_local,uint64_t cor_id);
  RC run_ycsb_1(access_t acctype, row_t * row_local);
  RC run_ycsb(yield_func_t &yield,uint64_t cor_id);
  bool is_done();
  bool is_local_request(uint64_t idx);
  RC send_remote_request();
  RC send_remote_subtxn();
  RC send_intra_subtxn();
  RC agent_check_commit();

  row_t * row;
	YCSBWorkload * _wl;
	YCSBRemTxnType state;
  uint64_t next_record_id;
  uint64_t remote_next_center_id;
  vector<vector<uint64_t>> remote_center{g_center_cnt};
};

#endif
