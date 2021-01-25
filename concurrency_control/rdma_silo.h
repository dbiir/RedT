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
#include "config.h"
//#if RMDA_SILO_OPEN
#ifndef _RDMA_SILO_H_
#define _RDMA_SILO_H_

#include "row.h"
#include "semaphore.h"
//#include "row_silo.h"

#if CC_ALG == RDMA_SILO

class TxnManager;

class silo_set_ent{
public:
	UInt32 set_size;
	row_t ** rows; //row 内存地址或偏移量
  bool ** isremote;
  Access** access;
};

class RDMA_silo {
public:
 // void init();
  RC validate_rdma_silo(TxnManager * txnMng);
  RC finish(RC rc,TxnManager * txnMng);
  // void cleanup(RC rc, TxnManager * txn);
  // void set_txn_ready(RC rc, TxnManager * txn);
  // bool is_txn_ready(TxnManager * txn);
  static bool     _pre_abort;
private:
  bool remote_try_lock(TxnManager * txnMng , uint64_t num);
  bool assert_remote_lock(TxnManager * txnMng , uint64_t num);
  bool remote_commit_write(TxnManager * txnMng , uint64_t num , row_t * data, uint64_t tid);
  void release_remote_lock(TxnManager * txn , uint64_t num);
  bool validate_rw_remote(TxnManager * txnMng , uint64_t num);
  row_t *read_remote_row(TxnManager * txn , uint64_t num);
  RC validate_key(TxnManager * txn , uint64_t num);
  // RC get_rw_set(TxnManager * txni, silo_set_ent * &rset, silo_set_ent *& wset);
  // RC validate_coor(TxnManager * txn);
  // RC validate_part(TxnManager * txn);
  // RC validate_write_set(tictoc_set_ent * wset, TxnManager * txn, uint64_t commit_ts);
  // RC validate_read_set(tictoc_set_ent * rset, TxnManager * txn, uint64_t commit_ts);
  // RC lock_write_set(tictoc_set_ent * wset, TxnManager * txn);
  // void unlock_write_set(RC rc, tictoc_set_ent * wset, TxnManager * txn);
  // void compute_commit_ts(TxnManager * txn);
 	// sem_t 	_semaphore;
};

#endif
 #endif
