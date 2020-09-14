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

#ifndef _TICTOC_H_
#define _TICTOC_H_

#include "row.h"
#include "semaphore.h"


class TxnManager;

class tictoc_set_ent{
public:
	tictoc_set_ent();
	UInt64 tn;
	TxnManager * txn;
	UInt32 set_size;
	row_t ** rows; //[MAX_WRITE_SET];
  Access** access;
	tictoc_set_ent * next;
};

class Tictoc {
public:
  void init();
  RC validate(TxnManager * txn);
  void cleanup(RC rc, TxnManager * txn);
  void set_txn_ready(RC rc, TxnManager * txn);
  bool is_txn_ready(TxnManager * txn);
  static bool     _pre_abort;
private:
  RC get_rw_set(TxnManager * txni, tictoc_set_ent * &rset, tictoc_set_ent *& wset);
  RC validate_coor(TxnManager * txn);
  RC validate_part(TxnManager * txn);
  RC validate_write_set(tictoc_set_ent * wset, TxnManager * txn, uint64_t commit_ts);
  RC validate_read_set(tictoc_set_ent * rset, TxnManager * txn, uint64_t commit_ts);
  RC lock_write_set(tictoc_set_ent * wset, TxnManager * txn);
  void unlock_write_set(RC rc, tictoc_set_ent * wset, TxnManager * txn);
  void compute_commit_ts(TxnManager * txn);
 	// sem_t 	_semaphore;
};

#endif
