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
#include "routine.h"
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
  RC validate_rdma_silo(yield_func_t &yield, TxnManager * txnMng, uint64_t cor_id);
  RC finish(yield_func_t &yield,RC rc,TxnManager * txnMng, uint64_t cor_id);
  
  static bool     _pre_abort;
private:
    bool validate_rw_remote(yield_func_t &yield,TxnManager * txnMng , uint64_t num, uint64_t cor_id);
    bool remote_commit_write(yield_func_t &yield, TxnManager * txnMng , uint64_t num , row_t * data , ts_t time, uint64_t cor_id);

};

#endif
 #endif
