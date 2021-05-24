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
#ifndef _RDMA_MVCC_H_
#define _RDMA_MVCC_H_

#include "row.h"
#include "semaphore.h"
#include "row_mvcc.h"

class table_t;
class Catalog;
class TxnManager;



class rdma_mvcc {
public:
	void init(row_t * row);
	RC access(TxnManager * txn, TsType type, row_t * row);
    RC finish(yield_func_t &yield, RC rc,TxnManager * txnMng, uint64_t cor_id); 
    RC validate_local(yield_func_t &yield, TxnManager * txnMng, uint64_t cor_id);
private:
 	pthread_mutex_t * latch;
	bool blatch;

	row_t * _row;


    uint64_t remote_lock(yield_func_t &yield, TxnManager * txnMng , uint64_t num, uint64_t cor_id);
    uint64_t local_lock(TxnManager * txnMng , uint64_t num);
    void * local_write_back(TxnManager * txnMng , uint64_t num);
    void * remote_write_back(yield_func_t &yield, TxnManager * txnMng , uint64_t num , row_t* remote_row, uint64_t cor_id);
    void * abort_release_local_lock(TxnManager * txnMng , uint64_t num);
    void * abort_release_remote_lock(yield_func_t &yield, TxnManager * txnMng , uint64_t num, uint64_t cor_id);
  

	MVReqEntry * readreq_mvcc;
    MVReqEntry * prereq_mvcc;
    MVHisEntry * readhis;
    MVHisEntry * writehis;
	MVHisEntry * readhistail;
	MVHisEntry * writehistail;
	uint64_t whis_len;
	uint64_t rhis_len;
	uint64_t rreq_len;
	uint64_t preq_len;

  //  RdmaMVHis historyVer[HIS_CHAIN_NUM];
};

#endif