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
    RC finish(RC rc,TxnManager * txnMng); 
    RC validate_local(TxnManager * txnMng);
    RC lock_row(TxnManager * txnMng);
private:
 	pthread_mutex_t * latch;
	bool blatch;

	row_t * _row;
	MVReqEntry * get_req_entry();
	void return_req_entry(MVReqEntry * entry);
	MVHisEntry * get_his_entry();
	void return_his_entry(MVHisEntry * entry);

	bool conflict(TsType type, ts_t ts);
	void buffer_req(TsType type, TxnManager * txn);
	MVReqEntry * debuffer_req( TsType type, TxnManager * txn = NULL);
	void update_buffer(TxnManager * txn);
	void insert_history( ts_t ts, row_t * row);

    
    uint64_t remote_lock(TxnManager * txnMng , uint64_t num);
    uint64_t local_lock(TxnManager * txnMng , uint64_t num);
    uint64_t lock_write_set(TxnManager * txnMng , uint64_t num);
    row_t * read_remote_row(TxnManager * txnMng , uint64_t num);
    void * remote_write_back(TxnManager * txnMng , uint64_t num);
    void * remote_release_lock(TxnManager * txnMng , uint64_t num);
  

	row_t * clear_history(TsType type, ts_t ts);

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