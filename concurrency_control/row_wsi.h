/*
   Copyright 2016 Massachusetts Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BAWSIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef ROW_WSI_H
#define ROW_WSI_H

class table_t;
class Catalog;
class TxnManager;
struct TsReqEntry;

struct WSIReqEntry {
	TxnManager * txn;
	ts_t ts;
	ts_t starttime;
	WSIReqEntry * next;
};

struct WSIHisEntry {
	// TxnManager * txn;
	ts_t ts;
	// only for write history. The value needs to be stored.
//	char * data;
	row_t * row;
	WSIHisEntry * next;
	WSIHisEntry * prev;
};

class Row_wsi {
public:
	void init(row_t * row);
	RC access(TxnManager * txn, TsType type, row_t * row);
	void update_last_commit(uint64_t ts);

	uint64_t get_last_commit() { return lastCommit; }
private:
 	pthread_mutex_t * latch;
	bool blatch;

	row_t * _row;
	void get_lock(lock_t type, TxnManager * txn);
	void release_lock(lock_t type, TxnManager * txn);

	void insert_history(ts_t ts, TxnManager * txn, row_t * row);

	WSIReqEntry * get_req_entry();
	void return_req_entry(WSIReqEntry * entry);
	WSIHisEntry * get_his_entry();
	void return_his_entry(WSIHisEntry * entry);

	bool conflict(TsType type, ts_t ts);
	void buffer_req(TsType type, TxnManager * txn);
	WSIReqEntry * debuffer_req( TsType type, TxnManager * txn = NULL);

	row_t * clear_history(TsType type, ts_t ts);

    WSIReqEntry * prereq_mvcc;
    WSIHisEntry * readhis;
    WSIHisEntry * writehis;
	WSIHisEntry * readhistail;
	WSIHisEntry * writehistail;

	uint64_t whis_len;
	uint64_t rhis_len;
	uint64_t preq_len;

	uint64_t lastCommit;
};

#endif
