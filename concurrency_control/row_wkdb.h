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

#ifndef ROW_WKDB_H
#define ROW_WKDB_H

class table_t;
class Catalog;
class TxnManager;

struct WKDBMVReqEntry {
	TxnManager * txn;
	ts_t ts;
	ts_t starttime;
	WKDBMVReqEntry * next;
};

struct WKDBMVHisEntry {
	ts_t ts;
	// only for write history. The value needs to be stored.
//	char * data;
	row_t * row;
	WKDBMVHisEntry * next;
	WKDBMVHisEntry * prev;
};

class Row_wkdb {
public:
	void init(row_t * row);
	RC access(TsType type, TxnManager * txn, row_t * row);
	RC read_and_write(TsType type, TxnManager * txn, row_t * row);
	//   RC prewrite(TxnManager * txn);
	RC abort(access_t type, TxnManager * txn);
	RC commit(access_t type, TxnManager * txn, row_t * data);
	void write(row_t * data);

	volatile bool wkdb_avail;

private:
	row_t * _row;

public:
	std::set<uint64_t> * uncommitted_reads;
	//std::set<uint64_t> * uncommitted_writes;

	uint64_t write_trans;
	uint64_t timestamp_last_read;
	uint64_t timestamp_last_write;

// multi-verison part
private:
	pthread_mutex_t * latch;
	WKDBMVReqEntry * get_req_entry();
	void return_req_entry(WKDBMVReqEntry * entry);
	WKDBMVHisEntry * get_his_entry();
	void return_his_entry(WKDBMVHisEntry * entry);

	bool conflict(TsType type, ts_t ts);
	void buffer_req(TsType type, TxnManager * txn);
	WKDBMVReqEntry * debuffer_req( TsType type, TxnManager * txn = NULL);
	void update_buffer(TxnManager * txn);
	void insert_history( ts_t ts, row_t * row);

	row_t * clear_history(TsType type, ts_t ts);

	WKDBMVReqEntry * readreq_mvcc;
  	WKDBMVReqEntry * prereq_mvcc;
  	WKDBMVHisEntry * writehis;
	WKDBMVHisEntry * writehistail;
	uint64_t whis_len;
	uint64_t rreq_len;
	uint64_t preq_len;

};

#endif
