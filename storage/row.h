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

#ifndef _ROW_H_
#define _ROW_H_

#include <cassert>
#include "global.h"
#include "routine.h"

#define ROW_DEFAULT_SIZE 1000
#include "row_rdma_cicada.h"


#define DECL_SET_VALUE(type) void set_value(int col_id, type value);

#define SET_VALUE(type) \
	void row_t::set_value(int col_id, type value) { set_value(col_id, &value); }

#define DECL_GET_VALUE(type) void get_value(int col_id, type &value);

/*
#define GET_VALUE(type)\
	void row_t::get_value(int col_id, type & value) {\
	value = *(type *)data; \
	}
	*/
#define GET_VALUE(type)\
	void row_t::get_value(int col_id, type & value) {\
		int pos = get_schema()->get_field_index(col_id);\
	DEBUG("get_value pos %d -- %lx\n",pos,(uint64_t)this); \
		value = *(type *)&data[pos];\
	}

class table_t;
class Catalog;
class TxnManager;
class Row_lock;
class Row_mvcc;
class Row_ts;
class Row_occ;
class Row_ssi;
class Row_wsi;
class Row_maat;
class Row_specex;
class Row_dli_base;
class Row_dta;
class Row_wkdb;
class Row_tictoc;
class Row_si;
class Row_null;
class Row_silo;
class Row_rdma_silo;
class Row_rdma_mvcc;
class rdma_mvcc;
class Row_rdma_2pl;
class Row_rdma_maat;
class Row_rdma_ts1;
class Row_rdma_cicada;

//struct RdmaMVHis;

struct RdmaMVHis {
    uint64_t mutex;//lock
    uint64_t rts;
    uint64_t start_ts;
    uint64_t end_ts;
    uint64_t txn_id;
    //RTS、start_ts、end_ts、txn-id：
	char data[ROW_DEFAULT_SIZE];
};

class row_t {
public:
	static int get_row_size(int tuple_size);
	RC init(table_t * host_table, uint64_t part_id, uint64_t row_id = 0);
	RC switch_schema(table_t * host_table);
	// not every row has a manager
	void init_manager(row_t * row);
  	RC remote_copy_row(row_t* remote_row, TxnManager * txn, Access *access);

	table_t * get_table();
	Catalog * get_schema();
	const char * get_table_name();
	uint64_t get_field_cnt();
	uint64_t get_tuple_size();
	uint64_t get_row_id() { return _row_id; };

	void copy(row_t * src);

	void 		set_primary_key(uint64_t key) { _primary_key = key; };
	uint64_t 	get_primary_key() {return _primary_key; };
	uint64_t 	get_part_id() { return _part_id; };

	void set_value(int id, void * ptr);
	void set_value(int id, void * ptr, int size);
	void set_value(const char * col_name, void * ptr);
	char * get_value(int id);
	char * get_value(char * col_name);

	DECL_SET_VALUE(uint64_t);
	DECL_SET_VALUE(int64_t);
	DECL_SET_VALUE(double);
	DECL_SET_VALUE(UInt32);
	DECL_SET_VALUE(SInt32);

	DECL_GET_VALUE(uint64_t);
	DECL_GET_VALUE(int64_t);
	DECL_GET_VALUE(double);
	DECL_GET_VALUE(UInt32);
	DECL_GET_VALUE(SInt32);


	void set_data(char * data);
	char * get_data();

	void free_row();

	// for concurrency control. can be lock, timestamp etc.
	RC get_lock(access_t type, TxnManager * txn);
	RC get_ts(uint64_t &orig_wts, uint64_t &orig_rts);
	RC get_row(access_t type, TxnManager * txn, row_t *& row, uint64_t &orig_wts, uint64_t &orig_rts);
	RC get_row(access_t type, TxnManager *txn, Access *access);
	RC get_row(yield_func_t &yield,access_t type, TxnManager *txn, Access *access,uint64_t cor_id);
	RC get_row_post_wait(access_t type, TxnManager * txn, row_t *& row);
	uint64_t return_row(RC rc, access_t type, TxnManager *txn, row_t *row);
#if CC_ALG == RDMA_TS1
	uint64_t return_row(yield_func_t &yield, access_t type, TxnManager *txn, Access *access, uint64_t cor_id);
#endif
	void return_row(RC rc, access_t type, TxnManager * txn, row_t * row, uint64_t _min_commit_ts);

    #if CC_ALG == RDMA_SILO
        volatile uint64_t	_tid_word;  //lcok info ：txn_id
        ts_t 			timestamp;
        Row_rdma_silo * manager;
	#elif CC_ALG == RDMA_NO_WAIT || CC_ALG == RDMA_NO_WAIT2 || CC_ALG == RDMA_WAIT_DIE2
		volatile uint64_t _tid_word; //RDMA_NO_WAIT2: only 0 or 1; RDMA_WAIT_DIE2: only 0 or ts
		Row_rdma_2pl * manager;
	#elif CC_ALG == RDMA_MAAT
	    volatile uint64_t _tid_word;
		Row_rdma_maat * manager;
        uint64_t ucreads_len;
        uint64_t ucwrites_len;
		uint64_t uncommitted_reads[ROW_SET_LENGTH];
		uint64_t uncommitted_writes[ROW_SET_LENGTH];
		uint64_t timestamp_last_read;
		uint64_t timestamp_last_write;
	#elif CC_ALG == RDMA_CICADA
		volatile uint64_t _tid_word;
		RdmaCicadaVersion cicada_version[HIS_CHAIN_NUM];
		Row_rdma_cicada * manager;
		int64_t version_cnt;

	#elif CC_ALG == RDMA_TS1
		volatile uint64_t	mutx;
		volatile uint64_t	tid;
		ts_t wts;
    	ts_t rts;
		Row_rdma_ts1 * manager;
	#elif CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == CALVIN || CC_ALG == RDMA_CALVIN
		Row_lock * manager;
	#elif CC_ALG == TIMESTAMP
	 	Row_ts * manager;
	#elif CC_ALG == MVCC
		Row_mvcc * manager;
	#elif CC_ALG == RDMA_MVCC
        volatile uint64_t	_tid_word;
        uint64_t version_num;
        //RdmaMVHis historyVer[HIS_CHAIN_NUM];
        //char datas[HIS_CHAIN_NUM][ROW_DEFAULT_SIZE];
        uint64_t txn_id[HIS_CHAIN_NUM];
        uint64_t rts[HIS_CHAIN_NUM];
        uint64_t start_ts[HIS_CHAIN_NUM];
        uint64_t end_ts[HIS_CHAIN_NUM];
		Row_rdma_mvcc *manager;
	#elif CC_ALG == OCC || CC_ALG == BOCC || CC_ALG == FOCC
		Row_occ * manager;

	#elif CC_ALG == DLI_BASE || CC_ALG == DLI_OCC
		Row_dli_base *manager;
	#elif CC_ALG == MAAT
		Row_maat * manager;
	#elif CC_ALG == WOOKONG
		Row_wkdb * manager;
	#elif CC_ALG == TICTOC
		Row_tictoc * manager;
	#elif CC_ALG == HSTORE_SPEC
		Row_specex * manager;
	#elif CC_ALG == AVOID
		Row_avoid * manager;
	#elif CC_ALG == DLI_MVCC_OCC || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3 || CC_ALG == DLI_MVCC
		Row_si *manager;
	#elif CC_ALG == DTA
		Row_dta *manager;
	#elif CC_ALG == SSI
		Row_ssi * manager;
	#elif CC_ALG == WSI
		Row_wsi * manager;
	#elif CC_ALG == CNULL
		Row_null * manager;
	#elif CC_ALG == SILO
		Row_silo * manager;
	#endif
	int tuple_size;
	table_t * table;
	char table_name[15];
    int table_idx;
private:
	// primary key should be calculated from the data stored in the row.
	uint64_t 		_primary_key;
	uint64_t		_part_id;
	bool part_info;
	uint64_t _row_id;
public:
#if RDMA_ONE_SIDE == true// == CHANGE_MSG_QUEUE || USE_RDMA == CHANGE_TCP_ONLY
	//#if CC_ALG != RDMA_MVCC
    char data[1];
	// char data[HIS_CHAIN_NUM * sizeof(get_row_size)]
    //#endif
#else
	char * data;
#endif
};

#endif
