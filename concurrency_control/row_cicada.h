#ifndef ROW_CICADA_H
#define ROW_CICADA_H
#include "global.h"

class table_t;
class Catalog;
class TxnManager;

struct CARecordEntry {
	uint64_t id;
	RecordStatus status;
	ts_t wts;
	ts_t rts;
	// row_t * row;
	// char * data; // The value needs to be stored.
	CARecordEntry * next;
};

class Row_cicada {
public:
	void init(row_t * row);
	RC access(TxnManager * txn, access_t type, Access * access);
	RC validate(Access * access, ts_t ts, bool check_consis);
	uint64_t commit(TxnManager * txn, access_t type);
private:
 	pthread_mutex_t * latch;
	bool blatch;

	row_t * _row;
	CARecordEntry * get_record_ts(ts_t ts);
	CARecordEntry * get_record_id(uint64_t num);
	uint64_t buffer_record(RecordStatus type, ts_t ts);
	
	CARecordEntry * recordlist;
	uint64_t record_len;
};

#endif
