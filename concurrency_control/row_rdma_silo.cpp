#include "txn.h"
#include "row.h"
#include "row_rdma_silo.h"
#include "mem_alloc.h"
#include "transport/rdma.h"
#include "qps/op.hh"
#include "routine.h"

#if CC_ALG==RDMA_SILO


void
Row_rdma_silo::init(row_t * row)
{
	_row = row;
	_tid_word = 0;
	timestamp = 0;
}

RC
Row_rdma_silo::access(TxnManager * txn, TsType type, row_t * local_row) {

	if (type == R_REQ) {
		DEBUG("READ %ld -- %ld, table name: %s \n",txn->get_txn_id(),_row->get_primary_key(),_row->get_table_name());
	} else if (type == P_REQ) {
		DEBUG("WRITE %ld -- %ld \n",txn->get_txn_id(),_row->get_primary_key());
	}

  //todo : lock currenct row
  local_row->copy(_row);

	return RCOK;
}

bool
Row_rdma_silo::validate(ts_t tid, ts_t ts , bool in_write_set) {
  uint64_t v = _row->_tid_word;
  DEBUG("silo try to validate lock %ld row %ld \n", v, _row->get_primary_key());
  if (v != tid && v != 0) return false;
  if(_row->timestamp != ts)return false;//the row has been rewrote
  return true;
}

void
Row_rdma_silo::write(row_t * data, uint64_t tid , ts_t time) {
  //todo : lock currenct row
	_row->copy(data);
	_row->timestamp = time;
  //_row->_tid_word = 0;
}

void
Row_rdma_silo::lock() {
  //todo :
}

void
Row_rdma_silo::release(yield_func_t &yield, TxnManager * txnMng , uint64_t num, uint64_t cor_id) {
#if CC_ALG == RDMA_SILO
  	bool result = false;
	Transaction *txn = txnMng->txn;

  	uint64_t remote_offset = txn->accesses[num]->offset;
  	uint64_t loc = g_node_id;
	uint64_t thd_id = txnMng->get_thd_id() + cor_id * g_thread_cnt;
	uint64_t lock = txnMng->get_txn_id();

    uint64_t try_lock = txnMng->cas_remote_content(yield,loc,remote_offset,lock,0,cor_id);
    // assert(try_lock == lock);

	result = true;
  	DEBUG("silo %ld try to acquire lock %ld row %ld \n", txnMng->get_txn_id(), _row->_tid_word, _row->get_primary_key());
#else
	// assert(_tid_word == txn_id);
  __sync_bool_compare_and_swap(&_row->_tid_word, txn_id, 0);
  DEBUG("silo %ld try to release lock %ld row %ld \n", txn_id, _row->_tid_word, _row->get_primary_key());
#endif
}

bool
Row_rdma_silo::try_lock(yield_func_t &yield, TxnManager * txnMng , uint64_t num, uint64_t cor_id)
{
#if CC_ALG == RDMA_SILO
  	bool result = false;
	Transaction *txn = txnMng->txn;

  	uint64_t remote_offset = txn->accesses[num]->offset;
  	uint64_t loc = g_node_id;
	uint64_t thd_id = txnMng->get_thd_id() + cor_id * g_thread_cnt;
	uint64_t lock = txnMng->get_txn_id();

    uint64_t try_lock = txnMng->cas_remote_content(yield,loc,remote_offset,0,lock,cor_id);

	result = true;
    DEBUG("silo %ld try to acquire lock %ld row %ld \n", txnMng->get_txn_id(), _row->_tid_word, _row->get_primary_key());
	return result;

#else
	bool success = __sync_bool_compare_and_swap(&_row->_tid_word, 0, txnMng->get_txn_id());
    DEBUG("silo %ld try to acquire lock %ld row %ld \n", txnMng->get_txn_id(), _row->_tid_word, _row->get_primary_key());
	return success;
#endif
}

uint64_t
Row_rdma_silo::get_tid()
{
  return _row->_tid_word;
}

#endif
