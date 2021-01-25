#include "txn.h"
#include "row.h"
#include "row_rdma_silo.h"
#include "mem_alloc.h"

#if CC_ALG==RDMA_SILO


void
Row_rdma_silo::init(row_t * row)
{
	_row = row;
	_tid_word = 0;
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
Row_rdma_silo::validate(ts_t tid, bool in_write_set) {
  uint64_t v = _row->_tid_word;
  if (v != tid && v != 0) return false;
  else return true;
}

void
Row_rdma_silo::write(row_t * data, uint64_t tid) {
  //todo : lock currenct row
	_row->copy(data);
  _row->_tid_word = 0;
}

void
Row_rdma_silo::lock() {
  //todo :
}

void
Row_rdma_silo::release(uint64_t txn_id) {
	// assert(_tid_word == txn_id);
	_row->_tid_word = 0;
}

bool
Row_rdma_silo::try_lock(uint64_t txn_id)
{
	__sync_bool_compare_and_swap(&_row->_tid_word, 0, txn_id);
	return true;
}

uint64_t
Row_rdma_silo::get_tid()
{
  return _row->_tid_word;
}

#endif
