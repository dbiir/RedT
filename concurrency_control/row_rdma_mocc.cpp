#include "txn.h"
#include "row.h"
#include "row_rdma_mocc.h"
#include "mem_alloc.h"
#include "transport/rdma.h"
#include "qps/op.hh"
#include "routine.h"

#if CC_ALG==RDMA_MOCC


void
Row_rdma_mocc::init(row_t * row)
{
	_row = row;
	_tid_word = 0;
	timestamp = 0;
}

RC
Row_rdma_mocc::access(yield_func_t &yield, TxnManager * txn, TsType type, row_t * local_row, uint64_t cor_id) {
	RC rc = RCOK;
	
	// set<uint64_t>::iterator end_lock = txn->lock_set.end();
			
	if (_row->is_hot > HOT_VALUE) {
	local_retry_lock:
		uint64_t loc = g_node_id;
		uint64_t try_lock = -1;
		uint64_t lock_type = 0;
		bool conflict = false;
		uint64_t thd_id = txn->get_thd_id();
		// uint64_t *tmp_buf2 = (uint64_t *)Rdma::get_row_client_memory(thd_id);
		// auto mr = client_rm_handler->get_reg_attr().value();
		uint64_t tts = txn->get_timestamp();
        try_lock = txn->cas_remote_content(yield,loc,(char*)_row - rdma_global_buffer,0,txn->get_txn_id(),cor_id);
		if(try_lock != 0) {
			rc = Abort;
			return rc;
		}
		//!_row->is_hot++;
		if (_row->is_hot > HOT_VALUE * 10) {
			_row->is_hot = 0;
		}
		lock_type = _row->lock_type;
		if(lock_type == 0) {
            _row->lock_owner[0] = txn->get_txn_id();
            _row->ts[0] = tts;
            _row->lock_type = type == P_REQ? 1:2;
            _row->_tid_word = 0; 
			txn->lock_set.insert(_row->get_primary_key());
			txn->num_locks ++;
			// printf("add lock %d on record %d\n", _row->lock_type, _row->get_primary_key());
            rc = RCOK;
        } else if(lock_type == 1 || type == P_REQ) {
            conflict = true;
            if(conflict) {
                uint64_t i = 0;
                for(i = 0; i < LOCK_LENGTH; i++) {
                    if((tts > _row->ts[i] && _row->ts[i] != 0)) {
                        _row->_tid_word = 0;
						_row->is_hot++;
                        rc = Abort;
                        return rc;
                    }
                    // if(_row->ts[0] == 0) break;
                }
                // total_num_atomic_retry++;
                // txn->num_atomic_retry++;
                // if(txn->num_atomic_retry > max_num_atomic_retry) max_num_atomic_retry = txn->num_atomic_retry;
                //sleep(1);
                _row->_tid_word = 0;
				if (!simulation->is_done()) goto local_retry_lock;
				else {
					DEBUG_M("TxnManager::get_row(abort) access free\n");
					// row_local = NULL;
					// txn->rc = Abort;
					// mem_allocator.free(m_item, sizeof(itemid_t));
					// mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
					return Abort; //原子性被破坏，CAS失败	
				}
				// rc = Abort;
                // return rc;	
            }
        } else {
            uint64_t i = 0; 
			for(i = 0; i < LOCK_LENGTH; i++) {
				if(_row->ts[i] == 0) {
					_row->ts[i] = tts;
					_row->lock_owner[i] == txn->get_txn_id();
					_row->lock_type = type == P_REQ? 1:2;
					txn->lock_set.insert(_row->get_primary_key());
					// printf("add lock %d on record %d\n", _row->lock_type, _row->get_primary_key());
					txn->num_locks ++;
					_row->_tid_word = 0;
                    rc = RCOK;
                    break;
				}
			}
            if(i == LOCK_LENGTH) {
				_row->_tid_word = 0;
				_row->is_hot++;
                rc = Abort;
                return rc;
			}
        }
	} else {
		if (type == R_REQ) {
			DEBUG("READ %ld -- %ld, table name: %s \n",txn->get_txn_id(),_row->get_primary_key(),_row->get_table_name());
		} else if (type == P_REQ) {
			DEBUG("WRITE %ld -- %ld \n",txn->get_txn_id(),_row->get_primary_key());
		}
	}
	

	//todo : lock currenct row
	local_row->copy(_row);

	return RCOK;
}

bool
Row_rdma_mocc::validate(ts_t tid, ts_t ts , bool in_write_set) {
//   uint64_t v = _row->_tid_word;
//   printf("mocc try to validate lock %ld row %ld \n", tid, _row->get_primary_key());
//   if (v != tid && v != 0) return false;
  if(_row->timestamp != ts) {
	  _row->is_hot++;
	  return false;//the row has been rewrote
  }
  return true;
}

void
Row_rdma_mocc::write(row_t * data, uint64_t tid , ts_t time) {
  //todo : lock currenct row
	_row->copy(data);
	_row->timestamp = time;
  //_row->_tid_word = 0;
}

void
Row_rdma_mocc::lock() {
  //todo :
}

void
Row_rdma_mocc::release(yield_func_t &yield, TxnManager * txnMng , uint64_t num, uint64_t cor_id) {
#if CC_ALG == RDMA_MOCC
  	bool result = false;
	Transaction *txn = txnMng->txn;

  	uint64_t remote_offset = txn->accesses[num]->offset;
  	uint64_t loc = g_node_id;
	uint64_t thd_id = txnMng->get_thd_id() + cor_id * g_thread_cnt;
	uint64_t lock = txnMng->get_txn_id();
	uint64_t try_lock = -1;
retry_unlock:
    try_lock = txnMng->cas_remote_content(yield,loc,remote_offset,0,lock,cor_id);
	uint64_t tts = txnMng->get_timestamp();
	uint64_t lock_type;
    // assert(try_lock == lock);
	if(try_lock != 0) {
        // printf("cas retry\n");
        // goto retry_unlock;
		if (!simulation->is_done()) goto retry_unlock;
		else {
			DEBUG_M("TxnManager::get_row(abort) access free\n");
			// row_local = NULL;
			// txn->rc = Abort;
			// mem_allocator.free(m_item, sizeof(itemid_t));
			// mem_allocator.free(test_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
			// return Abort; //原子性被破坏，CAS失败	
		}
    }
	uint64_t i = 0;
    uint64_t lock_num = 0;
    for(i = 0; i < LOCK_LENGTH; i++) {
        if(_row->lock_owner[i] == txnMng->get_txn_id()) {
            _row->ts[i] = 0;
            _row->lock_owner[i] = 0;
			// printf("release lock %d on record %d\n", _row->lock_type, _row->get_primary_key());
            if(_row->lock_type == 1) {
                _row->lock_type = 0;
            }
        }
        if(_row->ts[i] != 0) {
            lock_num += 1;
        }
    }
    if(lock_num == 0) {
        _row->lock_type = 0;
    }
    _row->_tid_word = 0;
	result = true;
	txnMng->lock_set.erase(_row->get_primary_key());
  	DEBUG("silo %ld try to acquire lock %ld row %ld \n", txnMng->get_txn_id(), _row->_tid_word, _row->get_primary_key());
#else
	// assert(_tid_word == txn_id);
  __sync_bool_compare_and_swap(&_row->_tid_word, txn_id, 0);
  DEBUG("silo %ld try to release lock %ld row %ld \n", txn_id, _row->_tid_word, _row->get_primary_key());
#endif
}

bool
Row_rdma_mocc::try_lock(yield_func_t &yield, TxnManager * txnMng , uint64_t num, uint64_t cor_id)
{
	bool result = false;
	Transaction *txn = txnMng->txn;

  	uint64_t remote_offset = txn->accesses[num]->offset;
	uint64_t thd_id = txnMng->get_thd_id() + cor_id * g_thread_cnt;
	uint64_t lock = txnMng->get_txn_id();

    uint64_t try_lock = -1;
	uint64_t loc = g_node_id;
	uint64_t lock_type = 0;
	bool conflict = false;
	// uint64_t *tmp_buf2 = (uint64_t *)Rdma::get_row_client_memory(thd_id);
	// auto mr = client_rm_handler->get_reg_attr().value();
	uint64_t tts = txnMng->get_timestamp();
	try_lock = txnMng->cas_remote_content(yield,loc,remote_offset,0,lock,cor_id);
	// _row->_tid_word = lock; 
	if(try_lock != 0) {
		return false;
	}
	lock_type = _row->lock_type;
	if(lock_type == 0) {
		_row->lock_owner[0] = txnMng->get_txn_id();
		_row->ts[0] = tts;
		_row->lock_type = 1;
		_row->_tid_word = 0; 
		txnMng->lock_set.insert(_row->get_primary_key());
		txnMng->num_locks ++;
		result = true;
		return result;
	} else if(lock_type == 1) {
		conflict = true;
		if(conflict) {
			uint64_t i = 0;
			for(i = 0; i < LOCK_LENGTH; i++) {
				if((tts > _row->ts[i] && _row->ts[i] != 0)) {
					_row->_tid_word = 0;
					_row->is_hot++;
					return false;
				}
				// if(_row->ts[0] == 0) break;
			}
			_row->_tid_word = 0;
		}
	}
    
    // if(try_lock != 0)printf("lock = %ld , origin number = %ld\n",lock,try_lock);
	result = false;
    DEBUG("silo %ld try to acquire lock %ld row %ld \n", txnMng->get_txn_id(), _row->_tid_word, _row->get_primary_key());
	return result;
}

uint64_t
Row_rdma_mocc::get_tid()
{
  return _row->_tid_word;
}

#endif
