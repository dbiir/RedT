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

#ifndef ROW_RDMA_MAAT_H
#define ROW_RDMA_MAAT_H
#include "routine.h"
#if CC_ALG == RDMA_MAAT

class Row_rdma_maat {
public:
	void init(row_t * row);
  RC access(access_t type, TxnManager * txn);
  RC read_and_prewrite(TxnManager * txn);
  RC read(TxnManager * txn);
  RC prewrite(TxnManager * txn);
  RC abort(yield_func_t &yield, access_t type, TxnManager * txn, uint64_t cor_id);
  RC commit(yield_func_t &yield, access_t type, TxnManager * txn, row_t * data, uint64_t cor_id);
  void write(row_t * data);
  void ucread_erase(uint64_t txn_id);
  void ucwrite_erase(uint64_t txn_id);
  bool local_cas_lock(TxnManager * txnMng , uint64_t info, uint64_t new_info);
  volatile bool maat_avail;

	row_t * _row;

};

#endif

#endif