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

#ifndef ROW_RDMA_CICADA_H
#define ROW_RDMA_CICADA_H

#if CC_ALG == RDMA_CICADA
enum CicadaState {
  Cicada_PENDING = 0,
  Cicada_COMMITTED,
  Cicada_ABORTED
};
struct RdmaCicadaVersion{
	uint64_t key;
	uint64_t Rts;
	uint64_t Wts;
	CicadaState state;
  // char data[ROW_DEFAULT_SIZE];

	void init(uint64_t key, uint64_t rts, uint64_t wts) {
		this->key = key;
		Rts = rts;
		Wts = wts;
		state = Cicada_PENDING;
	}
};

class Row_rdma_cicada {
public:
	void init(row_t * row);
  RC access(yield_func_t &yield, access_t type, TxnManager * txn, row_t * local_row, uint64_t cor_id);
  RC abort(uint64_t num, TxnManager * txn);
  RC commit(uint64_t num, TxnManager * txn, row_t * data);
  void write(row_t * data);
  bool local_cas_lock(TxnManager * txnMng , uint64_t info, uint64_t new_info);
	row_t * _row;
};

#endif

#endif