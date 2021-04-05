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

#include "row.h"
#include "txn.h"
#include "row_rdma_cicada.h"
#include "mem_alloc.h"
#include "manager.h"
#include "helper.h"
#include "rdma_cicada.h"

#if CC_ALG == RDMA_CICADA

void Row_rdma_cicada::init(row_t * row) {
	_row = row;
}

RC Row_rdma_cicada::access(access_t type, TxnManager * txn, row_t * local_row) {
	uint64_t starttime = get_sys_clock();
	
	RC rc = RCOK;
	uint64_t mtx_wait_starttime = get_sys_clock();
	while (!ATOM_CAS(_row->_tid_word, 0, 1)) {

	}
	DEBUG("READ %ld -- %ld\n", txn->get_txn_id(), _row->get_primary_key());
	uint64_t version = 0;	
	if(type == RD) {
		for(int cnt = _row->version_cnt - 1; cnt >= _row->version_cnt - 5 && cnt >= 0; cnt--) {
			int i = cnt % HIS_CHAIN_NUM;
			if(_row->cicada_version[i].Wts > txn->start_ts || _row->cicada_version[i].state == Cicada_ABORTED) {
				continue;
			}
			if(_row->cicada_version[i].state == Cicada_PENDING) {
				// --todo !---pendind need wait //
				rc = WAIT;
				while(rc == WAIT) {
					ATOM_CAS(_row->_tid_word,1,0);
					while (!ATOM_CAS(_row->_tid_word, 0, 1)) {}
					if(_row->cicada_version[i].state == Cicada_PENDING) {
						rc = WAIT;
					} else {
						rc = RCOK;
						version = _row->cicada_version[i].key;
					}
				}
			} else {
				rc = RCOK;
				version = _row->cicada_version[i].key;
			}
			
		}
	} else if(type == WR) {
		assert(_row->version_cnt > 0);
		for(int cnt = _row->version_cnt - 1; cnt >= _row->version_cnt - 5 && cnt >= 0; cnt--) {
			int i = cnt % HIS_CHAIN_NUM;
			if(_row->cicada_version[i].Wts > txn->start_ts || _row->cicada_version[i].state == Cicada_ABORTED) {
				continue;
			}
			if(_row->cicada_version[i].state == Cicada_PENDING) {
				// --todo !---pendind need wait //
				rc = WAIT;
				while(rc == WAIT) {
					ATOM_CAS(_row->_tid_word,1,0);
					while (!ATOM_CAS(_row->_tid_word, 0, 1)) {}
					if(_row->cicada_version[i].state == Cicada_PENDING) {
						rc = WAIT;
					} else {
						rc = RCOK;
						version = _row->cicada_version[i].key;
					}
				}
			} else {
				if(_row->cicada_version[i].Wts > txn->start_ts || _row->cicada_version[i].Rts > txn->start_ts) {
					rc = Abort;
				} else {
					rc = RCOK;
					version = _row->cicada_version[i].key;
				}
			}
			
		}
	}
	txn->version_num.push_back(version);
	uint64_t timespan = get_sys_clock() - starttime;
	txn->txn_stats.cc_time += timespan;
	txn->txn_stats.cc_time_short += timespan;
	ATOM_CAS(_row->_tid_word,1,0);

	return rc;
}

#endif

