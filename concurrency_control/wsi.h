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

#ifndef _WSI_H_
#define _WSI_H_

#include "row.h"
#include "semaphore.h"


// For simplicity, the txn hisotry for OCC is oganized as follows:
// 1. history is never deleted.
// 2. hisotry forms a single directional list. 
//		history head -> hist_1 -> hist_2 -> hist_3 -> ... -> hist_n
//    The head is always the latest and the tail the youngest. 
// 	  When history is traversed, always go from head -> tail order.

class TxnManager;

class wsi_set_ent{
public:
	wsi_set_ent();
	UInt64 tn;
	TxnManager * txn;
	UInt32 set_size;
	row_t ** rows; //[MAX_WRITE_SET];
	wsi_set_ent * next;
};

class wsi {
public:
	void init();
	RC validate(TxnManager * txn);
	void gene_finish_ts(TxnManager * txn);
	void finish(RC rc, TxnManager * txn);
private:
	// parallel validation in the original OCC paper.
	RC central_validate(TxnManager * txn);
	RC get_rw_set(TxnManager * txni, wsi_set_ent * &rset, wsi_set_ent *& wset);
	void central_finish(RC rc, TxnManager * txn);
	pthread_mutex_t latch;
 	sem_t 	_semaphore;
};

#endif
