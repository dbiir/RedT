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
#include "rdma_null.h"

#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "txn.h"

#if CC_ALG == RDMA_CNULL

RC RDMA_Null::finish(RC rc,TxnManager * txnMng){
  Transaction *txn = txnMng->txn;
  for (uint64_t i = 0; i < txn->row_cnt; i++) {
        if(txn->accesses[i]->location != g_node_id){
        //remote
       // mem_allocator.free(txn->accesses[i]->data,0);
        // mem_allocator.free(txn->accesses[i]->orig_row,0);
    
        // txn->accesses[i]->data = NULL;
        // txn->accesses[i]->orig_row = NULL;
    
        }
 }
  return RCOK;
}

#endif

