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

//#include "mvcc.h"
#include "txn.h"
#include "row.h"
#include "manager.h"
#include "helper.h"
#include "rdma.h"
#include "rdma_mvcc.h"
#include "row_rdma_mvcc.h"
#include "mem_alloc.h"
#include "lib.hh"
#include "qps/op.hh"
#if CC_ALG == RDMA_MVCC
void rdma_mvcc::init(row_t * row) {

}

void * rdma_mvcc::local_write_back(TxnManager * txnMng , uint64_t num){
    row_t *temp_row = txnMng->txn->accesses[num]->orig_row;

     // 2.get all versions of the data item, find the oldest version, and replace it with new data to write
    int version_change;
    int last_ver = (temp_row->version_num) % HIS_CHAIN_NUM;//latest version
    if(last_ver == HIS_CHAIN_NUM -1 )version_change = 0;
    else version_change = last_ver + 1 ;//the version to be replaced

    //the version to be unlocked by txn_id
    int release_ver = txnMng->txn->accesses[num]->old_version_num % HIS_CHAIN_NUM;
    
    // 3.writes the new version, RTS, start_ts, end_ts, txn-id, etc. by ONE SIDE WRITE, and unlocks the previous version's txn-id by setting it to 0
    temp_row->version_num = temp_row->version_num +1;

    temp_row->txn_id[version_change] = 0;// txnMng->get_txn_id();
    temp_row->rts[version_change] = txnMng->txn->timestamp;
    temp_row->start_ts[version_change] = txnMng->txn->timestamp;
    temp_row->end_ts[version_change] = UINT64_MAX;

    temp_row->txn_id[release_ver] = 0;
    temp_row->end_ts[release_ver] = txnMng->txn->timestamp;
    temp_row->_tid_word = 0;//release lock
}

void * rdma_mvcc::remote_write_back(TxnManager * txnMng , uint64_t num , row_t * remote_row){
    uint64_t off = txnMng->txn->accesses[num]->offset;
 	uint64_t loc = txnMng->txn->accesses[num]->location;
#if USE_DBPA
    row_t *temp_row = remote_row;
#else
    row_t *temp_row = txnMng->read_remote_row(loc,off);
#endif
	row_t * row = txnMng->txn->accesses[num]->orig_row;
    assert(temp_row->get_primary_key() == row->get_primary_key());

    // 2.get all versions of the data item, find the oldest version, and replace it with new data to write
    int version_change;
    int last_ver = (temp_row->version_num) % HIS_CHAIN_NUM;//latest version
    if(last_ver == HIS_CHAIN_NUM -1 )version_change = 0;
    else version_change = last_ver + 1 ;//the version to be replaced

    //the version to be unlocked by txn_id
    int release_ver = txnMng->txn->accesses[num]->old_version_num % HIS_CHAIN_NUM;
    
    // 3.writes the new version, RTS, start_ts, end_ts, txn-id, etc. by ONE SIDE WRITE, and unlocks the previous version's txn-id by setting it to 0
    temp_row->version_num = temp_row->version_num +1;

    temp_row->txn_id[version_change] = 0;// txnMng->get_txn_id();
    temp_row->rts[version_change] = txnMng->txn->timestamp;
    temp_row->start_ts[version_change] = txnMng->txn->timestamp;
    temp_row->end_ts[version_change] = UINT64_MAX;

    temp_row->txn_id[release_ver] = 0;
    temp_row->end_ts[release_ver] = txnMng->txn->timestamp;

    temp_row->_tid_word = 0;//release lock
//write back
    Transaction *txn = txnMng->txn;
	uint64_t thd_id = txnMng->get_thd_id();
	uint64_t lock = txnMng->get_txn_id() + 1;
    uint64_t operate_size = row_t::get_row_size(temp_row->tuple_size);

    // char *test_buf = Rdma::get_row_client_memory(thd_id);
    // memcpy(test_buf, (char*)temp_row , operate_size);
    txnMng->write_remote_row(loc,operate_size,off,(char*)temp_row);

    mem_allocator.free(temp_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
}
/*
RC rdma_mvcc::validate_local(TxnManager * txnMng){
    RC rc = RCOK;
    //sort the read and write sets
    Transaction *txn = txnMng->txn;
    uint64_t wr_cnt = txn->write_cnt;

	int wr = 0; 
  	//int read_set[10];
	int read_set[txn->row_cnt - txn->write_cnt];
	int rd = 0;
    for (uint64_t rid = 0; rid < txn->row_cnt; rid ++) {
		if (txn->accesses[rid]->type == WR)
			txnMng->write_set[wr ++] = rid;
		else
			read_set[rd ++] = rid;
	}

    int  i = 0;
    for(i = 0;i < txn->row_cnt;i ++ ){
        uint64_t thd_id = txnMng->get_thd_id();
        uint64_t loc = txn->accesses[i]->location;
        uint64_t remote_offset = txn->accesses[i]->offset;
        uint64_t lock_num = txnMng->get_txn_id() + 1;
        if(txn->accesses[i]->location == g_node_id){//local data
            //lock
            // assert(txnMng->loop_cas_remote(loc,remote_offset,0,lock_num) == true);
            if(txnMng->cas_remote_content(loc,remote_offset,0,lock_num) != 0){
                rc = Abort;
                goto cas_fail_break;
            }

            if(txn->accesses[i]->type == RD){
                 //ff the version's txn-id is not 0 and is not equal to the current transaction, the current transaction is rolled back
                row_t *temp_row = txn->accesses[i]->orig_row;

                uint64_t change_num = 0;
                bool result = false;
                result = get_version(temp_row,&change_num,txn);
                
                if(result == false){//no suitable version
                    INC_STATS(txnMng->get_thd_id(), result_false, 1);
                    // printf("rdam_mvcc:377 version_num = %ld , txn_id = %ld %ld %ld %ld \n",temp_row->version_num, temp_row->txn_id[0],temp_row->txn_id[1],temp_row->txn_id[2],temp_row->txn_id[3]);
                    rc = Abort;
                }
                else{//check txn_id
                    if(temp_row->txn_id[change_num] != 0 && temp_row->txn_id[change_num] != txnMng->get_txn_id() + 1){
                        INC_STATS(txnMng->get_thd_id(), result_false, 1);
                        // printf("【rdma_mvcc:356】version_num = %ld , txn_id = %ld %ld %ld %ld \n",temp_row->version_num, temp_row->txn_id[0],temp_row->txn_id[1],temp_row->txn_id[2],temp_row->txn_id[3]);
                        rc = Abort;//version does not qualify
                    }
                    else{
                        //in other cases, write a modified version of the RTS unilaterally and unlock it
                        //uint64_t version = (temp_row->version_num)%HIS_CHAIN_NUM;
                        uint64_t version = change_num;
                        temp_row->rts[version] = txn->timestamp;
                        //temp_row->_tid_word = 0;
                        //Stores read data into a read set(should but not)
                    }
                }
            }
            else if(txn->accesses[i]->type == WR){
                row_t * temp_row = txn->accesses[i]->orig_row;  
                uint64_t version = (temp_row->version_num)%HIS_CHAIN_NUM;
                if(temp_row->txn_id[version] != 0 && temp_row->txn_id[version] != txnMng->get_txn_id() + 1){
                    INC_STATS(txnMng->get_thd_id(), ts_error, 1);
                    // printf("【rdma_mvcc:322】version_num = %ld , txn_id = %ld %ld %ld %ld \n",temp_row->version_num, temp_row->txn_id[0],temp_row->txn_id[1],temp_row->txn_id[2],temp_row->txn_id[3]);
                    rc = Abort;
                }
                else if(txn->timestamp <= temp_row->rts[version]){
                    INC_STATS(txnMng->get_thd_id(), ts_error, 1);
                    // printf("【rdma_mvcc:327】txn->timestamp = %ld temp_row->rts[%ld] = %ld \n",txn->timestamp,version,temp_row->rts[version]);
                    rc = Abort;
                }
                else{
                    //in other cases, write a modified version of the RTS unilaterally and unlock it
                    temp_row->txn_id[version] = txnMng->get_txn_id() + 1;
                    temp_row->rts[version] = txn->timestamp;
                  //temp_row->version_num = temp_row->version_num + 1;
                   //temp_row->_tid_word = 0;//release lock

                }
           }
            // remote_release_lock(txnMng,i);//release lock
	        // uint64_t lock_num = txnMng->get_txn_id() + 1;
            uint64_t release_lock = txnMng->cas_remote_content(loc,remote_offset,lock_num,0);
            assert(release_lock == lock_num);
        }
        else{ //remote data
            //lock in loop
            //assert(txnMng->loop_cas_remote(loc,remote_offset,0,lock_num) == true);
            if(txnMng->cas_remote_content(loc,remote_offset,0,lock_num) != 0){
                rc = Abort;
                goto cas_fail_break;
            }

            bool write_back = false;

            row_t *temp_row = txn->accesses[i]->orig_row;
            uint64_t old_lock = temp_row->_tid_word;
            uint64_t operate_size = row_t::get_row_size(temp_row->tuple_size);
            if(txn->accesses[i]->type == RD){
                //get target version
              
                uint64_t change_num = 0;
                bool result = false;
                result = get_version(temp_row,&change_num,txn);

                if(result == false){//no proper version
                    INC_STATS(txnMng->get_thd_id(), result_false, 1);
                    rc = Abort;
                }
                else{//check txn_id
                    if(temp_row->txn_id[change_num] != 0 && temp_row->txn_id[change_num] != txnMng->get_txn_id() + 1){
                        INC_STATS(txnMng->get_thd_id(), result_false, 1);
                        rc = Abort;//version dont match condition
                    }
                    else{
                        //uint64_t version = (temp_row->version_num)%HIS_CHAIN_NUM;
                        uint64_t version = change_num;
                        temp_row->rts[version] = txn->timestamp;
                        temp_row->_tid_word = 0;

                        // char *test_buf = Rdma::get_row_client_memory(thd_id);
                        // memcpy(test_buf, (char*)temp_row, operate_size);

                        assert(txnMng->write_remote_row(loc,operate_size,remote_offset,(char*)temp_row)==true);
                        write_back = true;       
                        //Stores read data into a read set(should but not)

                    }
                }
                
            }
            else if(txn->accesses[i]->type == WR){
                uint64_t version = (temp_row->version_num)%HIS_CHAIN_NUM;
                if(temp_row->txn_id[version] != 0 && temp_row->txn_id[version] != txnMng->get_txn_id() + 1){
                    INC_STATS(txnMng->get_thd_id(), ts_error, 1);
                    rc = Abort;
                }else if(txn->timestamp <= temp_row->rts[version]){
                    INC_STATS(txnMng->get_thd_id(), ts_error, 1);
                    rc = Abort;
                }else{
                    //in other cases, write a modified version of the RTS unilaterally and unlock it
                    temp_row->txn_id[version] = txnMng->get_txn_id() + 1;
                    temp_row->rts[version] = txn->timestamp;
                    //temp_row->version_num = temp_row->version_num + 1;
                    temp_row->_tid_word = 0;//release lock

                    // char *test_buf = Rdma::get_row_client_memory(thd_id);
                    // memcpy(test_buf, (char*)temp_row, operate_size);

                    assert(txnMng->write_remote_row(loc,operate_size,remote_offset,(char*)temp_row)==true);
                    write_back = true;
              
                }
            }

             //rlease lock
            if(write_back == false){
                uint64_t release_lock = -1;
                release_lock = txnMng->cas_remote_content(loc,remote_offset,lock_num,0);

                // assert(release_lock == lock_num);
            }

        }
cas_fail_break:
    if(rc == Abort)break;
   }

    return rc;
}
*/
void * rdma_mvcc::abort_release_local_lock(TxnManager * txnMng , uint64_t num){
        Transaction *txn = txnMng->txn;
        row_t * temp_row = txn->accesses[num]->orig_row;
        int version = txn->accesses[num]->old_version_num % HIS_CHAIN_NUM ;//version be locked
        //int version = temp_row->version_num % HIS_CHAIN_NUM;
        temp_row->txn_id[version] = 0;
}

void * rdma_mvcc::abort_release_remote_lock(TxnManager * txnMng , uint64_t num){
        Transaction *txn = txnMng->txn;
        row_t * temp_row = txn->accesses[num]->orig_row;
        int version = txn->accesses[num]->old_version_num % HIS_CHAIN_NUM ;//version be locked
        //int version = temp_row->version_num % HIS_CHAIN_NUM;
        temp_row->txn_id[version] = 0;

        uint64_t off = txn->accesses[num]->offset;
        uint64_t loc = txn->accesses[num]->location;
        uint64_t thd_id = txnMng->get_thd_id();
        uint64_t lock = txnMng->get_txn_id() + 1;
        uint64_t operate_size = row_t::get_row_size(temp_row->tuple_size);

        // char *test_buf = Rdma::get_row_client_memory(thd_id);
        // memcpy(test_buf, (char*)temp_row , operate_size);
        assert(txnMng->write_remote_row(loc,operate_size,off,(char*)temp_row) == true);
}

RC rdma_mvcc::finish(RC rc,TxnManager * txnMng){
    Transaction *txn = txnMng->txn;

    int wr = 0; 
  	//int read_set[10];
	int read_set[txn->row_cnt - txn->write_cnt];
	int rd = 0;
	for (uint64_t rid = 0; rid < txn->row_cnt; rid ++) {
		if (txn->accesses[rid]->type == WR)
			txnMng->write_set[wr ++] = rid;
		else
			read_set[rd ++] = rid;
	}

    uint64_t wr_cnt = txn->write_cnt;

    vector<vector<uint64_t>> remote_access(g_node_cnt);
     if(rc == Abort){
        for (uint64_t i = 0; i < wr_cnt; i++) {
           uint64_t num = txnMng->write_set[i];
           uint64_t remote_offset = txn->accesses[num]->offset;
           uint64_t loc = txn->accesses[num]->location;
	       uint64_t lock_num = txnMng->get_txn_id() + 1;
#if USE_DBPA == true
           if(loc != g_node_id) remote_access[loc].push_back(num);
           else{ //local
               abort_release_local_lock(txnMng,num);
           }
#else
           //lock in loop
           assert(txnMng->loop_cas_remote(loc,remote_offset,0,lock_num) == true);
           //release lock
           if(txn->accesses[num]->location == g_node_id){
               abort_release_local_lock(txnMng,num);
           }else{
               abort_release_remote_lock(txnMng,num);
           }
           uint64_t release_lock = txnMng->cas_remote_content(loc,remote_offset,lock_num,0);
           assert(release_lock == lock_num);
#endif
        }
#if USE_DBPA == true
        for(int i=0;i<g_node_cnt;i++){ //for the same node, batch unlock remote
            if(remote_access[i].size() > 0){
                txnMng->batch_unlock_remote(i, Abort, txnMng, remote_access);
            }
        }
#if USE_OR == true
        for(int i=0;i<g_node_cnt;i++){ //poll result
            if(remote_access[i].size() > 0){
                auto dbres1 = rc_qp[i][txnMng->get_thd_id()]->wait_one_comp();
                RDMA_ASSERT(dbres1 == IOCode::Ok);       
            }
        }
#endif 
#endif
     }
     else{ //COMMIT
       for (uint64_t i = 0; i < wr_cnt; i++) {
           uint64_t num = txnMng->write_set[i];
           uint64_t remote_offset = txn->accesses[num]->offset;
           uint64_t loc = txn->accesses[num]->location;
	       uint64_t lock_num = txnMng->get_txn_id() + 1;
           row_t* remote_row = NULL;
#if USE_DBPA == true
            if(loc !=  g_node_id){ //remote
                uint64_t try_lock;
                remote_row = txnMng->cas_and_read_remote(try_lock,loc,remote_offset,remote_offset,0,lock_num);
                while(try_lock!=0){ //lock fail
                    mem_allocator.free(remote_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
                    remote_row = txnMng->cas_and_read_remote(try_lock,loc,remote_offset,remote_offset,0,lock_num);
                }
            }
            else{ //local
                assert(txnMng->loop_cas_remote(loc,remote_offset,0,lock_num) == true);
            }
#else
           //lock in loop
           assert(txnMng->loop_cas_remote(loc,remote_offset,0,lock_num) == true);
#endif
           if(txn->accesses[num]->location == g_node_id){//local
                local_write_back(txnMng , num);
           }
           else{
                remote_write_back(txnMng , num , remote_row);
                // uint64_t release_lock = txnMng->cas_remote_content(loc,remote_offset,lock_num,0);
           }  
            // remote_release_lock(txnMng,txnMng->write_set[i]);//release lock
            //uint64_t release_lock = txnMng->cas_remote_content(loc,remote_offset,lock_num,0);
            //assert(release_lock == lock_num);
       }
    }



    for (uint64_t i = 0; i < txn->row_cnt; i++) {
        if(txn->accesses[i]->location != g_node_id){
        //remote
        mem_allocator.free(txn->accesses[i]->orig_row,0);
        txn->accesses[i]->orig_row = NULL;
        }
    }
    
    return rc;
}

#endif
