#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"
#include "rdma.h"
#include "qps/op.hh"
#include "rdma_dslr_no_wait.h"
#include "row_rdma_dslr_no_wait.h"

#if CC_ALG == RDMA_DSLR_NO_WAIT
void RDMA_dslr_no_wait::remote_unlock(yield_func_t &yield,RC rc,TxnManager * txnMng , uint64_t num,uint64_t cor_id){
    // if(rc == Abort) return;
    Access *access = txnMng->txn->accesses[num];
    uint64_t off = access->offset;
    uint64_t loc = access->location;

    //unlock by featch and add
    uint64_t add_value = 1;
    add_value = add_value << 48;//0x1000

    uint64_t faa_result = 0;
    faa_result = txnMng->faa_remote_content(yield,loc,off,add_value,cor_id);//TODO - check faa result? 

    row_t * read_row = txnMng->read_remote_row(yield,loc,off,cor_id);


    // uint64_t new_faa_result = read_row->_tid_word;
    // uint64_t new_read_lock = txnMng->get_part_num(faa_result,1);
    // uint64_t new_write_lock = txnMng->get_part_num(faa_result,2);
    // uint64_t new_read_num = txnMng->get_part_num(faa_result,3);
    // uint64_t new_write_num = txnMng->get_part_num(faa_result,4);
    // uint64_t reset_read_lock = txnMng->get_part_num(new_faa_result,1);
    // uint64_t reset_write_lock = txnMng->get_part_num(new_faa_result,2);
    // uint64_t reset_read_num = txnMng->get_part_num(new_faa_result,3);
    // uint64_t reset_write_num = txnMng->get_part_num(new_faa_result,4);
    // printf("remote try to release %ld read lock ns:%ld, nx:%ld, maxs:%ld, maxx:%ld, now:ns:%ld, nx:%ld, maxs:%ld, maxx:%ld add_value:%ld\n",read_row->get_primary_key(), new_read_lock, new_write_lock, new_read_num, new_write_num, reset_read_lock, reset_write_lock, reset_read_num, reset_write_num,add_value);

    uint64_t reset_from = read_row->_reset_from, try_lock;
    bool success = false;
    int cas_num = 0;
    // if (reset_from > 0) {
    //     success = txnMng->loop_cas_remote(yield,loc,off,reset_from,0,cor_id);
    // }
    uint64_t reset_times = 0;
    if (reset_from > 0) {
        while(!simulation->is_done()){
            try_lock = txnMng->cas_remote_content(yield,loc,off,reset_from,0,cor_id);
            reset_times ++;
            if (try_lock == reset_from) break;
            uint64_t read_lock2 = txnMng->get_part_num(try_lock,1);
            uint64_t write_lock2 = txnMng->get_part_num(try_lock,2);
            uint64_t read_num2 = txnMng->get_part_num(try_lock,3);
            uint64_t write_num2 = txnMng->get_part_num(try_lock,4);
            if (write_num2 < count_max && read_num2 < count_max) break;
            // if(reset_times > 50)printf("52:try to reset read_lock %ld write_lock %ld read_num %ld, write_num %ld, reset_from %ld\n",read_lock2,write_lock2,read_num2,write_num2,reset_from);
        }
    }
    uint64_t reset_from_address = off + sizeof(uint64_t);
    try_lock = txnMng->cas_remote_content(yield,loc,reset_from_address,reset_from,0,cor_id);
    // assert(try_lock == 0 && _row->_reset_from == reset_from);
    mem_allocator.free(read_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
}

void RDMA_dslr_no_wait::unlock(yield_func_t &yield,RC rc,row_t * row , TxnManager * txnMng,uint64_t cor_id){
    // if(rc == Abort) return;
    uint64_t off = (char *)row - rdma_global_buffer;
    uint64_t loc = g_node_id;

    //unlock by featch and add
    uint64_t add_value = 1;
    add_value = add_value << 48;//0x1000
    uint64_t faa_result = 0;
    // printf("try to release lock %ld\n",faa_result);
    faa_result = txnMng->faa_remote_content(yield,loc,off,add_value,cor_id);//TODO - check faa result? 

    uint64_t reset_from = row->_reset_from, try_lock;
    bool success = false;
    int cas_num = 0;

    // uint64_t new_faa_result = row->_tid_word;
    // uint64_t new_read_lock = txnMng->get_part_num(faa_result,1);
    // uint64_t new_write_lock = txnMng->get_part_num(faa_result,2);
    // uint64_t new_read_num = txnMng->get_part_num(faa_result,3);
    // uint64_t new_write_num = txnMng->get_part_num(faa_result,4);
    // uint64_t reset_read_lock = txnMng->get_part_num(new_faa_result,1);
    // uint64_t reset_write_lock = txnMng->get_part_num(new_faa_result,2);
    // uint64_t reset_read_num = txnMng->get_part_num(new_faa_result,3);
    // uint64_t reset_write_num = txnMng->get_part_num(new_faa_result,4);
    // printf("try to release %ld read lock ns:%ld, nx:%ld, maxs:%ld, maxx:%ld, now:ns:%ld, nx:%ld, maxs:%ld, maxx:%ld add_value:%ld\n",row->get_primary_key(), new_read_lock, new_write_lock, new_read_num, new_write_num, reset_read_lock, reset_write_lock, reset_read_num, reset_write_num,add_value);

    // if (reset_from > 0) {
    //     success = txnMng->loop_cas_remote(yield,loc,off,reset_from,0,cor_id);
    // }
    uint64_t reset_times = 0;
    if (reset_from > 0) {
        while(!simulation->is_done()){
            reset_times++;
            try_lock = txnMng->cas_remote_content(yield,loc,off,reset_from,0,cor_id);
            if (try_lock == reset_from) break;
            uint64_t read_lock2 = txnMng->get_part_num(try_lock,1);
            uint64_t write_lock2 = txnMng->get_part_num(try_lock,2);
            uint64_t read_num2 = txnMng->get_part_num(try_lock,3);
            uint64_t write_num2 = txnMng->get_part_num(try_lock,4);
            if (write_num2 < count_max && read_num2 < count_max) break;
            // if(reset_times > 50)printf("102:try to reset read_lock %ld write_lock %ld read_num %ld, write_num %ld, reset_from %ld\n",read_lock2,write_lock2,read_num2,write_num2,reset_from);
        }
    }
    // printf("[109]process overflow success\n");
    uint64_t reset_from_address = off + sizeof(uint64_t);
    try_lock = txnMng->cas_remote_content(yield,loc,reset_from_address,reset_from,0,cor_id);
    // assert(try_lock == 0 && _row->_reset_from == reset_from);
    
}

void RDMA_dslr_no_wait::write_and_unlock(yield_func_t &yield,RC rc, row_t * row, row_t * data, TxnManager * txnMng,uint64_t cor_id) {
    // if(rc == Abort) return;
    uint64_t off = (char *)row - rdma_global_buffer;
    uint64_t loc = g_node_id;

    uint64_t faa_result = 0;
    uint64_t add_value = 1;
    add_value = add_value << 32;//0x0100
    faa_result = txnMng->faa_remote_content(yield,loc,off,add_value,cor_id);
    
    uint64_t reset_from = row->_reset_from, try_lock;
    bool success = false;
    int cas_num = 0;

    // uint64_t new_faa_result = row->_tid_word;
    // uint64_t new_read_lock = txnMng->get_part_num(faa_result,1);
    // uint64_t new_write_lock = txnMng->get_part_num(faa_result,2);
    // uint64_t new_read_num = txnMng->get_part_num(faa_result,3);
    // uint64_t new_write_num = txnMng->get_part_num(faa_result,4);
    // uint64_t reset_read_lock = txnMng->get_part_num(new_faa_result,1);
    // uint64_t reset_write_lock = txnMng->get_part_num(new_faa_result,2);
    // uint64_t reset_read_num = txnMng->get_part_num(new_faa_result,3);
    // uint64_t reset_write_num = txnMng->get_part_num(new_faa_result,4);
    // printf("try to release %ld write lock ns:%ld, nx:%ld, maxs:%ld, maxx:%ld, now:ns:%ld, nx:%ld, maxs:%ld, maxx:%ld\n",row->get_primary_key(), new_read_lock, new_write_lock, new_read_num, new_write_num, reset_read_lock, reset_write_lock, reset_read_num, reset_write_num);

    // if (reset_from > 0) {
    //     success = txnMng->loop_cas_remote(yield,loc,off,reset_from,0,cor_id);
    // }
    uint64_t reset_times = 0;
    if (reset_from > 0) {
        while(!simulation->is_done()){
            reset_times ++;
            try_lock = txnMng->cas_remote_content(yield,loc,off,reset_from,0,cor_id);
            if (try_lock == reset_from) break;
            uint64_t read_lock2 = txnMng->get_part_num(try_lock,1);
            uint64_t write_lock2 = txnMng->get_part_num(try_lock,2);
            uint64_t read_num2 = txnMng->get_part_num(try_lock,3);
            uint64_t write_num2 = txnMng->get_part_num(try_lock,4);
            if (write_num2 < count_max && read_num2 < count_max) break;
            // if(reset_times > 50)printf("148:try to reset read_lock %ld write_lock %ld read_num %ld, write_num %ld, reset_from %ld\n",read_lock2,write_lock2,read_num2,write_num2,reset_from);
        }
    }
    uint64_t reset_from_address = off + sizeof(uint64_t);
    try_lock = txnMng->cas_remote_content(yield,loc,reset_from_address,reset_from,0,cor_id);
    // assert(try_lock == 0 && _row->_reset_from == reset_from);
}

void RDMA_dslr_no_wait::remote_write_and_unlock(yield_func_t &yield,RC rc, TxnManager * txnMng , uint64_t num,uint64_t cor_id){
    // if(rc == Abort) return;
    Access *access = txnMng->txn->accesses[num];
    row_t *data = access->data;

    uint64_t off = access->offset + sizeof(uint64_t) * 2;
    uint64_t loc = access->location;
	uint64_t thd_id = txnMng->get_thd_id();

    uint64_t operate_size = 0;

    operate_size = row_t::get_row_size(data->tuple_size) - 2 * sizeof(uint64_t);//ignore _tid_wrd
    assert(txnMng->write_remote_row(yield,loc,operate_size,off,(char*)data + sizeof(uint64_t) + sizeof(uint64_t),cor_id) == true);//TODO - check address

    uint64_t faa_result = 0;
    off = access->offset;
    uint64_t add_value = 1;
    add_value = add_value << 32;//0x0100
    faa_result = txnMng->faa_remote_content(yield,loc,off,add_value,cor_id);

    row_t * read_row = txnMng->read_remote_row(yield,loc,off,cor_id);


    // uint64_t new_faa_result = read_row->_tid_word;
    // uint64_t new_read_lock = txnMng->get_part_num(faa_result,1);
    // uint64_t new_write_lock = txnMng->get_part_num(faa_result,2);
    // uint64_t new_read_num = txnMng->get_part_num(faa_result,3);
    // uint64_t new_write_num = txnMng->get_part_num(faa_result,4);
    // uint64_t reset_read_lock = txnMng->get_part_num(new_faa_result,1);
    // uint64_t reset_write_lock = txnMng->get_part_num(new_faa_result,2);
    // uint64_t reset_read_num = txnMng->get_part_num(new_faa_result,3);
    // uint64_t reset_write_num = txnMng->get_part_num(new_faa_result,4);
    // printf("remote try to release %ld read lock ns:%ld, nx:%ld, maxs:%ld, maxx:%ld, now:ns:%ld, nx:%ld, maxs:%ld, maxx:%ld add_value:%ld\n",read_row->get_primary_key(), new_read_lock, new_write_lock, new_read_num, new_write_num, reset_read_lock, reset_write_lock, reset_read_num, reset_write_num,add_value);

    uint64_t reset_from = read_row->_reset_from, try_lock;
    bool success = false;
    int cas_num = 0;
    // if (reset_from > 0) {
    //     success = txnMng->loop_cas_remote(yield,loc,off,reset_from,0,cor_id);
    // }
    uint64_t reset_times = 0;
    if (reset_from > 0) {
        while(!simulation->is_done()){
            reset_times ++;
            try_lock = txnMng->cas_remote_content(yield,loc,off,reset_from,0,cor_id);
            if (try_lock == reset_from) break;
            uint64_t read_lock2 = txnMng->get_part_num(try_lock,1);
            uint64_t write_lock2 = txnMng->get_part_num(try_lock,2);
            uint64_t read_num2 = txnMng->get_part_num(try_lock,3);
            uint64_t write_num2 = txnMng->get_part_num(try_lock,4);
            if (write_num2 < count_max && read_num2 < count_max) break;
            // if(reset_times > 50)printf("205:try to reset read_lock %ld write_lock %ld read_num %ld, write_num %ld, reset_from %ld\n",read_lock2,write_lock2,read_num2,write_num2,reset_from);
        }
    }
    uint64_t reset_from_address = off + sizeof(uint64_t);
    try_lock = txnMng->cas_remote_content(yield,loc,reset_from_address,reset_from,0,cor_id);
    // assert(try_lock == 0 && _row->_reset_from == reset_from);
    mem_allocator.free(read_row, row_t::get_row_size(ROW_DEFAULT_SIZE));
}

//write back and unlock
void RDMA_dslr_no_wait::finish(yield_func_t &yield,RC rc, TxnManager * txnMng,uint64_t cor_id){
	Transaction *txn = txnMng->txn;
    uint64_t starttime = get_sys_clock();
	int read_set[txn->row_cnt - txn->write_cnt];
	int cur_rd_idx = 0;
    int cur_wr_idx = 0;
    //get id of entry in readset and writeset
	for (uint64_t rid = 0; rid < txn->row_cnt; rid ++) {
		if (txn->accesses[rid]->type == WR)
			txnMng->write_set[cur_wr_idx ++] = rid;
		else
			read_set[cur_rd_idx ++] = rid;
	}
    
    vector<vector<uint64_t>> remote_access(g_node_cnt);
    for (uint64_t i = 0; i < txn->row_cnt-txn->write_cnt; i++) {//process read set
        //local
        if(txn->accesses[read_set[i]]->location == g_node_id){
            Access * access = txn->accesses[ read_set[i] ];
            unlock(yield,rc,access->orig_row, txnMng,cor_id);
        }else{
        //remote
            Access * access = txn->accesses[ read_set[i] ];
            remote_unlock(yield,rc,txnMng, read_set[i],cor_id);
        }
    }
    //for write set element,write back and release lock
    for (uint64_t i = 0; i < txn->write_cnt; i++) {
        //local
        if(txn->accesses[txnMng->write_set[i]]->location == g_node_id){
            Access * access = txn->accesses[ txnMng->write_set[i] ];
            write_and_unlock(yield,rc,access->orig_row, access->data, txnMng,cor_id); 
        }else{
        //remote
            Access * access = txn->accesses[ txnMng->write_set[i] ];
            remote_write_and_unlock(yield,rc, txnMng, txnMng->write_set[i],cor_id);
        }
    }
    

    for (uint64_t i = 0; i < txn->row_cnt; i++) {
        if(txn->accesses[i]->location != g_node_id){
        //remote
        mem_allocator.free(txn->accesses[i]->data,0);
        mem_allocator.free(txn->accesses[i]->orig_row,0);
        // mem_allocator.free(txn->accesses[i]->test_row,0);
        txn->accesses[i]->data = NULL;
        txn->accesses[i]->orig_row = NULL;
        txn->accesses[i]->orig_data = NULL;
        txn->accesses[i]->version = 0;

        //txn->accesses[i]->test_row = NULL;
        txn->accesses[i]->offset = 0;
        }
    }
	memset(txnMng->write_set, 0, 100);

}
#endif
