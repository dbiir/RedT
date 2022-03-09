#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"
#include "rdma.h"
#include "qps/op.hh"
#include "row_rdma_dslr_no_wait.h"
#include "global.h"

#if CC_ALG == RDMA_DSLR_NO_WAIT

void Row_rdma_dslr_no_wait::init(row_t * row){
	_row = row;
}

RC Row_rdma_dslr_no_wait::lock_get(yield_func_t &yield, access_t type, TxnManager * txn, row_t * row,uint64_t cor_id) {  //本地加锁
    RC rc = RCOK;
    int wait_slice = 1 * 1000UL;//1us

    //get remote address
    uint64_t remote_address = (char*)_row - rdma_global_buffer;
    uint64_t loc = g_node_id;
    //get ticket
    uint64_t faa_result = 0;
    uint64_t add_value = 1;
    if(type == RD) {
        add_value = add_value << 16;//0x0010
    }
    else if(type == WR)add_value = 1;//0x0001

    faa_result = txn->faa_remote_content(yield,loc,remote_address,add_value,cor_id);

    //checkticket
    uint64_t read_lock = txn->get_part_num(faa_result,1);
    uint64_t write_lock = txn->get_part_num(faa_result,2);
    uint64_t read_num = txn->get_part_num(faa_result,3);
    uint64_t write_num = txn->get_part_num(faa_result,4);
    // uint64_t new_faa_result = _row->_tid_word;
    // uint64_t new_faa_read_lock = txn->get_part_num(new_faa_result,1);
    // uint64_t new_faa_write_lock = txn->get_part_num(new_faa_result,2);
    // uint64_t new_faa_read_num = txn->get_part_num(new_faa_result,3);
    // uint64_t new_faa_write_num = txn->get_part_num(new_faa_result,4);
    // if(new_faa_read_num > 30000 || new_faa_write_num > 30000)printf("try to acquire %ld %s lock ns:%ld, nx:%ld, maxs:%ld, maxx:%ld, now:ns:%ld, nx:%ld, maxs:%ld, maxx:%ld\n",row->get_primary_key(), type == RD ? "read" : "write",read_lock,write_lock,read_num,write_num,new_faa_read_lock,new_faa_write_lock,new_faa_read_num,new_faa_write_num);

    uint64_t reset_from = 0, try_lock = 0;
    uint64_t reset_from_address = (char *)_row - rdma_global_buffer + sizeof(uint64_t);
    if((read_num == count_max - 1) && (type == RD)){
        //set lock from count_max|write_num|count_max|write_num
        //             (read_lock|write_lock|read_num|write_num) to 0 
        reset_from = (count_max<<48)|(write_num<<32)|(count_max<<16)|(write_num);
        try_lock = txn->cas_remote_content(yield,loc,reset_from_address,0,reset_from,cor_id);
        // assert(try_lock == 0 && _row->_reset_from == reset_from);
    }
    else if((write_num == count_max -1) && (type == WR)){
        reset_from = (read_num<<48)|(count_max<<32)|(read_num<<16)|(count_max);
        try_lock = txn->cas_remote_content(yield,loc,reset_from_address,0,reset_from,cor_id);
        // assert(try_lock == 0 && _row->_reset_from == reset_from);
    }

    //case 1 :process lock overflow
    else if(write_num >= count_max || read_num >= count_max){
        if(type == RD) {
            add_value = 1;
            add_value = add_value << 16;
            add_value = -add_value;
        }
        else if(type == WR)add_value = -1;
        faa_result = txn->faa_remote_content(yield,loc,remote_address,add_value,cor_id);

        // uint64_t read_result = _row->_tid_word;
        // uint64_t now_read_lock = txn->get_part_num(read_result,1);
        // uint64_t now_write_lock = txn->get_part_num(read_result,2);
        // uint64_t now_read_num = txn->get_part_num(read_result,3);
        // uint64_t now_write_num = txn->get_part_num(read_result,4);
        // printf("[76]now lock is read %ld, write %ld, read_num %ld, write_num %ld\n",now_read_lock, now_write_lock,now_read_num,now_write_num);

        // printf("[71]abort due %ld to max %ld, ns:%ld, nx:%ld, maxs:%ld, maxx:%ld\n",row->get_primary_key(),faa_result, read_lock,write_lock,read_num,write_num);
        // INC_STATS(txn->get_thd_id(),overflow_abort,1);
        // return Abort;

        INC_STATS(txn->get_thd_id(),overflow_abort,1);
        // return Abort;
    #if 1
        uint64_t wait = rand() % 4;
        if (wait != 0) return Abort;//random abort

        //reset lock to zero 
        int repeat_num = 0;
        uint64_t resetlock = row->_reset_from;
        uint64_t cas_result = 0;
        if(resetlock != 0){
            // printf("[83]process overflow\n");
            while(!simulation->is_done()){
                cas_result = txn->cas_remote_content(yield,loc,remote_address,resetlock,0,cor_id);
                repeat_num ++;
                uint64_t new_read_lock = txn->get_part_num(cas_result,1);
                uint64_t new_write_lock = txn->get_part_num(cas_result,2);
                uint64_t new_read_num = txn->get_part_num(cas_result,3);
                uint64_t new_write_num = txn->get_part_num(cas_result,4);
                if(cas_result == resetlock || cas_result == 0 ||
                    new_read_num < count_max && new_write_num < count_max) {
                    // printf("[87]process overflow success\n");
                    uint64_t reset_from_address = remote_address + sizeof(uint64_t);
                    try_lock = txn->cas_remote_content(yield,loc,reset_from_address,reset_from,0,cor_id);
                    return Abort;
                }
                    
                if(repeat_num < DSLR_MAX_RETRY_TIME)continue;


                // uint64_t expect_read_lock = txn->get_part_num(resetlock,1);
                // uint64_t expect_write_lock = txn->get_part_num(resetlock,2);
                // uint64_t expect_read_num = txn->get_part_num(resetlock,3);
                // uint64_t expect_write_num = txn->get_part_num(resetlock,4);
                // printf("[117]current lock:new_read_lock = %ld, new_write_lock = %ld, new_read_num = %ld, new_write_num = %ld; ****reset lock: expect_read_lock = %ld, expect_write_lock = %ld, expect_read_num = %ld, expect_write_num = %ld\n",new_read_lock,new_write_lock,new_read_num,new_write_num,expect_read_lock,expect_write_lock,expect_read_num,expect_write_num);
                if(new_read_lock == read_lock || new_write_lock == write_lock){
                    //detect deadlock
                    if((new_read_lock == count_max && new_read_num == count_max &&type == RD) || (new_write_lock == count_max && new_write_num == count_max && type == WR)){
                        continue;
                    }
                    if((new_read_lock > count_max && new_read_num == new_read_lock &&type == RD) || (new_write_lock > count_max && new_write_num == new_write_lock && type == WR)){
                        resetlock = (new_read_lock<<48)|(new_write_lock<<32)|(new_read_num<<16)|(new_write_num);
                        continue;
                    }
                    uint64_t new_lock;
                    if(type == RD){
                        new_read_lock = read_num + 1;
                        new_lock = (new_read_lock<<48)|(write_num<<32)|(new_read_num<<16)|(new_write_num);
                    }else if(type == WR){
                        new_write_lock = write_num + 1;
                        new_lock = (read_num<<48)|(new_write_lock<<32)|(new_read_num<<16)|(new_write_num);
                    }

                    uint64_t new_result = 0;
                    new_result = txn->cas_remote_content(yield,loc,remote_address,cas_result,new_lock,cor_id);
                    // if(new_result == cas_result){//release deadlock
                    //     return Abort;
                    // }else{//release fail, retry
                    // }
                }//if(deadlock)
            }//while true
        }
    #endif
    }// else if(write_num >= count_max || read_num >= count_max)
        
    //case 2:get lock
    if(((write_lock == write_num) && (type == RD)) || //no exclusive lock
        ((write_lock == write_num) && (read_lock == read_num) && (type == WR))){//no exclusive lock and no shared lock
        // row_t * read_row = read_remote_row(yield,loc,m_item->offset,cor_id);
        // rc = preserve_access(row_local, m_item, read_row, type,read_row->get_primary_key(), loc);
        return RCOK;
    }
    //case 3:meet conflict
    else{//wait
        int repeat_num = 0; 
        while(!simulation->is_done()){
            // printf("[150]2nd while thd %ld txn %ld\n",txn->get_thd_id(),txn->get_txn_id());
            // row_t *read_row = read_remote_row(loc,remote_address);
            row_t *read_row = _row;
            uint64_t new_faa_result = read_row->_tid_word;
            repeat_num ++;
            uint64_t new_read_lock = txn->get_part_num(new_faa_result,1);
            uint64_t new_write_lock = txn->get_part_num(new_faa_result,2);
            uint64_t new_read_num = txn->get_part_num(new_faa_result,3);
            uint64_t new_write_num = txn->get_part_num(new_faa_result,4);
            //case 3.1 : ignored because of deadlock
            if((new_read_lock > read_num) || (new_write_lock > write_num)){
                rc = Abort;
                // printf("[151]abort due to %ld jump ns:%ld, nx:%ld, maxs:%ld, maxx:%ld, repeat_num%d prev:ns:%ld, nx:%ld, maxs:%ld, maxx:%ld\n",row->get_primary_key(),new_read_lock,new_write_lock,new_read_num,new_write_num,repeat_num,read_lock,write_lock,read_num,write_num);
                INC_STATS(txn->get_thd_id(),jump_abort,1);
                return rc;
            }
            //case 3.2 : get lock
            if(((new_write_lock == write_num) && (type == RD)) || //no exclusive lock
                ((new_write_lock == write_num) && (new_read_lock == read_num) && (type == WR))){//no exclusive and no share lock
                // row_t * read_row = read_remote_row(yield,loc,m_item->offset,cor_id);
                // rc = preserve_access(row_local, m_item, read_row, type,read_row->get_primary_key(), loc);
                return RCOK;
            }
            //case 3.3 : detect deadlock
            // usleep(10000);
            if(repeat_num < DSLR_MAX_RETRY_TIME) continue;
            // printf("ns:%ld, nx:%ld, maxs:%ld, maxx:%ld, repeat_num%d prev:ns:%ld, nx:%ld, maxs:%ld, maxx:%ld\n",new_read_lock,new_write_lock,new_read_num,new_write_num,repeat_num,read_lock,write_lock,read_num,write_num);
            if(new_read_lock == read_lock && new_write_lock == write_lock){
                //detect deadlock
                uint64_t new_lock;
                if(type == RD){
                    new_read_lock = read_num + 1;
                    new_lock = (new_read_lock<<48)|(write_num<<32)|(new_read_num<<16)|(new_write_num);
                }else if(type == WR){
                    new_write_lock = write_num + 1;
                    new_lock = (read_num<<48)|(new_write_lock<<32)|(new_read_num<<16)|(new_write_num);
                }
                // printf("try to handle dead lock reset:%ld, origin_lock:%ld\n",new_lock,new_faa_result);
                uint64_t new_result = 0;
                new_result = txn->cas_remote_content(yield,loc,remote_address,new_faa_result,new_lock,cor_id);
                if(new_result != new_faa_result){//fail, retry 
                }else{
                    //deadlock release but lock overflow
                    if(new_read_num >= count_max || new_write_num >= count_max){
                        // uint64_t reset_from;
                        // if(type == RD){
                        //     reset_from = (count_max<<48)|(new_write_num<<32)|(count_max<<16)|(new_write_num);
                        // }else if(type == WR){
                        //     reset_from = (new_read_num<<48)|(count_max<<32)|(new_read_num<<16)|(count_max);
                        // }
                        uint64_t reset_from = read_row->_reset_from;
                        bool success = false;
                        int cas_num = 0;
                        if (reset_from > 0) {
                            // success = txn->loop_cas_remote(yield,loc,remote_address,reset_from,0,cor_id);
                            while(!simulation->is_done()){
                                try_lock = txn->cas_remote_content(yield,loc,remote_address,reset_from,0,cor_id);
                                if (try_lock == reset_from) break;
                                uint64_t read_lock2 = txn->get_part_num(try_lock,1);
                                uint64_t write_lock2 = txn->get_part_num(try_lock,2);
                                uint64_t read_num2 = txn->get_part_num(try_lock,3);
                                uint64_t write_num2 = txn->get_part_num(try_lock,4);
                                if (write_num2 < count_max && read_num2 < count_max) break;
                                // printf("210:try to reset read_lock %ld write_lock %ld read_num %ld, write_num %ld, reset_from %ld\n",read_lock2,write_lock2,read_num2,write_num2,reset_from);
                            }
                        }
                        // try_lock = txn->cas_remote_content(yield,loc,reset_from_address,reset_from,0,cor_id);
                        // assert(_row->_reset_from == reset_from);

                        INC_STATS(txn->get_thd_id(),process_overflow,1);
                    }//if overflow
                    // uint64_t reset_read_lock = txn->get_part_num(new_lock,1);
                    // uint64_t reset_write_lock = txn->get_part_num(new_lock,2);
                    // uint64_t reset_read_num = txn->get_part_num(new_lock,3);
                    // uint64_t reset_write_num = txn->get_part_num(new_lock,4);
                    // new_read_lock = txn->get_part_num(new_faa_result,1);
                    // new_write_lock = txn->get_part_num(new_faa_result,2);
                    // new_read_num = txn->get_part_num(new_faa_result,3);
                    // new_write_num = txn->get_part_num(new_faa_result,4);
                    // printf("[205]abort due to handle dead lock %ld prev:%ld, origin_lock:%ld ns:%ld, nx:%ld, maxs:%ld, maxx:%ld, reset:ns:%ld, nx:%ld, maxs:%ld, maxx:%ld\n", _row->get_primary_key(),new_lock,new_faa_result,new_read_lock,new_write_lock,new_read_num,new_write_num,reset_read_lock,reset_write_lock,reset_read_num,reset_write_num);
                    INC_STATS(txn->get_thd_id(),deadlock_abort,1);
                    return Abort;
                }
            }//if deadlock
        }//while
    }//else wait
    return rc;
}
#endif