#include "global.h"
#include "log_rdma.h"

#if USE_REPLICA

void ChangeInfo::set_change_info(const ChangeInfo& ci){ //set value of "this" by "ci", 
    index_key = ci.index_key;
    size = ci.size;
    memcpy(content,ci.content,size);
}

void ChangeInfo::set_change_info(uint64_t ikey, uint64_t s, char* cont){ 
    index_key = ikey;
    size = s;
    memcpy(content,cont,size);
}


void LogEntry::set_entry(int ccnt,const vector<ChangeInfo>& cinfo){ //set value before logging
    is_committed = false;
    is_aborted = false;
    // rewritable = false; 
    change_cnt = ccnt;
    assert(cinfo.size()<=CHANGE_PER_ENTRY);
    for(int i=0;i<cinfo.size();i++)
        change[i].set_change_info(cinfo[i]);
}

void LogEntry::init(){ //initialize during system startup
    // rewritable = true;  //ready to be overwritten
    is_committed = false;
    is_aborted = false;
    change_cnt = 0;
}

void LogEntry::reset(){ 
    is_committed = false;
    is_aborted = false;
    change_cnt = 0;
}

void RedoLogBuffer::init(){
    head = (uint64_t*)rdma_log_buffer;
    *head = 0;
    tail = (uint64_t*)(rdma_log_buffer+sizeof(uint64_t));
    *tail = 0;
    entries = (LogEntry*)(rdma_log_buffer+2*sizeof(uint64_t));

    log_buffer_size = (rdma_log_size-2*sizeof(uint64_t))/(sizeof(LogEntry));    
    for(int i=0;i<log_buffer_size;i++){
        entries[i].init();
    }
}

uint64_t RedoLogBuffer::get_size(){
    return log_buffer_size;
}

uint64_t RedoLogBuffer::get_head(){
    return *head;
}

uint64_t RedoLogBuffer::get_tail(){
    return *tail;
}

void RedoLogBuffer::set_head(uint64_t h){
    *head = h;
    return;
}

void RedoLogBuffer::forward_head(uint64_t steps){
    *head = *head + steps; 
    return;
}

#endif