#include "global.h"
#include "log_rdma.h"

#if USE_REPLICA

void ChangeInfo::set_change_info(const ChangeInfo& ci){ //set value of "this" by "ci", 
    is_primary = ci.is_primary;
    index_key = ci.index_key;
    size = ci.size;
    memcpy(content,ci.content,size);
}

void ChangeInfo::set_change_info(uint64_t ikey, uint64_t s, char* cont, bool is_pri){ 
    is_primary = is_pri;
    index_key = ikey;
    size = s;
    if(s>0) memcpy(content,cont,s);
}

void LogEntry::set_entry(int ccnt,const vector<ChangeInfo>& cinfo,uint64_t log_id){ //set value before logging
    logid = log_id; 
    ts = 0;
    is_committed = false;
    is_aborted = false;
    change_cnt = ccnt;
    assert(cinfo.size() == ccnt);
    assert(cinfo.size()>0 && cinfo.size()<=CHANGE_PER_ENTRY);
    for(int i=0;i<CHANGE_PER_ENTRY;i++){
        if(i<cinfo.size()) change[i].set_change_info(cinfo[i]);
        else change[i].set_change_info(0,0,nullptr,true);
    }
    rewritable = false;
    logid_check = log_id;
}

void LogEntry::init(){ //initialize during system startup
    logid = 0;
    is_committed = false;
    is_aborted = false;
    change_cnt = 0;
    rewritable = true;
    logid_check = 0;
}

void LogEntry::reset(){ 
    is_committed = false;
    is_aborted = false;
    change_cnt = 0;
}

void LogEntry::set_rewritable(){ 
    assert(!is_committed && !is_aborted && change_cnt==0 && !rewritable);
    // change[0].index_key = 888888;
    rewritable = true;
}

entry_status LogEntry::get_status(){    
    // if(rewritable && !ended && !change_exist) return INIT_STATE;   
    // if(logid!=logid_check) return INIT_STATE; //corrupt:write not finished yet
    while(logid!=logid_check && !simulation->is_done()){}
    if(simulation->is_done()) return INIT_STATE;

    if(rewritable){
        return INIT_STATE;
    }   

    bool ended = ((!is_committed&&!is_aborted) ? false : true);
    bool change_exist = (change_cnt==0 ? false : true);
    
    if(!ended && change_exist) return WRITTEN;
    else if(ended && change_exist) return ENDED;
    else if(!ended && !change_exist) return FLUSHED;
    else assert(false);   
}


void RedoLogBuffer::init(){
    set_head(0);
    set_tail(0);
    log_buffer_size = (rdma_log_size-2*sizeof(uint64_t))/(sizeof(LogEntry));    
    for(int i=0;i<log_buffer_size;i++){
        get_entry(i)->init();
    }    
}

uint64_t RedoLogBuffer::get_size(){
    return log_buffer_size;
}

uint64_t* RedoLogBuffer::get_head(){
    return (uint64_t*)rdma_log_buffer;
}

uint64_t* RedoLogBuffer::get_tail(){
    return (uint64_t*)(rdma_log_buffer+sizeof(uint64_t));
}

LogEntry* RedoLogBuffer::get_entry(uint64_t idx, bool skip_logid){
    assert(idx >= 0 && idx < log_buffer_size);
    if(skip_logid) return (LogEntry*)(rdma_log_buffer+2*sizeof(uint64_t)+idx*sizeof(LogEntry)+sizeof(uint64_t));
    else return (LogEntry*)(rdma_log_buffer+2*sizeof(uint64_t)+idx*sizeof(LogEntry));
}

uint64_t RedoLogBuffer::get_entry_offset(uint64_t idx, bool skip_logid){
    assert(idx >= 0 && idx < log_buffer_size);
    if(skip_logid) return rdma_buffer_size-rdma_log_size+2*sizeof(uint64_t)+idx*sizeof(LogEntry)+sizeof(uint64_t);
    else return rdma_buffer_size-rdma_log_size+2*sizeof(uint64_t)+idx*sizeof(LogEntry);
}

void RedoLogBuffer::set_head(uint64_t h){
    *(get_head()) = h;
}

void RedoLogBuffer::set_tail(uint64_t t){
    *(get_tail()) = t;
}

#endif