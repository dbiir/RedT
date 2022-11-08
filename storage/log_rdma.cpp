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

void LogEntry::set_entry(int ccnt,const vector<ChangeInfo>& cinfo, uint64_t sts){ //set value before logging
    assert(sts != UINT64_MAX && sts != 0);

    state = LOGGED;
    c_ts = 0;
    s_ts = sts; 
    change_cnt = ccnt;
    if (ccnt != 0) {
        assert(cinfo.size() == ccnt);
        assert(cinfo.size()>0 && cinfo.size()<=CHANGE_PER_ENTRY);
        for(int i=0;i<CHANGE_PER_ENTRY;i++){
            if(i<cinfo.size()) change[i].set_change_info(cinfo[i]);
            else change[i].set_change_info(0,0,nullptr,true);
        }
    } 
}

void LogEntry::init(){ //initialize during system startup    
    state = EMPTY;
    c_ts = 0;
    s_ts = 0;
    change_cnt = 0;
}

void LogEntry::set_flushed(){ 
    state = FLUSHED;
    c_ts = 0;
    s_ts = 0;
    change_cnt = 0;
}

void LogEntry::reset(){ 
    // change[0].index_key = 888888;
    assert(state == FLUSHED && c_ts == 0 && s_ts == 0 && change_cnt == 0);
    state = EMPTY;
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

LogEntry* RedoLogBuffer::get_entry(uint64_t idx){
    assert(idx >= 0 && idx < log_buffer_size);
    return (LogEntry*)(rdma_log_buffer+2*sizeof(uint64_t)+idx*sizeof(LogEntry));
}

uint64_t RedoLogBuffer::get_entry_offset(uint64_t idx){
    assert(idx >= 0 && idx < log_buffer_size);
    return rdma_buffer_size-rdma_log_size+2*sizeof(uint64_t)+idx*sizeof(LogEntry);
}

void RedoLogBuffer::set_head(uint64_t h){
    *(get_head()) = h;
}

void RedoLogBuffer::set_tail(uint64_t t){
    *(get_tail()) = t;
}

#endif