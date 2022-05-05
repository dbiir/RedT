#include "global.h"
#include "row.h"

#if USE_REPLICA 
#define CHANGE_PER_ENTRY REQ_PER_QUERY
// #define CONTENT_DEFAULT_SIZE (sizeof(row_t)+ROW_DEFAULT_SIZE)
#define CONTENT_DEFAULT_SIZE (3*sizeof(uint64_t))

enum entry_status {INIT_STATE = 0, WRITTEN, ENDED, FLUSHED};

class ChangeInfo{
public:
    // void init();
    void set_change_info(const ChangeInfo& ci);
    void set_change_info(uint64_t ikey, uint64_t s, char* cont, bool is_pri = false);

    bool is_primary; //primary replica can be skipped in AsyncRedo Thread
    uint64_t index_key; //index_key of the row index
    uint64_t size; //size of content, maximum CONTENT_DEFAULT_SIZE
    char content[CONTENT_DEFAULT_SIZE]; //content after change
};

class LogEntry {
public:
	void init(); //initialize during system startup
    void reset(); //reset after applying.
    void set_rewritable();
    void set_entry(int ccnt,const vector<ChangeInfo>& cinfo,uint64_t log_id);
    entry_status get_status();

    uint64_t logid;
    uint64_t ts;
    bool is_committed;
    bool is_aborted;
    int change_cnt;
    bool rewritable;    
    ChangeInfo change[CHANGE_PER_ENTRY];
    uint64_t logid_check;
};

class RedoLogBuffer {
public:
	void init();
    uint64_t get_size();
    uint64_t* get_head();    
    uint64_t* get_tail();
    LogEntry* get_entry(uint64_t idx, bool skip_logid = false);
    uint64_t  get_entry_offset(uint64_t idx, bool skip_logid = false);

    void set_head(uint64_t h);
    void set_tail(uint64_t t);

private:
    uint64_t log_buffer_size;
};

#endif