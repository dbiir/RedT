#include "global.h"
#include "row.h"

#if USE_REPLICA 
#if WORKLOAD== YCSB
#define CHANGE_PER_ENTRY REQ_PER_QUERY
#else 
#define CHANGE_PER_ENTRY 100
#endif
// #define CONTENT_DEFAULT_SIZE (sizeof(row_t)+ROW_DEFAULT_SIZE)
#define CONTENT_DEFAULT_SIZE (3*sizeof(uint64_t))

enum entry_status : uint8_t {EMPTY = 0, LOGGED, LE_COMMITTED, LE_ABORTED, FLUSHED};

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
    void set_flushed(); //set flushed after applying.
    void reset();
    void set_entry(uint64_t txnid, int ccnt,const vector<ChangeInfo>& cinfo, uint64_t sts);
    // entry_status get_status();

    entry_status state;
    uint64_t c_ts; //commit_timestamp
    uint64_t s_ts; //start_timestamp
    int change_cnt;
    uint64_t txn_id;
    ChangeInfo change[CHANGE_PER_ENTRY];
};

class RedoLogBuffer {
public:
	void init();
    uint64_t get_size();
    uint64_t* get_head();    
    uint64_t* get_tail();
    LogEntry* get_entry(uint64_t idx);
    uint64_t  get_entry_offset(uint64_t idx);

    void set_head(uint64_t h);
    void set_tail(uint64_t t);

private:
    uint64_t log_buffer_size;
};

#endif