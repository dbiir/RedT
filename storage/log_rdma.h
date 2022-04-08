#include "global.h"
#include "row.h"

#if USE_REPLICA 
#define CHANGE_PER_ENTRY REQ_PER_QUERY
// #define CONTENT_DEFAULT_SIZE (sizeof(row_t)+ROW_DEFAULT_SIZE)
#define CONTENT_DEFAULT_SIZE (3*sizeof(uint64_t))

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
    void reset(); //reset after applying. after reset, move head
    void set_entry(int ccnt,const vector<ChangeInfo>& cinfo);

    bool is_committed;
    bool is_aborted;
    // bool rewritable;
    int change_cnt;
    ChangeInfo change[CHANGE_PER_ENTRY];
};

class RedoLogBuffer {
public:
	void init();
    uint64_t get_size();
    uint64_t get_head();
    uint64_t get_tail();
    void set_head(uint64_t h);
    void forward_head(uint64_t steps);
private:
    uint64_t log_buffer_size;
    uint64_t* head;
    uint64_t* tail;
    LogEntry* entries;
};

#endif