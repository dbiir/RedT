#ifndef _DA_H_
#define _DA_H_
#include "config.h"
#include "query.h"
#include "row.h"
#include "txn.h"
#include "wl.h"
#include "creator.h"

class DAQuery;
class DAQueryMessage;
struct Item_no;

class table_t;
class INDEX;
class DAQuery;

class DAWorkload : public Workload {
 public:
  RC init();
  RC init_table();
  table_t* get_table(const std::string& tbl_name);
  table_t* get_table(int tbl_idx);
  RC init_schema(const char* schema_file);
  RC get_txn_man(TxnManager*& txn_manager);
  void reset_tab_idx();
  table_t* t_datab;
	uint64_t nextstate;
  INDEX* i_datab;
  bool** delivering;

 private:
  //void init_tab_DAtab(int id, uint64_t w_id);
  void init_tab_DAtab();
  static void* threadInitDAtab(void* This);
};

struct DA_thr_args {
  DAWorkload* wl;
  UInt32 id;
  UInt32 tot;
};

class DATxnManager : public TxnManager {
 public:
  void init(uint64_t thd_id, Workload* h_wl);
  void reset();
  RC acquire_locks();
  RC run_txn(yield_func_t &yield, uint64_t cor_id);
  RC run_txn_post_wait();
  RC run_calvin_txn(yield_func_t &yield,uint64_t cor_id);
#if USE_REPLICA
	RC redo_log(yield_func_t &yield, RC status, uint64_t cor_id);
  RC redo_commit_log(yield_func_t &yield, RC status, uint64_t cor_id){}
#endif
  void copy_remote_items(DAQueryMessage* msg);

 private:
  DAWorkload* _wl;
  volatile RC _rc;
  row_t* row;

  uint64_t next_item_id;

  bool is_done();
  bool is_local_item(uint64_t idx);
  RC send_remote_request() {return RCOK;}
  RC run_delivery(DAQuery* query);
  void insert_failed_partition(uint64_t key) {
    failed_partition.add(key);
  }
  void update_query_status(uint64_t return_id, OpStatus status){}
  void update_query_status(bool timeout_check) {}
  RC check_query_status(OpStatus status){}

  RC resend_remote_subtxn(){}
  RC agent_check_commit(){}
};
#endif
