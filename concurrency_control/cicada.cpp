#include "global.h"
#include "helper.h"
#include "txn.h"
#include "cicada.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row_cicada.h"

#if CC_ALG == CICADA
void Cicada::init() {
  	sem_init(&_semaphore, 0, 1);
}

RC Cicada::validate(TxnManager * txnMan) {
    uint64_t starttime = get_sys_clock();
    assert(CC_ALG == CICADA);
	RC rc = RCOK;
	Transaction * txn = txnMan->txn;
	uint64_t row_cnt = txn->accesses.get_count();
	assert(row_cnt == txn->row_cnt);
	DEBUG("Validate: txn = %ld\n", txn->txn_id);
    sem_wait(&_semaphore);
	bool check_consis = false;
	Access * access;
	row_t * orig_r;
	ts_t ts = txnMan->get_timestamp();
	// Wset: insert pending version
	for (int rid = row_cnt - 1; rid >= 0; rid --) {
		access = txn->accesses[rid];
		orig_r = access->orig_row;
		if (access->type == WR)
			rc = orig_r->manager->validate(access, ts, check_consis);
		if (rc == Abort) goto end;
	}
	// Update read timestamp
	for (int rid = row_cnt - 1; rid >= 0; rid --) {
		access = txn->accesses[rid];
		orig_r = access->orig_row;
		if (access->type == RD)
			rc = orig_r->manager->validate(access, ts, check_consis);
		if (rc == Abort) goto end;
	}
	check_consis = true;
	// Check version consistency
	for (int rid = row_cnt - 1; rid >= 0; rid --) {
		access = txn->accesses[rid];
		orig_r = access->orig_row;
		rc = orig_r->manager->validate(access, ts, check_consis);
		if (rc == Abort) goto end;
	}
end:
    sem_post(&_semaphore);
  	INC_STATS(txnMan->get_thd_id(),occ_validate_time,get_sys_clock() - starttime);
	return rc;
}
#endif