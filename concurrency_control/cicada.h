#ifndef _CICADA_H_
#define _CICADA_H_

#include "row.h"
#include "semaphore.h"


// the record for CICADA is oganized as follows:
// 1. record is never deleted.
// 2. record forms a single directional list.
//		head version -> version_x+1 -> version_x -> version_x-1 -> ... -> original
//    The head is always the latest and the tail the youngest.
// 	  When history is traversed, always go from head -> tail order.

class TxnManager;

class Cicada {
public:
	void init();
	RC validate(TxnManager * txn);
private:
 	sem_t 	_semaphore;
};

#endif
