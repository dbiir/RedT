#ifndef _ROUTINE_H_
#define _ROUTINE_H_

#include "global.h"
/* Using boost coroutine  */
#include<boost/coroutine/all.hpp>

typedef boost::coroutines::symmetric_coroutine<void>::call_type coroutine_func_t;
typedef boost::coroutines::symmetric_coroutine<void>::yield_type yield_func_t;


#endif