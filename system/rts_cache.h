// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef RTS_CACHE
#define RTS_CACHE
#include <limits>
#include <string>
#include <vector>
#include <map>

#include "key_xid.h"

class RtsEnt : public KeyRangeEnt {
 public:
  RtsEnt() : KeyRangeEnt(), rts(0) {};
  RtsEnt(uint64_t Sk, uint64_t Ek, uint64_t timestamp) : KeyRangeEnt(Sk, Ek), rts(timestamp) {};
  uint64_t rts;
};

class RtsCache {
 public:
  RtsCache() {};
  void init();
  RC add(uint64_t StartKey, uint64_t EndKey, uint64_t timestamp);

  RC addWithMutex(uint64_t StartKey, uint64_t EndKey, uint64_t timestamp);
  uint64_t getRts(uint64_t StartKey, uint64_t EndKey);

 private:
  pthread_mutex_t* lock_;
  std::map<uint64_t, RtsEnt> RtsCaches;
};

#endif  // !RTS_CACHE
