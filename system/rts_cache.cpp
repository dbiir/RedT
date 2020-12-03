//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "global.h"
#include "helper.h"
#include "rts_cache.h"
#include "mem_alloc.h"

void RtsCache::init() {
  lock_ = (pthread_mutex_t*)mem_allocator.alloc(sizeof(pthread_mutex_t));
  pthread_mutex_init(lock_, NULL);
}

RC RtsCache::add(uint64_t StartKey, uint64_t EndKey, uint64_t timestamp) {
  RC ret = RCOK;
  map<uint64_t, RtsEnt>::iterator it;
  RtsEnt newent;
  newent.StartKey = StartKey;
  if (EndKey == 0)
    newent.EndKey = StartKey + 1;
  else
    newent.EndKey = EndKey;
  newent.rts = timestamp;

  map<uint64_t, RtsEnt>::iterator startIt, endIt;
  startIt = RtsCaches.lower_bound(newent.StartKey);
  if (startIt != RtsCaches.begin()) startIt--;
  endIt = RtsCaches.upper_bound(newent.EndKey);
  if (endIt != RtsCaches.end()) endIt++;

  for (it = startIt; it != endIt;) {
    RtsEnt& rtsent = it->second;
    if (!newent.getOverlaps(rtsent)) {
      it++;
      continue;
    }
    int ss = KeyCompare(newent.StartKey, rtsent.StartKey);
    int ee = KeyCompare(newent.EndKey, rtsent.EndKey);

    if (rtsent.rts < timestamp) {
      // New: ------------
      // Old: ------------
      //
      // New: ------------
      // Old:
      if (ss == 0 && ee == 0) {
        rtsent.rts = timestamp;
        return ret;
      }
      // New: ------------
      // Old:   --------
      //
      // New: ------------
      // Old:
      else if (ss <= 0 && ee >= 0) {
        RtsCaches.erase(it++);
      }
      // New:   --------
      // Old: ------------
      //
      // New:   --------
      // Old: --        --
      else if (ss > 0 && ee < 0) {
        uint64_t oldEnd = rtsent.EndKey;
        rtsent.EndKey = newent.StartKey;

        RtsEnt entl(newent.EndKey, oldEnd, timestamp);
        RtsCaches.emplace(entl.StartKey, std::move(entl));
        it++;
      }
      // New:     --------          --------
      // Old: --------      or  ------------
      //
      // New:     --------          --------
      // Old: ----              ----
      else if (ee >= 0) {
        rtsent.EndKey = newent.StartKey;
        it++;
      }
      // New: --------          --------
      // Old:     --------  or  ------------
      //
      // New: --------          --------
      // Old:         ----              ----
      else if (ss <= 0) {
        RtsEnt entr(newent.EndKey, rtsent.EndKey, rtsent.rts);
        RtsCaches.emplace(entr.StartKey, std::move(entr));

        RtsCaches.erase(it++);
      } else {
        // here means that the two range have no overlap.
      }
    } else if (rtsent.rts > timestamp) {
      // Old: -----------      -----------      -----------      -----------
      // New:    -----     or  -----------  or  --------     or     --------
      //
      // Old: -----------      -----------      -----------      -----------
      // New:
      if (ss >= 0 && ee <= 0) {
        return ret;
      }
      // Old:    ------
      // New: ------------
      //
      // Old:    ------
      // New: ---      ---
      else if (ss < 0 && ee > 0) {
        RtsEnt entl(newent.StartKey, rtsent.StartKey, timestamp);
        RtsCaches.emplace(entl.StartKey, std::move(entl));

        newent.StartKey = rtsent.EndKey;
        it++;
      }
      // Old: --------          --------
      // New:     --------  or  ------------
      //
      // Old: --------          --------
      // New:         ----              ----
      else if (ee > 0) {
        newent.StartKey = rtsent.EndKey;
        it++;
      }
      // Old:     --------          --------
      // New: --------      or  ------------
      //
      // Old:     --------          --------
      // New: ----              ----
      else if (ss < 0) {
        newent.EndKey = rtsent.StartKey;
        it++;
      } else {
        // here no overlap;
        it++;
      }
    } else {
      if (ss == 0 && ee == 0) {
        // New: ------------
        // Old: ------------
        //
        // New: ------------
        // Old:

        // do nothing
        return ret;
      } else if (ss == 0 && ee > 0) {
        // New: ------------
        // Old: ----------
        //
        // New:           --
        // Old: ==========
        newent.StartKey = rtsent.EndKey;
        it++;
      } else if (ss < 0 && ee == 0) {
        // New: ------------
        // Old:   ----------
        //
        // New: --
        // Old:   ==========
        newent.EndKey = rtsent.StartKey;
        it++;
      } else if (ss < 0 && ee > 0) {
        // New: ------------
        // Old:   --------
        //
        // New: --        --
        // Old:   ========
        RtsEnt entl(newent.StartKey, rtsent.StartKey, timestamp);
        RtsCaches.emplace(entl.StartKey, std::move(entl));

        newent.StartKey = rtsent.EndKey;
        it++;
      } else if (ss >= 0 && ee <= 0) {
        // New:     ----
        // Old: ------------
        //
        // New:
        // Old: ------------

        // do nothing
        return ret;
      } else if (ee > 0) {
        // New:     --------
        // Old: --------
        //
        // New:         ----
        // Old: --------
        newent.StartKey = rtsent.EndKey;
        it++;
      } else if (ss < 0) {
        // New: --------
        // Old:     --------
        //
        // New: ----
        // Old:     ====----
        newent.EndKey = rtsent.StartKey;
        it++;
      } else {
        // no overlap
        it++;
      }
    }
  }
  RtsCaches.emplace(newent.StartKey, std::move(newent));
  return ret;
}

RC RtsCache::addWithMutex(uint64_t StartKey, uint64_t EndKey, uint64_t timestamp) {
  pthread_mutex_lock(lock_);
  RC ret = add(StartKey, EndKey, timestamp);
  pthread_mutex_unlock(lock_);
  return ret;
}

uint64_t RtsCache::getRts(uint64_t StartKey, uint64_t EndKey) {
  pthread_mutex_lock(lock_);
  map<uint64_t, RtsEnt>::iterator it;
  uint64_t rts = 0;
  RtsEnt newent;
  newent.StartKey = StartKey;
  if (EndKey == 0)
    newent.EndKey = StartKey + 1;
  else
    newent.EndKey = EndKey;

  map<uint64_t, RtsEnt>::iterator startIt, endIt;
  startIt = RtsCaches.lower_bound(newent.StartKey);
  if (startIt != RtsCaches.begin()) startIt--;
  endIt = RtsCaches.upper_bound(newent.EndKey);
  if (endIt != RtsCaches.end()) endIt++;

  for (it = startIt; it != endIt; it++) {
    RtsEnt rtsent = it->second;
    if (newent.getOverlaps(rtsent)) rts = rtsent.rts;
  }
  pthread_mutex_unlock(lock_);
  return rts;
}
