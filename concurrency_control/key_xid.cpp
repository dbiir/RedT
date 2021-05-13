//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "global.h"
#include "helper.h"
#include "key_xid.h"
#include "mem_alloc.h"

bool KeyRangeEnt::getOverlaps(KeyRangeEnt anotherEnt){
  if (anotherEnt.StartKey > EndKey && StartKey < anotherEnt.EndKey)
    return true;
  else
    return false;
}

void KeyXidCache::init() {
  lock_ = (pthread_mutex_t *)
		mem_allocator.alloc(sizeof(pthread_mutex_t));
	pthread_mutex_init(lock_, NULL);
}

RC KeyXidCache::addReadXidWithMutex( uint64_t StartKey, uint64_t EndKey, uint64_t txn, int32_t isolation){
  RC ret = RCOK;
  // if (isolation < TDTransIsolation::SEQUENTIAL_CONCURRENT)
  //   return ret;
  pthread_mutex_lock(lock_);
  if (EndKey == 0)
    ret = addReadXid(StartKey, txn);
  else
    ret = addReadXid(StartKey, EndKey, txn);
  pthread_mutex_unlock(lock_);
  return ret;
}

std::vector<uint64_t> KeyXidCache::addWriteXidWithMutex(uint64_t StartKey, uint64_t EndKey, uint64_t txn, int32_t isolation, bool is_delete){
  std::vector<uint64_t> result;
  // if (isolation < TDTransIsolation::SEQUENTIAL_CONCURRENT)
  //   return result;
  pthread_mutex_lock(lock_);
  result = getWriteXid(StartKey, EndKey, is_delete);
  if (result.size() == 0) {
    if (EndKey == 0)
      addWriteXid(StartKey, txn);
    else
      addWriteXid(StartKey, EndKey, txn);
  }
  pthread_mutex_unlock(lock_);
  return result;
}

RC KeyXidCache::addWriteXidTest(uint64_t StartKey, uint64_t EndKey, uint64_t txn, int32_t isolation){
  std::vector<uint64_t> result;
  RC ret = RCOK;
  // if (isolation < TDTransIsolation::SEQUENTIAL_CONCURRENT)
  //   return 0;
  pthread_mutex_lock(lock_);
  result = getWriteXid(StartKey, EndKey, false);
  if (result.size() == 0) {
    if (EndKey == 0)
      addWriteXid(StartKey, txn);
    else
      addWriteXid(StartKey, EndKey, txn);
  }
  pthread_mutex_unlock(lock_);
  return ret;
}

RC KeyXidCache::removeReadXidWithMutex(uint64_t StartKey, uint64_t EndKey, uint64_t txn, int32_t isolation){
  RC ret = RCOK;
  // if (isolation < TDTransIsolation::SEQUENTIAL_CONCURRENT)
  //   return ret;
  pthread_mutex_lock(lock_);
  if (StartKey == 0)
    ret = removeAllReadXid(txn);
  else if (EndKey == 0)
    ret = removeReadXid(StartKey, txn);
  else
    ret = removeReadXid(StartKey, EndKey, txn);
  pthread_mutex_unlock(lock_);
  return ret;
}

RC KeyXidCache::removeWriteXidWithMutex(uint64_t StartKey, uint64_t EndKey, uint64_t txn, int32_t isolation){
  RC ret = RCOK;
  // if (isolation < TDTransIsolation::SEQUENTIAL_CONCURRENT)
  //   return ret;
  pthread_mutex_lock(lock_);
  if (StartKey == 0)
    ret = removeAllWriteXid(txn);
  else if (EndKey == 0)
    ret = removeWriteXid(StartKey, txn);
  else
    ret = removeWriteXid(StartKey, EndKey, txn);
  pthread_mutex_unlock(lock_);
  return ret;
}

RC KeyXidCache::addReadXid(uint64_t StartKey, uint64_t EndKey, uint64_t txn){
  RC ret = RCOK;
  KeyRangeEnt newent(StartKey, EndKey);
  unordered_map<KeyRangeEnt,KeyXidEnt,key_range_hash,key_range_equal>::iterator it = KeyXids_multi.find(newent);

  if (it != KeyXids_multi.end()) {
    KeyXidEnt& xident = it->second;
    xident.readTxn.insert(xident.readTxn.end(), txn);
  } else {
    KeyXidEnt newxid(newent, std::vector<uint64_t>(1, txn), 0);
    KeyXids_multi.emplace(newent, std::move(newxid));
    //TRANS_LOG_DEBUG("addReadXid do not find StartKey:%s EndKey:%s", StartKey.TO_HEX(), EndKey.TO_HEX());
  }
  return ret;
}

RC KeyXidCache::addReadXid(uint64_t Key, uint64_t txn){
  RC ret = RCOK;

  map<uint64_t,KeyXidEnt>::iterator it = KeyXids_simple.find(Key);
  if (it != KeyXids_simple.end()) {
    KeyXidEnt& xident = it->second;
    xident.readTxn.insert(xident.readTxn.end(), txn);
  } else {
    KeyXidEnt newxid(Key, 0, std::vector<uint64_t>(1, txn), 0);
    KeyXids_simple.emplace(newxid.StartKey, std::move(newxid));
    //TRANS_LOG_DEBUG("addReadXid do not find key:%s", Key.TO_HEX());
  }
  return ret;
}

uint64_t KeyXidCache::addWriteXid(uint64_t StartKey, uint64_t EndKey, uint64_t txn){
  uint64_t nullWriteTrans = 0;
  KeyRangeEnt newent(StartKey, EndKey);
  unordered_map<KeyRangeEnt,KeyXidEnt,key_range_hash,key_range_equal>::iterator it = KeyXids_multi.find(newent);

  if (it != KeyXids_multi.end()) {
    KeyXidEnt& xident = it->second;
    if (xident.writeTxn != 0 && xident.writeTxn != txn) {
      nullWriteTrans = xident.writeTxn;
    } else {
      xident.writeTxn = txn;
    }
  } else {
    KeyXidEnt newxid(newent);
    newxid.writeTxn = txn;
    KeyXids_multi.emplace(newent, std::move(newxid));
    //TRANS_LOG_DEBUG("addWriteXid do not find StartKey:%s EndKey:%s", StartKey.TO_HEX(), EndKey.TO_HEX());
  }
  return nullWriteTrans;
}

uint64_t KeyXidCache::addWriteXid(uint64_t Key, uint64_t txn){
  uint64_t nullWriteTrans = 0;

  map<uint64_t,KeyXidEnt>::iterator it = KeyXids_simple.find(Key);
  if (it != KeyXids_simple.end()) {
    KeyXidEnt& xident = it->second;
    if (xident.writeTxn != 0 && xident.writeTxn != txn) {
      nullWriteTrans = xident.writeTxn;
    } else {
      xident.writeTxn = txn;
    }
  } else {
    KeyXidEnt newxid(Key, 0);
    newxid.writeTxn = txn;
    KeyXids_simple.emplace(newxid.StartKey, std::move(newxid));
    //TRANS_LOG_DEBUG("addWriteXid do not find key:%s", Key.TO_HEX());
  }
  return nullWriteTrans;
}

RC KeyXidCache::removeAllReadXid(uint64_t txn) {
  RC ret = RCOK;
  map<uint64_t,KeyXidEnt>::iterator sit;

  for(sit=KeyXids_simple.begin();sit!=KeyXids_simple.end();sit++){
    // Slice key = sit->first;
    KeyXidEnt& xident = sit->second;
    // TRANS_LOG_DEBUG("removeAllReadXid txn:%lu key:%s, value:%s", txn, key.TO_HEX(), xident.StartKey.TO_HEX());
    xident.readTxn.erase(remove(xident.readTxn.begin(), xident.readTxn.end(), txn), xident.readTxn.end());
  }
  std::unordered_map<KeyRangeEnt,KeyXidEnt,key_range_hash,key_range_equal>::iterator mit;
  for(mit=KeyXids_multi.begin();mit!=KeyXids_multi.end();mit++){
    KeyXidEnt& xident = mit->second;
    xident.readTxn.erase(remove(xident.readTxn.begin(), xident.readTxn.end(), txn), xident.readTxn.end());
  }
  return ret;
}

RC KeyXidCache::removeReadXid(uint64_t StartKey, uint64_t EndKey, uint64_t txn){
  RC ret = RCOK;
  KeyRangeEnt newent(StartKey, EndKey);
  unordered_map<KeyRangeEnt,KeyXidEnt,key_range_hash,key_range_equal>::iterator it = KeyXids_multi.find(newent);

  if (it != KeyXids_multi.end()) {
    KeyXidEnt& xident = it->second;
    xident.readTxn.erase(remove(xident.readTxn.begin(), xident.readTxn.end(), txn), xident.readTxn.end());
    //TRANS_LOG_DEBUG("removeReadXid xid_count:%d StartKey:%s EndKey:%s", xident.readTxn.size(), StartKey.TO_HEX(), EndKey.TO_HEX());
    if (xident.readTxn.size() == 0 && xident.writeTxn == 0) {
      KeyXids_multi.erase(it);
    }
  }
  return ret;
}

RC KeyXidCache::removeReadXid(uint64_t Key, uint64_t txn){
  RC ret = RCOK;

  map<uint64_t,KeyXidEnt>::iterator it = KeyXids_simple.find(Key);
  if (it != KeyXids_simple.end()) {
    KeyXidEnt& xident = it->second;
    xident.readTxn.erase(remove(xident.readTxn.begin(), xident.readTxn.end(), txn), xident.readTxn.end());
    //TRANS_LOG_DEBUG("removeReadXid xid_count:%d Key:%s", xident.readTxn.size(), Key.TO_HEX());
    if (xident.isDelete && xident.readTxn.size() == 0 && xident.writeTxn == 0) {

      KeyXids_simple.erase(it);
    }
  }
  return ret;
}

RC KeyXidCache::removeAllWriteXid(uint64_t txn) {
  RC ret = RCOK;
  map<uint64_t,KeyXidEnt>::iterator sit;

  for(sit=KeyXids_simple.begin();sit!=KeyXids_simple.end();sit++){
    KeyXidEnt& xident = sit->second;
    if (xident.writeTxn == txn)
      xident.writeTxn = 0;
  }
  std::unordered_map<KeyRangeEnt,KeyXidEnt,key_range_hash,key_range_equal>::iterator mit;
  for(mit=KeyXids_multi.begin();mit!=KeyXids_multi.end();mit++){
    KeyXidEnt& xident = mit->second;
    if (xident.writeTxn == txn)
      xident.writeTxn = 0;
  }
  return ret;
}

RC KeyXidCache::removeWriteXid(uint64_t StartKey, uint64_t EndKey, uint64_t txn){
  RC ret = RCOK;
  KeyRangeEnt newent(StartKey, EndKey);
  unordered_map<KeyRangeEnt,KeyXidEnt,key_range_hash,key_range_equal>::iterator it = KeyXids_multi.find(newent);

  if (it != KeyXids_multi.end()) {
    KeyXidEnt& xident = it->second;
    if (xident.writeTxn == txn) {
      xident.writeTxn = 0;
    }
    if (xident.readTxn.size() == 0 && xident.writeTxn == 0) {
      KeyXids_multi.erase(it);
    }
  }
  return ret;
}

RC KeyXidCache::removeWriteXid(uint64_t Key, uint64_t txn){
  RC ret = RCOK;

  map<uint64_t,KeyXidEnt>::iterator it = KeyXids_simple.find(Key);
  if (it != KeyXids_simple.end()) {
    KeyXidEnt& xident = it->second;
    if (xident.writeTxn == txn) {
      xident.writeTxn = 0;
    }
    if (xident.isDelete && xident.readTxn.size() == 0 && xident.writeTxn == 0) {
      KeyXids_simple.erase(it);
    }
  }
  return ret;
}

std::vector<uint64_t> KeyXidCache::getSingleReadXid(uint64_t StartKey, uint64_t EndKey, bool is_delete){
  map<uint64_t,KeyXidEnt>::iterator it;
  std::vector<uint64_t> rtxn;
  KeyXidEnt newxid;
  newxid.StartKey = StartKey;
  if (EndKey == 0)
    newxid.EndKey = StartKey + 1;
  else
    newxid.EndKey = EndKey;

  map<uint64_t,KeyXidEnt>::iterator startIt, endIt;
  startIt = KeyXids_simple.lower_bound(newxid.StartKey);
  if (startIt != KeyXids_simple.begin())
    startIt--;
  endIt = KeyXids_simple.upper_bound(newxid.EndKey);;

  for(it=startIt;it!=endIt;it++){
    KeyXidEnt& xident = it->second;
    if (newxid.getOverlaps(xident)) {
      rtxn.insert(rtxn.end(), xident.readTxn.begin(), xident.readTxn.end());
      if (is_delete == true)
        xident.isDelete = is_delete;
    }
  }
  return rtxn;
}

std::vector<uint64_t> KeyXidCache::getMultiReadXid(uint64_t StartKey, uint64_t EndKey){
  unordered_map<KeyRangeEnt,KeyXidEnt,key_range_hash,key_range_equal>::iterator it;
  std::vector<uint64_t> rtxn;
  KeyXidEnt newxid;
  newxid.StartKey = StartKey;
  if (EndKey == 0)
    newxid.EndKey = StartKey+1;
  else
    newxid.EndKey = EndKey;

  for(it=KeyXids_multi.begin();it!=KeyXids_multi.end();it++){
    KeyXidEnt xident = it->second;
    if (newxid.getOverlaps(xident)) {
      rtxn.insert(rtxn.end(), xident.readTxn.begin(), xident.readTxn.end());
    }
  }
  return rtxn;
}

std::vector<uint64_t> KeyXidCache::getReadXid(uint64_t StartKey, uint64_t EndKey, bool is_delete){
  pthread_mutex_lock(lock_);
  map<uint64_t,KeyXidEnt>::iterator it;
  std::vector<uint64_t> rtxns = getSingleReadXid(StartKey, EndKey, is_delete);
  std::vector<uint64_t> rtxnm = getMultiReadXid(StartKey, EndKey);
  rtxns.insert(rtxns.end(), rtxnm.begin(), rtxnm.end());
  pthread_mutex_unlock(lock_);
  return rtxns;
}

std::vector<uint64_t> KeyXidCache::getSingleWriteXid(uint64_t StartKey, uint64_t EndKey, bool is_delete){
  map<uint64_t,KeyXidEnt>::iterator it;
  std::vector<uint64_t> rtxn;
  KeyXidEnt newxid;
  newxid.StartKey = StartKey;
  if (EndKey == 0)
    newxid.EndKey = StartKey+1;
  else
    newxid.EndKey = EndKey;

  map<uint64_t,KeyXidEnt>::iterator startIt, endIt;
  startIt = KeyXids_simple.lower_bound(newxid.StartKey);
  if (startIt != KeyXids_simple.begin())
    startIt--;
  endIt = KeyXids_simple.upper_bound(newxid.EndKey);;

  for(it=startIt;it!=endIt;it++){
    KeyXidEnt& xident = it->second;
    if (newxid.getOverlaps(xident)) {
      if (is_delete == true)
        xident.isDelete = is_delete;
      if (xident.writeTxn != 0)
        rtxn.push_back(xident.writeTxn);
    }
  }
  return rtxn;
}

std::vector<uint64_t> KeyXidCache::getMultiWriteXid(uint64_t StartKey, uint64_t EndKey){
  unordered_map<KeyRangeEnt,KeyXidEnt,key_range_hash,key_range_equal>::iterator it;
  std::vector<uint64_t> rtxn;
  KeyXidEnt newxid;
  newxid.StartKey = StartKey;
  if (EndKey == 0)
    newxid.EndKey = StartKey+1;
  else
    newxid.EndKey = EndKey;

  for(it=KeyXids_multi.begin();it!=KeyXids_multi.end();it++){
    KeyXidEnt xident = it->second;
    if (newxid.getOverlaps(xident) && xident.writeTxn != 0) {
      rtxn.push_back(xident.writeTxn);
    }
  }
  return rtxn;
}

std::vector<uint64_t> KeyXidCache::getWriteXid(uint64_t StartKey, uint64_t EndKey, bool is_delete){
  // lock_.lock();
  map<uint64_t,KeyXidEnt>::iterator it;
  std::vector<uint64_t> wtxns = getSingleWriteXid(StartKey, EndKey, is_delete);
  std::vector<uint64_t> wtxnm = getMultiWriteXid(StartKey, EndKey);
  wtxns.insert(wtxns.end(), wtxnm.begin(), wtxnm.end());
  // lock_.unlock();
  return wtxns;
}
