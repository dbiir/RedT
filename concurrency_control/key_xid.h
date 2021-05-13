// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef KEY_XID
#define KEY_XID

#include <limits>
#include <string>
#include <vector>
#include <map>
#include <unordered_map>

#include "spin_lock.h"
#include "hash.h"

inline int KeyCompare(const uint64_t a, const uint64_t b) {
  if (a < b)
    return -1;
  else if (a == b)
    return 0;
  else
    return 1;
}

class KeyRangeEnt {
public:
  uint64_t StartKey;
  uint64_t EndKey;
  KeyRangeEnt() : StartKey(0), EndKey(0) {};
  KeyRangeEnt(const uint64_t Sk, const uint64_t Ek) {
    StartKey = Sk;
    if (Ek == 0)
      EndKey = Sk + 1;
    else
      EndKey = Ek;
  };
  ~KeyRangeEnt() {}
  bool getOverlaps(KeyRangeEnt anotherEnt);
};

struct key_range_hash
{
  unsigned int operator () (const KeyRangeEnt& pk) const
  {
    return hash_any((unsigned char*) pk.StartKey, sizeof(uint64_t));
  }
};

struct key_range_equal
{
  bool operator () (const KeyRangeEnt& a, const KeyRangeEnt& b) const
  {
    if (a.StartKey == b.StartKey && a.EndKey == b.EndKey)
      return true;
    else
      return false;
  }
};

class KeyXidEnt : public KeyRangeEnt {
  public:
    KeyXidEnt() : writeTxn(0), isDelete(false)  { };
    KeyXidEnt(uint64_t Sk, uint64_t Ek) :
    KeyRangeEnt(Sk, Ek), writeTxn(0), isDelete(false) { };
    KeyXidEnt(uint64_t Sk, uint64_t Ek, const std::vector<uint64_t>& readTxn_, uint64_t writeTxn_) :
    KeyRangeEnt(Sk, Ek), readTxn(readTxn_), writeTxn(writeTxn_), isDelete(false) { };
    KeyXidEnt(const KeyRangeEnt& ent) {
      StartKey = ent.StartKey;
      EndKey = ent.EndKey;
    }
    KeyXidEnt(const KeyRangeEnt& ent, const std::vector<uint64_t>& readTxn_, uint64_t writeTxn_) : readTxn(readTxn_), writeTxn(writeTxn_) {
      StartKey = ent.StartKey;
      EndKey = ent.EndKey;
    }
    std::vector<uint64_t> readTxn;
    uint64_t writeTxn;
    bool isDelete = false;
};

class KeyXidCache {
  public:
    KeyXidCache() {};
    void init();
    int getKeyXidSize() { return KeyXids_simple.size() + KeyXids_multi.size(); }
    int getMultiKeyXidSize() { return KeyXids_multi.size(); }
    RC addReadXidWithMutex(uint64_t StartKey, uint64_t EndKey, uint64_t txn, int32_t isolation);
    std::vector<uint64_t> addWriteXidWithMutex(uint64_t StartKey, uint64_t EndKey, uint64_t txn, int32_t isolation, bool is_delete);
    RC addWriteXidTest(uint64_t StartKey, uint64_t EndKey, uint64_t txn, int32_t isolation);
    RC removeReadXidWithMutex(uint64_t StartKey, uint64_t EndKey, uint64_t txn, int32_t isolation);
    RC removeWriteXidWithMutex(uint64_t StartKey, uint64_t EndKey, uint64_t txn, int32_t isolation);
    std::vector<uint64_t> getReadXid(uint64_t StartKey, uint64_t EndKey, bool is_delete);
    std::vector<uint64_t> getWriteXid(uint64_t StartKey, uint64_t EndKey, bool is_delete);

  private:
    pthread_mutex_t * lock_;
    std::map<uint64_t,KeyXidEnt> KeyXids_simple;
    std::unordered_map<KeyRangeEnt,KeyXidEnt,key_range_hash,key_range_equal> KeyXids_multi;
    RC addReadXid(uint64_t Key, uint64_t txn);
    RC addReadXid(uint64_t StartKey, uint64_t EndKey, uint64_t txn);
    uint64_t addWriteXid(uint64_t Key, uint64_t txn);
    uint64_t addWriteXid(uint64_t StartKey, uint64_t EndKey, uint64_t txn);
    RC removeReadXid(uint64_t Key, uint64_t txn);
    RC removeReadXid(uint64_t StartKey, uint64_t EndKey, uint64_t txn);
    RC removeWriteXid(uint64_t Key, uint64_t txn);
    RC removeWriteXid(uint64_t StartKey, uint64_t EndKey, uint64_t txn);
    RC removeAllReadXid(uint64_t txn);
    RC removeAllWriteXid(uint64_t txn);

    std::vector<uint64_t> getSingleReadXid(uint64_t StartKey, uint64_t EndKey, bool is_delete);
    std::vector<uint64_t> getMultiReadXid(uint64_t StartKey, uint64_t EndKey);
    std::vector<uint64_t> getSingleWriteXid(uint64_t StartKey, uint64_t EndKey, bool is_delete);
    std::vector<uint64_t> getMultiWriteXid(uint64_t StartKey, uint64_t EndKey);
};

#endif  // !RTS_CACHE
