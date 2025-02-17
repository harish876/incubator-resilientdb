#ifndef LRU_CACHE_H
#define LRU_CACHE_H

#include <list>
#include <unordered_map>

namespace resdb {

template <typename KeyType, typename ValueType>
class LRUCache {
 public:
  LRUCache(int capacity);
  ~LRUCache();

  ValueType Get(KeyType key);
  void Put(KeyType key, ValueType value);
  void SetCapacity(int new_capacity);
  void Flush();
  int GetCacheHits() const;
  int GetCacheMisses() const;
  double GetCacheHitRatio() const;

 private:
  int m_;
  int cache_hits_;
  int cache_misses_;
  std::list<KeyType> dq_;
  std::unordered_map<KeyType, ValueType> um_;
};

}  // namespace resdb

#endif  // LRU_CACHE_H