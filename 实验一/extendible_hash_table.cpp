#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : bucket_size_(bucket_size) {
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return bucket_cnt_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return dir_[IndexOf(key)]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return dir_[IndexOf(key)]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);

  while (dir_[IndexOf(key)]->IsFull()) {
    size_t idx = IndexOf(key);
    auto bucket = dir_[idx];

    if (bucket->GetDepth() == GetGlobalDepthInternal()) {
      ++global_depth_;
      size_t old_sz = dir_.size();
      dir_.resize(old_sz << 1);
      for (size_t i = 0; i < old_sz; ++i) dir_[i + old_sz] = dir_[i];
    }

    int split_mask = 1 << bucket->GetDepth();
    auto zero_bucket = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth() + 1);
    auto one_bucket  = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth() + 1);

    for (auto &kv : bucket->GetItems()) {
      size_t h = std::hash<K>()(kv.first);
      ((h & split_mask) ? one_bucket : zero_bucket)->Insert(kv.first, kv.second);
    }

    ++bucket_cnt_;

    for (size_t i = 0; i < dir_.size(); ++i) {
      if (dir_[i] == bucket) dir_[i] = (i & split_mask) ? one_bucket : zero_bucket;
    }
  }

  size_t pos = IndexOf(key);
  auto target = dir_[pos];
  for (auto &kv : target->GetItems())
    if (kv.first == key) {
      kv.second = value;
      return;
    }
  target->Insert(key, value);
}

//===-------------------------- Bucket Implementation ----------------------===//

template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth)
    : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &kv : cell_)
    if (kv.first == key) {
      value = kv.second;
      return true;
    }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = cell_.begin(); it != cell_.end(); ++it)
    if (it->first == key) {
      cell_.erase(it);
      return true;
    }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (cell_.size() >= size_) return false;
  cell_.emplace_back(key, value);
  return true;
}

//===-------------------------- Explicit Instantiation ---------------------===//

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
