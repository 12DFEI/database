//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.h
//
// Identification: src/include/container/hash/extendible_hash_table.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * extendible_hash_table.h
 *
 * Implementation of in-memory hash table using extendible hashing
 */

#pragma once

#include <list>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "container/hash/hash_table.h"

namespace bustub {

template <typename K, typename V>
class ExtendibleHashTable : public HashTable<K, V> {
 public:
  explicit ExtendibleHashTable(size_t bucket_size);

  auto GetGlobalDepth() const -> int;
  auto GetLocalDepth(int dir_index) const -> int;
  auto GetNumBuckets() const -> int;

  auto Find(const K &key, V &value) -> bool override;
  void Insert(const K &key, const V &value) override;
  auto Remove(const K &key) -> bool override;

  class Bucket {
   public:
    explicit Bucket(size_t size, int depth = 0);

    inline auto IsFull() const -> bool { return cell_.size() == size_; }
    inline auto GetDepth() const -> int { return depth_; }
    inline void IncrementDepth() { ++depth_; }
    inline auto GetItems() -> std::list<std::pair<K, V>> & { return cell_; }

    auto Find(const K &key, V &value) -> bool;
    auto Remove(const K &key) -> bool;
    auto Insert(const K &key, const V &value) -> bool;

   private:
    size_t size_;
    int depth_;
    std::list<std::pair<K, V>> cell_;
  };

 private:
  int global_depth_{0};
  size_t bucket_size_;
  int bucket_cnt_{1};
  mutable std::mutex latch_;
  std::vector<std::shared_ptr<Bucket>> dir_;

  auto IndexOf(const K &key) -> size_t;
  auto GetGlobalDepthInternal() const -> int;
  auto GetLocalDepthInternal(int dir_index) const -> int;
  auto GetNumBucketsInternal() const -> int;
};

}  // namespace bustub
