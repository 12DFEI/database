#pragma once
#include <limits>
#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

class LRUKReplacer {
 public:
  explicit LRUKReplacer(size_t num_frames, size_t k);
  DISALLOW_COPY_AND_MOVE(LRUKReplacer);
  ~LRUKReplacer() = default;

  auto Evict(frame_id_t *frame_id) -> bool;
  void RecordAccess(frame_id_t frame_id);
  void SetEvictable(frame_id_t frame_id, bool set_evictable);
  void Remove(frame_id_t frame_id);
  auto Size() -> size_t;

 private:
  size_t current_timestamp_{0};
  [[maybe_unused]] size_t curr_size_{0};
  size_t replacer_size_;
  size_t k_;
  std::unordered_map<frame_id_t, std::list<size_t>> hist_map_;
  std::unordered_map<frame_id_t, size_t> last_map_;
  std::unordered_map<frame_id_t, bool> evict_flag_;
  std::mutex latch_;
};

}  // namespace bustub
