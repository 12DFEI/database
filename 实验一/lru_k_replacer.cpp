#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : replacer_size_(num_frames), k_(k) {
  hist_map_ = std::unordered_map<frame_id_t, std::list<size_t>>();
  last_map_ = std::unordered_map<frame_id_t, size_t>();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  *frame_id = -1;
  current_timestamp_++;

  if (hist_map_.empty() && last_map_.empty()) return false;

  size_t lr_ts = current_timestamp_;

  for (const auto &kv : hist_map_) {
    if (evict_flag_.find(kv.first) != evict_flag_.end() && kv.second.front() < lr_ts) {
      *frame_id = kv.first;
      lr_ts = kv.second.front();
    }
  }

  if (hist_map_.find(*frame_id) != hist_map_.end()) {
    hist_map_.erase(*frame_id);
    evict_flag_.erase(*frame_id);
    return true;
  }

  for (const auto &kv : last_map_) {
    if (evict_flag_.find(kv.first) != evict_flag_.end() && kv.second < lr_ts) {
      *frame_id = kv.first;
      lr_ts = kv.second;
    }
  }

  if (*frame_id == -1) return false;

  last_map_.erase(*frame_id);
  evict_flag_.erase(*frame_id);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  current_timestamp_++;
  if (frame_id >= static_cast<int>(replacer_size_)) return;

  if (last_map_.find(frame_id) != last_map_.end()) {
    last_map_[frame_id] = current_timestamp_;
    return;
  }

  if (hist_map_.find(frame_id) == hist_map_.end()) hist_map_[frame_id] = std::list<size_t>();
  hist_map_[frame_id].push_back(current_timestamp_);

  if (hist_map_[frame_id].size() >= k_) {
    last_map_[frame_id] = current_timestamp_;
    hist_map_.erase(frame_id);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  current_timestamp_++;
  if (set_evictable && hist_map_.find(frame_id) == hist_map_.end() && last_map_.find(frame_id) == last_map_.end()) return;
  if (set_evictable) evict_flag_[frame_id] = true;
  else evict_flag_.erase(frame_id);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  current_timestamp_++;
  hist_map_.erase(frame_id);
  last_map_.erase(frame_id);
  evict_flag_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return evict_flag_.size();
}

}  // namespace bustub
