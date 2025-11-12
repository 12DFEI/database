//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager_instance.cpp
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // allocate buffer pool array
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size_, replacer_k);

  // initialize free list with all frames
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // next_page_id_ already default-initialized to 0
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

/**
 * AllocatePage: returns the next page id (thread-safe via atomic next_page_id_).
 */
auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

/**
 * NewPgImp - create a new page in the buffer pool.
 */
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> guard(latch_);

  frame_id_t frame = -1;
  // 1) try free list first
  if (!free_list_.empty()) {
    frame = free_list_.front();
    free_list_.pop_front();
  } else {
    // try replacer
    if (!replacer_->Evict(&frame)) {
      // no replacable frame
      return nullptr;
    }
    // Evicted a frame -> need to write back if dirty and remove old mapping
    Page &victim = pages_[frame];
    if (victim.GetPageId() != INVALID_PAGE_ID) {
      if (victim.IsDirty()) {
        disk_manager_->WritePage(victim.GetPageId(), victim.GetData());
        victim.is_dirty_ = false;
      }
      // remove old mapping
      page_table_->Remove(victim.GetPageId());
      // clear replacer record for this frame
      replacer_->Remove(frame);
    }
  }

  // allocate new page id
  page_id_t new_id = AllocatePage();
  *page_id = new_id;

  // initialize the page frame
  Page &page = pages_[frame];
  page.ResetMemory();
  page.page_id_ = new_id;
  page.is_dirty_ = false;
  page.pin_count_ = 1;

  // insert into page table
  page_table_->Insert(new_id, frame);

  // record access and mark not evictable
  replacer_->RecordAccess(frame);
  replacer_->SetEvictable(frame, false);

  return &page;
}

/**
 * FetchPgImp - fetch a page from buffer pool or load from disk
 */
auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard<std::mutex> guard(latch_);

  frame_id_t frame = -1;
  // If present in page table, return it
  if (page_table_->Find(page_id, frame)) {
    Page &page = pages_[frame];
    page.pin_count_ += 1;
    // update replacer: record access and make non-evictable
    replacer_->RecordAccess(frame);
    replacer_->SetEvictable(frame, false);
    return &page;
  }

  // not found: need to find a replacement frame
  frame_id_t victim = -1;
  if (!free_list_.empty()) {
    victim = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&victim)) {
      // no available frame to evict
      return nullptr;
    }
    // write back if dirty and remove old mapping
    Page &old_page = pages_[victim];
    if (old_page.GetPageId() != INVALID_PAGE_ID) {
      if (old_page.IsDirty()) {
        disk_manager_->WritePage(old_page.GetPageId(), old_page.GetData());
        old_page.is_dirty_ = false;
      }
      page_table_->Remove(old_page.GetPageId());
      replacer_->Remove(victim);
    }
  }

  // read the requested page from disk into the chosen frame
  Page &page = pages_[victim];
  page.ResetMemory();
  disk_manager_->ReadPage(page_id, page.GetData());
  page.page_id_ = page_id;
  page.is_dirty_ = false;
  page.pin_count_ = 1;

  // update page table and replacer
  page_table_->Insert(page_id, victim);
  replacer_->RecordAccess(victim);
  replacer_->SetEvictable(victim, false);

  return &page;
}

/**
 * UnpinPgImp - decrease pin_count of a page and update dirty flag / replacer
 */
auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> guard(latch_);

  frame_id_t frame = -1;
  if (!page_table_->Find(page_id, frame)) {
    // not in buffer pool
    return false;
  }

  Page &page = pages_[frame];
  if (page.pin_count_ <= 0) {
    // cannot unpin a page with non-positive pin count
    return false;
  }

  page.pin_count_ -= 1;
  if (is_dirty) {
    page.is_dirty_ = true;
  }

  if (page.pin_count_ == 0) {
    // now evictable
    replacer_->SetEvictable(frame, true);
  }
  return true;
}

/**
 * FlushPgImp - write a single page to disk, regardless of dirty flag
 */
auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);

  frame_id_t frame = -1;
  if (!page_table_->Find(page_id, frame)) {
    return false;
  }

  Page &page = pages_[frame];
  disk_manager_->WritePage(page_id, page.GetData());
  page.is_dirty_ = false;
  return true;
}

/**
 * FlushAllPgsImp - write all resident pages to disk
 */
void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard<std::mutex> guard(latch_);
  for (size_t i = 0; i < pool_size_; ++i) {
    Page &page = pages_[i];
    if (page.GetPageId() != INVALID_PAGE_ID) {
      disk_manager_->WritePage(page.GetPageId(), page.GetData());
      page.is_dirty_ = false;
    }
  }
}

/**
 * DeletePgImp - delete a page from buffer pool (and deallocate on disk, which is a no-op)
 */
auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);

  frame_id_t frame = -1;
  if (!page_table_->Find(page_id, frame)) {
    // page not in buffer pool; still consider deleted
    return true;
  }

  Page &page = pages_[frame];
  if (page.pin_count_ > 0) {
    // pinned: cannot delete
    return false;
  }

  // if dirty, we can choose to write back; spec says deallocate -> no-op, but safer to write back
  if (page.IsDirty()) {
    disk_manager_->WritePage(page.GetPageId(), page.GetData());
    page.is_dirty_ = false;
  }

  // remove from page table and replacer, reset metadata and add frame to free list
  page_table_->Remove(page_id);
  replacer_->Remove(frame);

  page.ResetMemory();
  page.page_id_ = INVALID_PAGE_ID;
  page.pin_count_ = 0;
  page.is_dirty_ = false;

  free_list_.push_back(frame);

  // (optional) deallocate page on disk (no-op in this project)
  DeallocatePage(page_id);

  return true;
}

}  // namespace bustub
