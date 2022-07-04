//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id)) {
    frame_id_t frame_id = page_table_[page_id];
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    return true;
  }
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::lock_guard<std::mutex> lock(latch_);
  for (page_id_t page_id = instance_index_; page_id < next_page_id_; page_id += num_instances_) {
    if (page_table_.count(page_id)) {
      frame_id_t frame_id = page_table_[page_id];
      disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    }
  }
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  // 0.   Make sure you call AllocatePage!
  std::lock_guard<std::mutex> lock(latch_);
  if (free_list_.empty() && replacer_->Size() == 0)
    return nullptr;
  page_id_t new_page_id = AllocatePage();
  Page* p;
  frame_id_t frame_id = -1;
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  FindUseableFrame(&frame_id);
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  p = &pages_[frame_id];
  // pay attention new page's pin count should be set to 1.
  p->page_id_ = new_page_id;
  p->pin_count_ = 1;
  // new page need to write back even it is all space.
  p->is_dirty_ = true;
  p->ResetMemory();
  page_table_[new_page_id] = frame_id;
  replacer_->Pin(frame_id);
  // 4.   Set the page ID output parameter. Return a pointer to P.
  *page_id = new_page_id;
  return p;
}

void BufferPoolManagerInstance::FindUseableFrame(frame_id_t* frame_id) {
  if (!free_list_.empty()) {
    *frame_id = *free_list_.begin();
    free_list_.pop_front();
  } else {
    replacer_->Victim(frame_id);
    // this frame will be used by new page, so flush it
    if (pages_[*frame_id].IsDirty())
      disk_manager_->WritePage(pages_[*frame_id].page_id_, pages_[*frame_id].GetData());
    page_table_.erase(pages_[*frame_id].page_id_);
  }
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // auto old = replacer_->Size();
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (page_table_.count(page_id)) {
    frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_ += 1;
    replacer_->Pin(frame_id);
    return &pages_[frame_id];
  }
  if (free_list_.empty() && replacer_->Size() == 0)
    return nullptr;
  FindUseableFrame(&frame_id);
  // 2.     If R is dirty, write it back to the disk.
  Page* p = &pages_[frame_id];

  // 3.     Delete R from the page table and insert P.
  page_table_.erase(p->page_id_);
  page_table_[page_id] = frame_id;
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  p->page_id_ = page_id;
  p->pin_count_ = 1;
  p->is_dirty_ = false;
  replacer_->Pin(frame_id);
  disk_manager_->ReadPage(page_id, p->data_);
  return p;
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  // 0.   Make sure you call DeallocatePage!
  std::lock_guard<std::mutex> lock(latch_);
  DeallocatePage(page_id);
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  if (page_table_.count(page_id)) {
    Page* p = &pages_[page_table_[page_id]];
    if (p->pin_count_ == 0) {
      p->page_id_ = page_id;
      p->pin_count_ = 0;
      p->is_dirty_ = false;
      p->ResetMemory();
      free_list_.push_front(page_table_[page_id]);
      page_table_.erase(page_id);
      return true;
    }
    return false;
  }
  return true;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (page_table_.count(page_id) && pages_[(frame_id = page_table_[page_id])].pin_count_ > 0) {
    pages_[frame_id].is_dirty_ = is_dirty;
    if (--pages_[frame_id].pin_count_ == 0) {
      // auto old = replacer_->Size();
      replacer_->Unpin(frame_id);
      // printf("instance%d size increase from %d to %d\n", (int)instance_index_, (int)old, (int)replacer_->Size());
    }
    return true;
  }
  return false;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
