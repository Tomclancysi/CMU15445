//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
    this->cursor_ = 0;
    this->num_pages_ = num_pages;
    this->num_unpinned_frame_ = 0;
    this->unpinned_ = std::vector<bool>(this->num_pages_, false);
    this->reference_ = std::vector<bool>(this->num_pages_, true);
    // this->used = std::vector<bool>(this->num_pages_, true);
}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
    // move cursor_ to next position where is unpinned and reference bit equal to zero
    std::lock_guard<std::mutex> guard(latch_);
    if (num_unpinned_frame_ == 0) {
        return false;
    }
    MoveToFristUnpinned();
    if (!reference_[cursor_]) {
        ResetCursor(cursor_);
        *frame_id = static_cast<frame_id_t>(cursor_);
        cursor_ = (cursor_ + 1) % num_pages_;
        return true;
    }
    auto old_cursor = cursor_;
    do {
        reference_[cursor_] = false;
        cursor_ = (cursor_ + 1) % num_pages_;
    }while(!(unpinned_[cursor_] && !reference_[cursor_]) && cursor_ != old_cursor);
    // vicit it to disk or...
    ResetCursor(cursor_);
    // return value
    *frame_id = static_cast<frame_id_t>(cursor_);
    cursor_ = (cursor_ + 1) % num_pages_;
    return true;
}

void LRUReplacer::MoveToFristUnpinned() {
    while (!unpinned_[cursor_]) {
        cursor_ = (cursor_ + 1) % num_pages_;
    }
}

void LRUReplacer::ResetCursor(size_t cursor_) {
    reference_[cursor_] = false;
    // unpinned cursor_ should be false because it is used by a new block
    unpinned_[cursor_] = false;
    num_unpinned_frame_ -= 1;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> guard(latch_);
    if (unpinned_[frame_id]) {
        unpinned_[frame_id] = false;
        num_unpinned_frame_ -= 1;
    }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> guard(latch_);
    if (!unpinned_[frame_id]) {
        reference_[frame_id] = true;
        unpinned_[frame_id] = true;
        num_unpinned_frame_ += 1;
    }
}

auto LRUReplacer::Size() -> size_t {
    std::lock_guard<std::mutex> guard(latch_);
    return num_unpinned_frame_;
}

}  // namespace bustub
