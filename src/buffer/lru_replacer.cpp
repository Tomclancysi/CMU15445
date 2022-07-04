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
    free_frame_count = 0;
    this->cursor = 0;
    this->num_pages = num_pages;
    this->num_unpinned_frame = 0;
    this->unpinned = std::vector<bool>(this->num_pages, false);
    this->reference = std::vector<bool>(this->num_pages, true);
    this->used = std::vector<bool>(this->num_pages, true);
}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool { 
    // move cursor to next position where is unpinned and reference bit equal to zero
    if(num_unpinned_frame == 0)
        return false;
    MoveToFristUnpinned();
    if(!reference[cursor]){
        ResetCursor(cursor);
        *frame_id = (frame_id_t)cursor;
        return true;
    }
    auto old_cursor = cursor;
    do{
        reference[cursor] = 0;
        cursor = (cursor + 1) % num_pages;
    }while(!(unpinned[cursor] && !reference[cursor]) && cursor != old_cursor);
    // vicit it to disk or...
    ResetCursor(cursor);
    // return value
    *frame_id = (frame_id_t)cursor;
    return true;
}

void LRUReplacer::MoveToFristUnpinned() {
    while(unpinned[cursor] == false)
        cursor = (cursor + 1) % num_pages;
}

void LRUReplacer::ResetCursor(size_t cursor){
    reference[cursor] = 0;
    // unpinned cursor should be false because it is used by a new block
    unpinned[cursor] = 0;
    num_unpinned_frame -= 1;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    if(unpinned[frame_id]){
        unpinned[frame_id] = false;
        num_unpinned_frame -= 1;
    }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    if(!unpinned[frame_id]){
        reference[frame_id] = 1;
        unpinned[frame_id] = true;
        num_unpinned_frame += 1;
    }
}

auto LRUReplacer::Size() -> size_t { return num_unpinned_frame; }

}  // namespace bustub
