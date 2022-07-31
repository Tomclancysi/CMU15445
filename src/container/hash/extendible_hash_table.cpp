//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>
#include <limits>
#include <assert.h>


#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  // e 感觉应该提供一个文件夹的page id，但是既然没有那我就用默认了新建
  inf_ = std::numeric_limits<page_id_t>::max();
  directory_page_id_ = inf_;
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  // 给定一个key，首先查到它在文件夹的哪个index
  auto glb_mask = dir_page->GetGlobalDepthMask();
  auto hash_val = Hash(key);
  return hash_val & glb_mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  // 给定一个k，找到它对应的桶子 初始时，global depth等于0，他就没有桶子，这里默认桶子存在
  auto index = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(index);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  // 从头开始，首先就要获得文件夹目录，是new一个么，应该是的。
  Page *pg;
  if (directory_page_id_ == inf_) {
    pg = buffer_pool_manager_->NewPage(&directory_page_id_);
  } else {
    pg = buffer_pool_manager_->FetchPage(directory_page_id_);
  }
  // 要从page 转换成相应的对象, 页表以锁死，大胆的操作把
  if (pg == nullptr) {
    return nullptr;
  }
  // 由于没有虚函数啥的，可以直接reinterpret
  auto ret = reinterpret_cast<HashTableDirectoryPage *>(pg->GetData());
  ret->SetPageId(directory_page_id_);
  return ret;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> HASH_TABLE_BUCKET_TYPE * {
  Page *pg;
  pg = buffer_pool_manager_->FetchPage(bucket_page_id);
  // 转换成hash table对象
  if (pg == nullptr) {
    return nullptr;
  }
  HASH_TABLE_BUCKET_TYPE *ret =  reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(pg->GetData());
  return ret;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  auto dir_page = FetchDirectoryPage();
  // check if this is a empty dir
  if (dir_page->GetGlobalDepth() == 0) {
    return false;
  }
  auto page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(page_id);
  auto state = bucket_page->GetValue(key, comparator_, result);
  // unpin these page
  buffer_pool_manager_->UnpinPage(directory_page_id_);
  buffer_pool_manager_->UnpinPage(page_id);
  return state;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  // 找到桶之后，看是否存在？是否满了？是否可以split？是否global split？
  auto dir_page = FetchDirectoryPage();
  if (dir_page->GetGlobalDepth() == 0) {
    // 创建一个空桶
    page_id_t new_page_id;
    buffer_pool_manager_->NewPage(&new_page_id);
    dir_page->IncrGlobalDepth();
    dir_page->SetBucketPageId(0, new_page_id);
    dir_page->SetBucketPageId(1, new_page_id);
    dir_page->IncrLocalDepth(0);
    dir_page->IncrLocalDepth(1);
  }

  auto page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(page_id);
  // 这样page肯定存在，先验证key value是否已经存在
  std::vector<ValueType> exist_val;
  bucket_page->GetValue(key, comparator_, &exist_val);
  for (const auto &val : exist_val) {
    if (val == value) {
      return false;
    }
  }

  // 桶是否已满？ 如果已满，是增加全局还是局部的depth？
  if (bucket_page->IsFull()) {
    // old_idx 文件夹的老下标了，它的新伙伴下标new_idx
    auto old_idx = KeyToDirectoryIndex(key);
    // auto new_idx = old_idx | (1 << dir_page->GetLocalDepth(old_idx));
    auto new_idx = dir_page->GetSplitImageIndex(old_idx);
    if (dir_page->GetGlobalDepth() == dir_page->GetLocalDepth(old_idx)) {
      // 先增全局，然后局部。  需要对元素重新摆放位置（当localdepth增加时，这个桶里的就得重新分配）？ 这里怎么增？wtf
      // 一个bucket，全局增加一位后，这个bucket可以被两个同时指向，此时局部的不变，然后重新分配，找到bucket下标
      uint32_t glb_depth = dir_page->GetGlobalDepth();
      uint32_t upper_index = 1 << glb_depth;
      // 新增这一位到底是0是1，对于local暂时不影响
      for (uint32_t cur_idx = 0; cur_idx < upper_index; cur_idx++) {
        dir_page->SetBucketPageId(cur_idx | upper_index, dir_page->GetBucketPageId(cur_idx));
      }
      dir_page->IncrGlobalDepth();
    }

    // 增局部，然后判断还可能得增全局 （当localdepth增加时，这个桶里的就得重新分配）
    // 1. 首先要创建一个新的桶
    page_id_t new_page_id;
    HASH_TABLE_BUCKET_TYPE *new_bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(&new_page_id));
    // 2. 将原来桶里的值重新分配到两个桶子中，这时候直接插即可
    auto old_local_d = dir_page->GetLocalDepth(old_idx);
    uint32_t upper_index = 1 << dir_page->GetGlobalDepth();
    for (uint32_t cur_idx = 0; cur_idx < BUCKET_ARRAY_SIZE; cur_idx++) {
      if (bucket_page->IsReadable(cur_idx)) {
        auto the_key = bucket_page->KeyAt(cur_idx);
        if ((the_key >> old_local_d) & 1) {
          // 删除老桶 放到新桶之中
          auto the_value = bucket_page->ValueAt(cur_idx);
          bucket_page->SetReadable(cur_idx, false);
          new_bucket_page->Insert(the_key, the_value, comparator_);
        }
      }
    }
    // 3. 重新配置索引（注意：可能会有多个指向同一个桶的，所以这里更新bucket要循环）, 增加local depth
    auto mask = dir_page->GetLocalDepthMask(old_idx);
    for (uint32_t cur_idx = 0; cur_idx < upper_index; cur_idx++) {
      if ((cur_idx & mask) == (old_idx & mask)) {
        assert(dir_page->GetLocalDepth(cur_idx) == old_local_d);
        if ((cur_idx >> old_local_d) & 1) {
          dir_page->SetBucketPageId(cur_idx, new_page_id);
        } else {
          dir_page->SetBucketPageId(cur_idx, page_id);
        }
        dir_page->SetLocalDepth(cur_idx, old_local_d + 1);
      }
    }
    // 4. 就算这个时候也有可能会再次full，因此这段可能需要个循环，不过我暂时不想在这里处理
    // TODO 0730
    auto final_page_id = KeyToPageId(key);
    assert(final_page_id == page_id || final_page_id == new_page_id);
    if (final_page_id == page_id) {
      buffer_pool_manager_->UnpinPage(new_page_id, true);
    } else {
      buffer_pool_manager_->UnpinPage(page_id, true);
      page_id = new_page_id;
      bucket_page = new_bucket_page;
    }
  }

  auto ret = bucket_page->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(page_id, true);
  return ret;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  // eh i do not use this api
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  // 找到对应的bucket（如果存在的话，事实上它一定存在），从bucket删除，然后如果为空，调用shrink
  auto dir_page = FetchDirectoryPage();
  if (dir_page->GetGlobalDepth() == 0) {
    buffer_pool_manager_->UnpinPage(directory_page_id_);
    return false;
  }
  auto page_id = KeyToPageId(key);
  auto bucket_idx = KeyToDirectoryIndex(key);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(page_id);

  auto ret = bucket_page->Remove(key, value, comparator_);

  // 找到他的另一半，问题是什么时候能合并？注意他的另一半page一定存在
  // 首先，local depth相同。当前为空
  auto img_bucket_idx = dir_page->GetMergeImageIndex(bucket_idx);
  auto img_page_id = dir_page->GetBucketPageId(img_bucket_idx);
  HASH_TABLE_BUCKET_TYPE *img_bucket_page = FetchBucketPage(img_page_id);
  // assert(dir_page->GetLocalDepth(bucket_idx) == dir_page->GetLocalDepth(bucket_img_idx));

  if (bucket_page->IsEmpty() && dir_page->GetLocalDepth(bucket_idx) == dir_page->GetLocalDepth(img_bucket_idx) && dir_page->GetLocalDepth(bucket_idx) > 0) {
    // 合并之后，localdepth减少
    buffer_pool_manager_->UnpinPage(page_id);
    buffer_pool_manager_->DeletePage(page_id);
    // 我觉得其实就只需要修改一下dir的table,删除老的page
    auto old_local_d = dir_page->GetLocalDepth(bucket_idx);
    auto mask = dir_page->GetLocalDepthMask(bucket_idx) >> 1;

    dir_page->SetBucketPageId(bucket_idx, img_page_id);
    // 修改之后global 的也可能会变化，如果每个localdepth 都小于global depth，global depth应该等于其中最大值
    uint32_t max_glb_d = 0;
    uint32_t upper_index = 1 << dir_page->GetGlobalDepth();
    for (uint32_t cur_idx = 0; cur_idx < upper_index; cur_idx++) {
      if ((cur_idx & mask) == (bucket_idx & mask)) {
        dir_page->SetBucketPageId(cur_idx, img_page_id);
        dir_page->SetLocalDepth(cur_idx, old_local_d - 1);
      }
      max_glb_d = std::max(max_glb_d, dir_page->GetLocalDepth(cur_idx));
    }
    dir_page->SetGlobalDepth(max_glb_d);
  }

  buffer_pool_manager_->UnpinPage(page_id, ret);
  return ret;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
