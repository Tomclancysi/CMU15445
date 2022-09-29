//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

#include <utility>
#include <vector>

namespace bustub {

auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
  // record级别的锁，共享锁的集合里插入这个rid，有啥用？ shared就是读锁
  // 1. return false if the transaction is aborted; and
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // 2. *block on wait*, return true when the lock request is granted; and
  if (txn->GetSharedLockSet()->count(rid)) {
    return true; // already have
  }
  {
    std::unique_lock<std::mutex> ulk(latch_);
    auto &lrq = lock_table_[rid];
    auto check = [&](){
      // chech if lrq has exlusive lock
      bool ret = true;
      for (const auto& req : lrq.request_queue_) {
        if (req.lock_mode_ == LockMode::EXCLUSIVE && req.granted_) {
          if (req.txn_id_ > txn->GetTransactionId()) {
            // 虽然可以砍了它，但是还是得循环遍历，看看它是否被移除
            auto trans = TransactionManager::GetTransaction(req.txn_id_);
            trans->SetState(TransactionState::ABORTED);
          }
          ret = false;
        }
      }
      return ret;
    };
    lrq.cv_.wait(ulk, check);
    LockRequest req(txn->GetTransactionId(), LockMode::SHARED);
    req.granted_ = true;
    lrq.request_queue_.push_back(req);
    txn->GetSharedLockSet()->emplace(rid);
    return true;
  }
  // 3. it is undefined behavior to try locking an already locked RID
  return false;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  // 1. return false if the transaction is aborted; and
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // 2. *block on wait*, return true when the lock request is granted; and
  if (txn->GetExclusiveLockSet()->count(rid)) {
    return true; // already have
  }
  {
    std::unique_lock<std::mutex> ulk(latch_);
    auto &lrq = lock_table_[rid];
    auto check = [&](){
      // chech if lrq is empty
      bool ret = true;
      for (const auto& req : lrq.request_queue_) {
        if (req.granted_) {
          if (req.txn_id_ > txn->GetTransactionId()) {
            auto trans = TransactionManager::GetTransaction(req.txn_id_);
            trans->SetState(TransactionState::ABORTED);
          }
          ret = false;
        }
      }
      return ret;
    };
    lrq.cv_.wait(ulk, check);
    LockRequest req(txn->GetTransactionId(), LockMode::EXCLUSIVE);
    req.granted_ = true;
    lrq.request_queue_.push_back(req);
    txn->GetExclusiveLockSet()->emplace(rid);
    return true;
  }
  // 3. it is undefined behavior to try locking an already locked RID
  return false;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::SHRINKING) {
    return false;
  }
  // 这个过程并不是直接就能实现的，首先要判断锁能否满足了
  std::unique_lock<std::mutex> ulk(latch_);
  auto &lrq = lock_table_[rid];
  auto &que = lrq.request_queue_;
  auto iter = std::find_if(que.begin(), que.end(), [&](const LockRequest &x){
      return x.txn_id_ == txn->GetTransactionId();
  });
  if (iter->lock_mode_ == LockMode::EXCLUSIVE) {
    return true;
  }
  auto check = [&](){
    bool ret = true;
    for(const auto &req : lrq.request_queue_) {
      if (req.txn_id_ != txn->GetTransactionId() && req.granted_) {
          if (req.txn_id_ > txn->GetTransactionId()) {
            auto trans = TransactionManager::GetTransaction(req.txn_id_);
            trans->SetState(TransactionState::ABORTED);
          }
        ret = false;
      }
    }
    return ret;
  };
  lrq.cv_.wait(ulk, check);

  // 没有其他人读写这玩意了，我就可以升级为写锁
  iter->lock_mode_ = LockMode::EXCLUSIVE;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  if(txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }
  {
    std::unique_lock<std::mutex> ulk(latch_);
    auto &lrq = lock_table_[rid];
    // remove the request from list
    auto &que = lrq.request_queue_;
    auto iter = std::find_if(que.begin(), que.end(), [&](const LockRequest &x){
      return x.txn_id_ == txn->GetTransactionId();
    });
    que.erase(iter);
    ulk.unlock();
    lrq.cv_.notify_all();
  }
  return true;
}

}  // namespace bustub
