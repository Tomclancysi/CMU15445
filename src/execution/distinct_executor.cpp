//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), child_executor_(std::move(child_executor)) {
        plan_ = plan;
    }

void DistinctExecutor::Init() {
    child_executor_->Init();
    exist_.clear();
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
        // 从tuple中取出需要distinct的key，也就是output scheme
        MultiKey mk;
        for (const auto &col : GetOutputSchema()->GetColumns()) {
            // 根据名字查scheme，从child中取出来
            auto sche = child_executor_->GetOutputSchema();
            Value v = tuple.GetValue(sche, sche->GetColIdx(col.GetName()));
            mk.vals_.push_back(v);
        }
        if (exist_.count(mk) == 0) {
            printf("insert key to ht %d\n", mk.vals_[0].GetAs<int>());
            exist_[mk] = rid;
        }
    }
    iter_ = exist_.begin();
    printf("The size of all is %lu\n",exist_.size());
}

auto DistinctExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if (iter_ != exist_.end()) {
        *tuple = Tuple(iter_->first.vals_, GetOutputSchema());
        *rid = iter_->second;
        ++iter_;
        return true;
    }
    return false;
}

}  // namespace bustub
