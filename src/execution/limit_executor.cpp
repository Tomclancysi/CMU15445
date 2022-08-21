//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), child_executor_(std::move(child_executor)) {
        plan_ = plan;
        limit_ = 0;
    }

void LimitExecutor::Init() {
    child_executor_->Init();
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if (limit_ < plan_->GetLimit() && child_executor_->Next(tuple, rid)) {
        ++limit_;
        return true;
    }
    return false;
}

}  // namespace bustub
