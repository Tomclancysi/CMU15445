//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), left_executor_(std::move(left_executor)), right_executor_(std::move(right_executor)) {
        plan_ = plan;
    }

void NestedLoopJoinExecutor::Init() {
    left_executor_->Init();
    right_executor_->Init();
    // right_table_end_ = true;
    left_null_ = !left_executor_->Next(&left_tuple_, &left_rid_);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if (left_null_) {
        return false;
    }
    Tuple right_tuple;
    RID right_rid;

    if (right_executor_->Next(&right_tuple, &right_rid)) {
        auto left_sch = left_executor_->GetOutputSchema();
        auto right_sch = right_executor_->GetOutputSchema();
        bool f =plan_->Predicate()->EvaluateJoin(&left_tuple_, left_sch, &right_tuple, right_sch).GetAs<bool>();
        if (f) {
            
            return true;
        }
    }
    if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
        return false;
    }
    // next loop
    right_executor_->Init();
    return this->Next(tuple, rid);
}

}  // namespace bustub
