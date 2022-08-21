//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx), plan_(plan), left_child_(std::move(left_child)), right_child_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
    // HashJoinPlanNode defines the GetLeftJoinKey() and GetRightJoinKey()
    // we hash the right keys are traverse the left keys
    // different between plan executor expression, first we need the key express
    ht_.clear();
    Tuple right_tuple;
    RID right_rid;
    right_child_->Init();
    // column exp找到右边值的类型之后，用hash表处理，这是动态类型吗？NO，Hash Value类型变量
    while (right_child_->Next(&right_tuple, &right_rid)) {
        // 需要这个类型的定义吗？没有？
        Value key_value = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple, right_child_->GetOutputSchema());
        SingleKey key{key_value};
        ht_[key].push_back(right_tuple);
        key_value.GetTypeId();
    }
    left_child_->Init();

    Tuple left_tuple;
    RID left_rid;
    result_set_.clear();
    while (left_child_->Next(&left_tuple, &left_rid)) {
        Value key_value = plan_->LeftJoinKeyExpression()->Evaluate(&left_tuple, left_child_->GetOutputSchema());
        SingleKey key{key_value};
        if (ht_.find(key) != ht_.end()) {
            for (const auto& rt : ht_[key]) {
                std::vector<Value> output;
                for (const auto &col : GetOutputSchema()->GetColumns()) {
                    output.push_back(col.GetExpr()->EvaluateJoin(&left_tuple, left_child_->GetOutputSchema(),
                                                                &rt, right_child_->GetOutputSchema()));
                }
                result_set_.emplace_back(output, GetOutputSchema());
            }
        }
    }
    result_set_iter_ = result_set_.begin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if (result_set_iter_ != result_set_.end()) {
        *tuple = *result_set_iter_;
        ++result_set_iter_;
        return true;
    }
    return false;
}

}  // namespace bustub
