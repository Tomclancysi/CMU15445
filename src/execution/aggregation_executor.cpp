//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), child_(std::move(child)), aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
    aht_iterator_(aht_.Begin())  {
        plan_ = plan;
    }

void AggregationExecutor::Init() {
    child_->Init();
    // 遍历child，把每个tuple放到aht里面？
    Tuple tuple;
    RID rid;
    while (child_->Next(&tuple, &rid)) {
        if (plan_->GetGroupBys().empty()) {
            // 没有group by子句,对所有结果聚合，取出需要聚合的列，这里应该是column accessor
            AggregateValue aggv;
            for (const auto &exp : plan_->GetAggregates()) {
                aggv.aggregates_.push_back(exp->Evaluate(&tuple, child_->GetOutputSchema()));
            }
            aht_.InsertCombine(default_key_, aggv);
        } else {
            AggregateKey aggk;
            for (const auto &exp : plan_->GetGroupBys()) {
                aggk.group_bys_.push_back(exp->Evaluate(&tuple, child_->GetOutputSchema()));
            }
            AggregateValue aggv;
            for (const auto &exp : plan_->GetAggregates()) {
                aggv.aggregates_.push_back(exp->Evaluate(&tuple, child_->GetOutputSchema()));
            }
            aht_.InsertCombine(aggk, aggv);
        }
    }
    // 根据having子句，筛选掉不符合题意的值，可以根据组的key或聚合的value筛选
    if (plan_->GetHaving() != nullptr) {
        std::vector<AggregateKey> group_wait_delete;
        auto having = plan_->GetHaving();
        for (auto iter = aht_.Begin(); iter != aht_.End(); ++iter) {
            if (!having->EvaluateAggregate(iter.Key().group_bys_, iter.Val().aggregates_).GetAs<bool>()) {
                group_wait_delete.push_back(iter.Key());
            }
        }
        for (const auto &k : group_wait_delete) {
            aht_.RemoveKey(k);
        }
    }
    aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if (aht_iterator_ != aht_.End()) {
        // const auto &aggv = aht_iterator_.Val();
        // *tuple = Tuple(aggv.aggregates_, GetOutputSchema());
        std::vector<Value> output;
        for (const auto &col : GetOutputSchema()->GetColumns()) {
            output.push_back(col.GetExpr()->EvaluateAggregate(aht_iterator_.Key().group_bys_,
            aht_iterator_.Val().aggregates_));
        }
        *tuple = Tuple(output, GetOutputSchema());
        ++aht_iterator_;
        return true;
    }
    return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
