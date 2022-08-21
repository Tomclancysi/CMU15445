//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
    plan_ = plan;
}

void SeqScanExecutor::Init() {
    result_set_.clear();
    // read the source code, then you know
    auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
    // from the table heap get the table iter from begin to end, this const point will fail.
    TableIterator iter = table_info->table_->Begin(exec_ctx_->GetTransaction());
    TableIterator end = table_info->table_->End();
    Schema table_schema = table_info->schema_;

    while (iter != end) {
        const Tuple &tuple = *iter;
        // wtf where is the scheme of query?
        if (plan_->GetPredicate() == nullptr || plan_->GetPredicate()->Evaluate(&tuple, &table_schema).GetAs<bool>()) {
            // do we need use output schema? no? maybe is yes, i find how to use this
            // however it's quite no!
            // auto temp_value = *iter;
            // std::vector<Value> output;
            // for (const auto& col : GetOutputSchema()->GetColumns()) {
            //     // output.push_back(col.GetExpr()->Evaluate(&temp_value, GetOutputSchema()));
            //     auto sche = &table_info->schema_;
            //     Value v = tuple.GetValue(sche, sche->GetColIdx(col.GetName()));
            //     output.push_back(v);
            // }
            // result_set_.push_back(Tuple(output, GetOutputSchema()));
            result_set_.push_back(*iter);
        }
        ++iter;
    }
    cursor_ = result_set_.begin();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if (cursor_ != result_set_.end()) {
        *tuple = *cursor_;
        *rid = (*cursor_).GetRid();
        ++cursor_;
        return true;
    } else {
        return false;
    }
}

}  // namespace bustub
