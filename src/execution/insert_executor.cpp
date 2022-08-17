//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executor_factory.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
        plan_ = plan;
    }

void InsertExecutor::Init() {
    if (plan_->IsRawInsert()) {
        raw_values_length_ = plan_->RawValues().size();
        raw_values_index_ = 0;
    } else {
        child_executor_ = ExecutorFactory::CreateExecutor(exec_ctx_, plan_->GetChildPlan());
        child_executor_->Init();
    }
    inserted_table_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
    Tuple temp_tuple;
    RID temp_rid;
    // the value maybe from the plan node(directly) or select plan from child node
    if (plan_->IsRawInsert()) {
        if (raw_values_index_ < raw_values_length_) {
            // 1. transfrom vector value to tuple
            const auto &raw_values = plan_->RawValuesAt(raw_values_index_++);
            temp_tuple = Tuple(raw_values, &inserted_table_->schema_);
            // 2. assume the value match the table schema, part insert is not allowed
            if (inserted_table_->table_->InsertTuple(temp_tuple, &temp_rid, exec_ctx_->GetTransaction())) {
                // 3. update index of table
                auto catalog = exec_ctx_->GetCatalog();
                auto indexes_info = catalog->GetTableIndexes(inserted_table_->name_);
                for (const auto &index_info : indexes_info) {
                    index_info->index_->InsertEntry(temp_tuple, temp_rid, exec_ctx_->GetTransaction());
                }
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    } else {
        if (child_executor_->Next(&temp_tuple, &temp_rid)) {
            if (inserted_table_->table_->InsertTuple(temp_tuple, &temp_rid, exec_ctx_->GetTransaction())) {
                // 3. update index of table
                auto catalog = exec_ctx_->GetCatalog();
                auto indexes_info = catalog->GetTableIndexes(inserted_table_->name_);
                for (const auto &index_info : indexes_info) {
                    index_info->index_->InsertEntry(temp_tuple, temp_rid, exec_ctx_->GetTransaction());
                }
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }
}

}  // namespace bustub
