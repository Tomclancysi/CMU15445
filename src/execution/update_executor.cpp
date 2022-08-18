//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), child_executor_(std::move(child_executor)) {
      plan_ = plan;
    }

void UpdateExecutor::Init() {
  // it must had a child seq
  // child_executor_ = ExecutorFactory::CreateExecutor(exec_ctx_, plan_->GetChildPlan());
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());

  ExecuteUpdate();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  return false;
}

void UpdateExecutor::ExecuteUpdate() {
  Tuple temp_tuple;
  Tuple updated_tuple;
  RID temp_rid;
  auto catalog = exec_ctx_->GetCatalog();
  auto indexes_info = catalog->GetTableIndexes(table_info_->name_);
  // 1. get tuple and rid from its child node
  while (child_executor_->Next(&temp_tuple, &temp_rid)) {
    updated_tuple = GenerateUpdatedTuple(temp_tuple);
    // 2. update the table
    bool f = table_info_->table_->UpdateTuple(updated_tuple, temp_rid, exec_ctx_->GetTransaction());
    // 3. re-index this tuple, cause its value are changed
    if (f) {
      for (const auto &index_info : indexes_info) {
          const auto & index = index_info->index_;
          auto key = temp_tuple.KeyFromTuple(*child_executor_->GetOutputSchema(), *index->GetKeySchema(), index->GetKeyAttrs());
          index_info->index_->DeleteEntry(key, temp_rid, exec_ctx_->GetTransaction());
          index_info->index_->InsertEntry(key, temp_rid, exec_ctx_->GetTransaction());
      }
    }
  }
}

auto UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) -> Tuple {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
