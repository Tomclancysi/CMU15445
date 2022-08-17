//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), child_executor_(std::move(child_executor)) {
        plan_ = plan;
    }

void DeleteExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());

  ExecuteDelete();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { return false; }

void DeleteExecutor::ExecuteDelete() {
  Tuple temp_tuple;
  RID temp_rid;
  auto catalog = exec_ctx_->GetCatalog();
  auto indexes_info = catalog->GetTableIndexes(table_info_->name_);
  // 1. get tuple and rid from its child node
  while (child_executor_->Next(&temp_tuple, &temp_rid)) {
    for (const auto &index_info : indexes_info) {
        index_info->index_->DeleteEntry(temp_tuple, temp_rid, nullptr);
    }
  }
}

}  // namespace bustub
