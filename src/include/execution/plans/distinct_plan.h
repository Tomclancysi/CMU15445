//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_plan.h
//
// Identification: src/include/execution/plans/distinct_plan.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"

namespace bustub {

/**
 * Distinct removes duplicate rows from the output of a child node.
 */
class DistinctPlanNode : public AbstractPlanNode {
 public:
  /**
   * Construct a new DistinctPlanNode instance.
   * @param child The child plan from which tuples are obtained
   */
  DistinctPlanNode(const Schema *output_schema, const AbstractPlanNode *child)
      : AbstractPlanNode(output_schema, {child}) {}

  /** @return The type of the plan node */
  auto GetType() const -> PlanType override { return PlanType::Distinct; }

  /** @return The child plan node */
  auto GetChildPlan() const -> const AbstractPlanNode * {
    BUSTUB_ASSERT(GetChildren().size() == 1, "Distinct should have at most one child plan.");
    return GetChildAt(0);
  }
};

struct MultiKey
{
  std::vector<Value> vals_;
  auto operator==(const MultiKey &other) const -> bool {
    for (std::size_t i = 0; i < vals_.size(); ++i) {
      if (vals_[i].CompareEquals(other.vals_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

}  // namespace bustub

namespace std {

/** Implements std::hash on MultiKey */
template <>
struct hash<bustub::MultiKey> {
  auto operator()(const bustub::MultiKey &agg_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : agg_key.vals_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}