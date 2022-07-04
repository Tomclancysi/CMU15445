//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stdexcept>
#include <vector>

#include "common/exception.h"

namespace bustub {

/**
 * The Matrix type defines a common
 * interface for matrix operations.
 */
template <typename T>
class Matrix {
 protected:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new Matrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   *
   */
  Matrix(int rows, int cols) :rows_(rows), cols_(cols) {
    this->linear_ = reinterpret_cast<T*>(malloc(rows*cols*sizeof(T)));
  }

  /** The number of rows in the matrix */
  int rows_;
  /** The number of columns in the matrix */
  int cols_;

  /**
   * TODO(P0): Allocate the array in the constructor.
   * TODO(P0): Deallocate the array in the destructor.
   * A flattened array containing the elements of the matrix.
   */
  T *linear_;

 public:
  /** @return The number of rows in the matrix */
  virtual auto GetRowCount() const -> int = 0;

  /** @return The number of columns in the matrix */
  virtual auto GetColumnCount() const -> int = 0;

  /**
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual auto GetElement(int i, int j) const -> T = 0;

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual void SetElement(int i, int j, T val) = 0;

  /**
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  virtual void FillFrom(const std::vector<T> &source) = 0;

  /**
   * Destroy a matrix instance.
   * TODO(P0): Add implementation
   */
  virtual ~Matrix(){
    free(this->linear_);
  }
};

/**
 * The RowMatrix type is a concrete matrix implementation.
 * It implements the interface defined by the Matrix type.
 */
template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new RowMatrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   */
  RowMatrix(int rows, int cols) : Matrix<T>(rows, cols) {
    this->data_ = reinterpret_cast<T**>(malloc(sizeof(void*) * rows));
    for (int i = 0; i < rows; ++i) {
      this->data_[i] = this->linear_ + cols * i;
    }
  }

  /**
   * TODO(P0): Add implementation
   * @return The number of rows in the matrix
   */
  auto GetRowCount() const -> int override {
    return this->rows_;
  }

  /**
   * TODO(P0): Add implementation
   * @return The number of columns in the matrix
   */
  auto GetColumnCount() const -> int override {
    return this->cols_;
  }

  /**
   * TODO(P0): Add implementation
   *
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  auto GetElement(int i, int j) const -> T override {
    if (i >= 0 && i < this->rows_ && j >= 0 && j < this->cols_)
      return data_[i][j];
    throw Exception(ExceptionType::OUT_OF_RANGE, "index out of range");
  }

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  void SetElement(int i, int j, T val) override {
    if (i >= 0 && i < this->rows_ && j >= 0 && j < this->cols_)
      data_[i][j] = val;
    else
      throw Exception(ExceptionType::OUT_OF_RANGE, "index out of range");
  }

  /**
   * TODO(P0): Add implementation
   *
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  void FillFrom(const std::vector<T> &source) override {
    if (static_cast<int>(source.size()) != this->rows_ * this->cols_)
      throw Exception(ExceptionType::OUT_OF_RANGE, "index out of range");
    for (size_t i = 0; i < source.size(); ++i)
      this->linear_[i] = source[i];
    for (size_t i = 0; i < source.size(); ++i)
      printf("%d ", this->linear_[i]);
    printf("\n");
  }

  /**
   * TODO(P0): Add implementation
   *
   * Destroy a RowMatrix instance.
   */
  ~RowMatrix() override {
    free(this->data_);
  }

 private:
  /**
   * A 2D array containing the elements of the matrix in row-major format.
   *
   * TODO(P0):
   * - Allocate the array of row pointers in the constructor.
   * - Use these pointers to point to corresponding elements of the `linear` array.
   * - Don't forget to deallocate the array in the destructor.
   */
  T **data_;
};

/**
 * The RowMatrixOperations class defines operations
 * that may be performed on instances of `RowMatrix`.
 */
template <typename T>
class RowMatrixOperations {
 public:
  /**
   * Compute (`matrixA` + `matrixB`) and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix addition
   */
  static auto Add(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) -> std::unique_ptr<RowMatrix<T>> {
    // TODO(P0): Add implementation
    int rowA = matrixA->GetRowCount(), colA = matrixA->GetColumnCount();
    int rowB = matrixB->GetRowCount(), colB = matrixB->GetColumnCount();
    if (rowA != rowB || colA != colB)
      throw Exception(ExceptionType::OUT_OF_RANGE, "index out of range");
    auto res = new RowMatrix<T>(rowA, colA);
    for (int i = 0; i < rowA; ++i)
      for (int j = 0; j < colA; ++j)
        res->SetElement(i, j, matrixA->GetElement(i, j) + matrixB->GetElement(i, j));
    return std::unique_ptr<RowMatrix<T>>(res);
  }

  /**
   * Compute the matrix multiplication (`matrixA` * `matrixB` and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix multiplication
   */
  static auto Multiply(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) -> std::unique_ptr<RowMatrix<T>> {
    // TODO(P0): Add implementation
    int rowA = matrixA->GetRowCount(), colA = matrixA->GetColumnCount();
    int rowB = matrixB->GetRowCount(), colB = matrixB->GetColumnCount();
    if (colA != rowB)
      throw Exception(ExceptionType::OUT_OF_RANGE, "index out of range");
    auto res = new RowMatrix<T>(rowA, colB);
    for (int i = 0; i < rowA; i++) {
      for (int j = 0; j < colB; j++) {
        int val = 0;
        for (int k = 0; k < colA; k++) {
          val +=  matrixA->GetElement(i, k) * matrixB->GetElement(k, j);
        }
        res->SetElement(i, j, val);
      }
    }
    return std::unique_ptr<RowMatrix<T>>(res);
  }

  /**
   * Simplified General Matrix Multiply operation. Compute (`matrixA` * `matrixB` + `matrixC`).
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @param matrixC Input matrix
   * @return The result of general matrix multiply
   */
  static auto GEMM(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB, const RowMatrix<T> *matrixC)
      -> std::unique_ptr<RowMatrix<T>> {
    // TODO(P0): Add implementation
    return std::unique_ptr<RowMatrix<T>>(nullptr);
  }
};
}  // namespace bustub
