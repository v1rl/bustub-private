//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog_presto.cpp
//
// Identification: src/primer/hyperloglog_presto.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog_presto.h"
#include <bitset>

namespace bustub {

/** @brief Parameterized constructor. */
template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits) {
  cardinality_ = 0;
  if (n_leading_bits < 0) {
    valid_ = false;
    return;
  }
  n_leading_bits_ = n_leading_bits;
  dense_bucket_.assign(1 << n_leading_bits, {});
}

/** @brief Element is added for HLL calculation. */
template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  if (!valid_) {
    return;
  }
  hash_t hash = CalculateHash(val);
  uint16_t bucket_num = hash >> (64 - n_leading_bits_);
  uint8_t p = 0;
  for (int i = 0; i < 64 - n_leading_bits_; i++) {
    if (((hash >> i) & 1) == 0) {
      p++;
    } else {
      break;
    }
  }

  auto dense_val = static_cast<uint8_t>(dense_bucket_[bucket_num].to_ulong());
  uint8_t overflow_val =
      overflow_bucket_.count(bucket_num) > 0 ? static_cast<uint8_t>(overflow_bucket_[bucket_num].to_ulong()) : 0;

  uint8_t old_val = (overflow_val << DENSE_BUCKET_SIZE) + dense_val;
  if (p > old_val) {
    dense_bucket_[bucket_num] = std::bitset<DENSE_BUCKET_SIZE>(p & 0x0F);
    p >>= 4;
    if (p > 0) {
      overflow_bucket_[bucket_num] = std::bitset<OVERFLOW_BUCKET_SIZE>(p);
    }
  }
}

/** @brief Function to compute cardinality. */
template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  if (!valid_) {
    return;
  }
  std::lock_guard<std::mutex> lock(mtx_);
  int m = 1 << n_leading_bits_;
  double res = 0.0;
  for (int i = 0; i < m; i++) {
    auto dense_val = static_cast<uint8_t>(dense_bucket_[i].to_ulong());
    uint8_t overflow_val = overflow_bucket_.count(i) > 0 ? static_cast<uint8_t>(overflow_bucket_[i].to_ulong()) : 0;
    uint8_t val = (overflow_val << 4) + dense_val;
    res += 1.0 / static_cast<double>(static_cast<__uint128_t>(1) << val);
  }
  cardinality_ = CONSTANT * m / res * m;
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub
