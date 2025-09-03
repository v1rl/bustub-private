//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog.cpp
//
// Identification: src/primer/hyperloglog.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog.h"
#include <bitset>
#include "common/config.h"
#include "primer/hyperloglog_presto.h"

namespace bustub {

/** @brief Parameterized constructor. */
template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) {
  cardinality_ = 0;
  if (n_bits < 0) {
    valid_ = false;
    return;
  }
  n_bits_ = n_bits;
  bucket_ = std::vector<uint8_t>(1 << n_bits);
}

/**
 * @brief Function that computes binary.
 *
 * @param[in] hash
 * @returns binary of a given hash
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  /** @TODO(student) Implement this function! */
  auto turn = std::bitset<BITSET_CAPACITY>(hash);
  return turn;
}

/**
 * @brief Function that computes leading zeros.
 *
 * @param[in] bset - binary values of a given bitset
 * @returns leading zeros of given binary set
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  /** @TODO(student) Implement this function! */
  for (int i = BITSET_CAPACITY - n_bits_ - 1; i >= 0; i--) {
    if (bset[i]) {
      return BITSET_CAPACITY - n_bits_ - i;
    }
  }
  return BITSET_CAPACITY - n_bits_;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::GetBucketValue(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  /** @TODO(student) Implement this function! */
  uint64_t value = 0;
  for (int i = 0; i < n_bits_; i++) {
    value <<= 1;
    value |= static_cast<uint64_t>(bset[BITSET_CAPACITY - 1 - i]);
  }
  return value;
}

/**
 * @brief Adds a value into the HyperLogLog.
 *
 * @param[in] val - value that's added into hyperloglog
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  if (!valid_) {
    return;
  }
  hash_t hash = CalculateHash(val);
  auto bset = ComputeBinary(hash);
  auto p = PositionOfLeftmostOne(bset);
  auto bucket_num = GetBucketValue(bset);
  bucket_[bucket_num] = std::max<uint8_t>(bucket_[bucket_num], p);
}

/**
 * @brief Function that computes cardinality.
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  if (!valid_) {
    return;
  }
  std::lock_guard<std::mutex> lock(mtx_);
  int m = 1 << n_bits_;
  double res = 0.0;
  for (int i = 0; i < m; i++) {
    res += 1.0 / static_cast<double>(1ULL << bucket_[i]);
  }
  cardinality_ = CONSTANT * m * m * (1.0 / res);
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
