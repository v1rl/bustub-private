//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return the frame ID if a frame is successfully evicted, or `std::nullopt` if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard<std::mutex> latch(latch_);
  if (!cold_frames_.empty()) {
    frame_id_t fid = -1;
    for (const auto &cold_fid : cold_frames_) {
      auto it = node_store_.find(cold_fid);
			// 判断是否存在于哈希表中是必要的吗 ？
      if (it != node_store_.end() && it->second.is_evictable_) {
        fid = cold_fid;
        break;
      }
    }
    if (fid != -1) {
      cold_frames_.remove(fid);
      node_store_.erase(fid);
      curr_size_--;
      return fid;
    }
  }
  if (!hot_frames_.empty()) {
    int mx = 0;
    int max_fid = -1;
    for (const auto &fid : hot_frames_) {
      auto it = node_store_.find(fid);
      if (it != node_store_.end() && it->second.is_evictable_) {
        int backward_k_distance = current_timestamp_ - it->second.history_.front();
        if (backward_k_distance > mx) {
          mx = backward_k_distance;
          max_fid = fid;
        }
      }
    }
    if (max_fid != -1) {
      hot_frames_.remove(max_fid);
      node_store_.erase(max_fid);
      curr_size_--;
      return max_fid;
    }
  }
  return std::nullopt;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> latch(latch_);
  BUSTUB_ENSURE(frame_id >= 0 && static_cast<size_t>(frame_id) < replacer_size_,
                "Frame ID is out of bounds for the replacer size.");
  if (node_store_.count(frame_id) == 0) {
    // 直接用等于号赋值是否可行 ？
    node_store_.emplace(frame_id, LRUKNode());
    cold_frames_.push_back(frame_id);
  }
  node_store_[frame_id].history_.push_back(current_timestamp_++);
  if (node_store_[frame_id].history_.size() == k_) {
    cold_frames_.remove(frame_id);
    hot_frames_.push_back(frame_id);
  }
  if (node_store_[frame_id].history_.size() > k_) {
    node_store_[frame_id].history_.pop_front();
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> latch(latch_);
  BUSTUB_ENSURE(frame_id >= 0 && static_cast<size_t>(frame_id) < replacer_size_,
                "Frame ID is out of bounds for the replacer size.");
  if (node_store_.count(frame_id) == 0) {
    return;  // Frame ID not found, nothing to do.
  }
  if (set_evictable) {
    if (!node_store_[frame_id].is_evictable_) {
      node_store_[frame_id].is_evictable_ = true;
      curr_size_++;
    }
  } else {
    if (node_store_[frame_id].is_evictable_) {
      node_store_[frame_id].is_evictable_ = false;
      curr_size_--;
    }
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> latch(latch_);
  if (node_store_.count(frame_id) == 0) {
    return;
  }
  BUSTUB_ENSURE(node_store_[frame_id].is_evictable_, "Cannot remove a non-evictable frame.");

  if (node_store_[frame_id].history_.size() < k_) {
    cold_frames_.remove(frame_id);
  } else {
    hot_frames_.remove(frame_id);
  }

  node_store_.erase(frame_id);
  curr_size_--;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> latch(latch_);
  return curr_size_;
}

}  // namespace bustub