//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_page.h
//
// Identification: src/include/storage/page/b_plus_tree_page.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "common/rid.h"
#include "storage/index/generic_key.h"

namespace bustub {

#define MappingType std::pair<KeyType, ValueType>

#define INDEX_TEMPLATE_ARGUMENTS template <typename KeyType, typename ValueType, typename KeyComparator>
/*
内部节点的value是page_id_t，指向下一级节点所在的页
叶子节点的value存数据，但并不直接存数据，而是存一个指向数据位置的指针，即RID

RID = {page_id, slot_num},用来唯一定位 tuple 在数据页里的位置
只有 page_id：你知道数据在第 100 页，但不知道在第几行。
有 RID：你知道数据在第 100 页的第 5 个槽位，能直接拿到 tuple。
*/

// define page type enum
enum class IndexPageType { INVALID_INDEX_PAGE = 0, LEAF_PAGE, INTERNAL_PAGE };

/**
 * Both internal and leaf page are inherited from this page.
 *
 * It actually serves as a header part for each B+ tree page and
 * contains information shared by both leaf page and internal page.
 *
 * Header format (size in byte, 12 bytes in total):
 * ---------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |  ...   |
 * ---------------------------------------------------------
 */
class BPlusTreePage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreePage() = delete;
  BPlusTreePage(const BPlusTreePage &other) = delete;
  ~BPlusTreePage() = delete;

  auto IsLeafPage() const -> bool;
  auto IsRootPage() const -> bool;
  void SetPageType(IndexPageType page_type);

  auto GetSize() const -> int;
  void SetSize(int size);
  void ChangeSizeBy(int amount);

  auto GetMaxSize() const -> int;
  void SetMaxSize(int max_size);
  auto GetMinSize() const -> int;

  /*
   * TODO(P2): Remove __attribute__((__unused__)) if you intend to use the fields.
   */
 private:
  // Member variables, attributes that both internal and leaf page share
  IndexPageType page_type_;
  // Number of key & value pairs in a page
  int size_;
  // Max number of key & value pairs in a page
  // 一个节点最多能装多少个entry，对于内部节点，key数为max_size_ - 1，对于叶子节点，key数为max_size_
  // 和Max Degree是两个不同的概念
  int max_size_;
};

}  // namespace bustub
