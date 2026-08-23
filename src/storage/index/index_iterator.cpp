//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.cpp
//
// Identification: src/storage/index/index_iterator.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/**
 * @note you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t page_id, int index)
    : bpm_(bpm), page_id_(page_id), index_(index), result_({KeyType{}, ValueType{}}) {
  if (page_id == INVALID_PAGE_ID) {
    return;
  }
  auto page_guard = bpm_->ReadPage(page_id_);
  auto leaf_page = page_guard.template As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  result_.first = leaf_page->KeyAt(index_);
  result_.second = leaf_page->ValueAt(index_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return index_ == -1; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> { return result_; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  index_++;
  auto page_guard = bpm_->ReadPage(page_id_);
  auto leaf_page = page_guard.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  // auto leaf_page = page_guard.template As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  if (index_ >= leaf_page->GetSize()) {
    // 需要翻页
    page_id_ = leaf_page->GetNextPageId();
    if (page_id_ == INVALID_PAGE_ID) {
      // 已经到达末尾
      index_ = -1;
      return *this;
    }
    index_ = 0;
    page_guard = bpm_->ReadPage(page_id_);
    leaf_page = page_guard.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  }
  result_.first = leaf_page->KeyAt(index_);
  result_.second = leaf_page->ValueAt(index_);
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
