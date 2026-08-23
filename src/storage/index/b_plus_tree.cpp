//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"
#include <optional>
#include "storage/index/b_plus_tree_debug.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/**
 * @brief Helper function to decide whether current b+tree is empty
 * @return Returns true if this B+ tree has no keys and values.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  // 从磁盘读入BPlusTreeHeaderPage
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  // 必须保证 GetData() 指向的那块内存 确实是按 BPlusTreeHeaderPage 的结构写进去的，否则访问就会错位
  auto head_page = guard.As<BPlusTreeHeaderPage>();
  return head_page->root_page_id_ == INVALID_PAGE_ID;
}

// 该函数用于在 B+ 树的节点中查找指定的键，对于叶子节点找到匹配键时返回对应索引；对于内部节点返回导航到子节点的索引。
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::KeyBinarySearch(const BPlusTreePage *page, const KeyType &key) -> int {
  if (page->IsLeafPage()) {
    /*
      static_cast 只是 类型转换，不会改变内存，也不会新分配/释放内存。
      转换后，leaf_page 和原来的 page 指向同一块内存。
      它们指针值相同，只是「编译器认为」它们的类型不同。
    */
    auto leaf_page = static_cast<const LeafPage *>(page);
    int l = 0;
    int r = leaf_page->GetSize() - 1;
    // 寻找第一个大于等于当前key的key
    while (l < r) {
      int mid = (l + r) >> 1;
      if (comparator_(leaf_page->KeyAt(mid), key) >= 0) {
        r = mid;
      } else {
        l = mid + 1;
      }
    }
    if (l < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(l), key) == 0) {
      return l;
    }
    return -1;
  }

  auto internal_page = static_cast<const InternalPage *>(page);
  int l = 1;                         // 忽略第0个key
  int r = internal_page->GetSize();  // 多一个哨兵节点
  // 寻找第一个大于当前key的key
  while (l < r) {
    int mid = (l + r) >> 1;
    if (comparator_(internal_page->KeyAt(mid), key) > 0) {
      r = mid;
    } else {
      l = mid + 1;
    }
  }
  return l - 1;
  // 2 5 11
}

// 辅助 insert 函数查找目标 key 要在叶子节点中插入的位置。
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IndexBinarySearchLeaf(LeafPage *page, const KeyType &key) -> int {
  int l = 0;
  int r = page->GetSize();  // 多一个哨兵节点
  // 寻找第一个大于等于当前key的key
  while (l < r) {
    int mid = (l + r) >> 1;
    if (comparator_(page->KeyAt(mid), key) >= 0) {
      r = mid;
    } else {
      l = mid + 1;
    }
  }
  return l;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BorrowFromLeft(BPlusTreePage *page, BPlusTreePage *left_page, BPlusTreePage *parent_page,
                                    int index) {
  // 父节点一定是内部节点
  auto parent_internal_page = static_cast<InternalPage *>(parent_page);
  int size = page->GetSize();
  int left_size = left_page->GetSize();
  if (page->IsLeafPage()) {
    // leaf node
    auto leaf_page = static_cast<LeafPage *>(page);
    auto left_leaf_page = static_cast<LeafPage *>(left_page);
    // 1. 当前结点腾出一个位置
    for (int i = size - 1; i >= 0; i--) {
      leaf_page->SetKeyAt(i + 1, leaf_page->KeyAt(i));
      leaf_page->SetValueAt(i + 1, leaf_page->ValueAt(i));
    }
    // 2. 借左兄弟最后一个数据(key&value)放在第一个位置
    leaf_page->SetKeyAt(0, left_leaf_page->KeyAt(left_size - 1));
    leaf_page->SetValueAt(0, left_leaf_page->ValueAt(left_size - 1));
    leaf_page->ChangeSizeBy(1);
    left_leaf_page->ChangeSizeBy(-1);
    // 3. 更新父结点的key为当前结点的第一个key
    parent_internal_page->SetKeyAt(index, leaf_page->KeyAt(0));
  } else {
    // internal node
    auto internal_page = static_cast<InternalPage *>(page);
    auto left_internal_page = static_cast<InternalPage *>(left_page);
    // 1. 当前结点整体后移一个位置
    for (int i = size - 1; i >= 0; i--) {
      if (i != 0) {
        internal_page->SetKeyAt(i + 1, internal_page->KeyAt(i));
      }
      internal_page->SetValueAt(i + 1, internal_page->ValueAt(i));
    }
    // 2. 借父节点的key放在当前节点的第一个位置
    internal_page->SetKeyAt(1, parent_internal_page->KeyAt(index));
    // 3. 借左兄弟最后一个key放在父节点被借走的key的位置
    parent_internal_page->SetKeyAt(index, left_internal_page->KeyAt(left_size - 1));
    // 4. 借左兄弟最后一个value放在当前节点的第一个位置
    internal_page->SetValueAt(0, left_internal_page->ValueAt(left_size - 1));
    internal_page->ChangeSizeBy(1);
    left_internal_page->ChangeSizeBy(-1);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BorrowFromRight(BPlusTreePage *page, BPlusTreePage *right_page, BPlusTreePage *parent_page,
                                     int index) {
  auto parent_internal_page = static_cast<InternalPage *>(parent_page);
  int size = page->GetSize();
  int right_size = right_page->GetSize();
  if (page->IsLeafPage()) {
    // leaf node
    auto leaf_page = static_cast<LeafPage *>(page);
    auto right_leaf_page = static_cast<LeafPage *>(right_page);
    // 1. 当前结点借右兄弟第一个数据(key&value)放在最后一个位置
    leaf_page->SetKeyAt(size, right_leaf_page->KeyAt(0));
    leaf_page->SetValueAt(size, right_leaf_page->ValueAt(0));
    leaf_page->ChangeSizeBy(1);
    // 2. 右兄弟整体前移一个位置
    for (int i = 0; i < right_size - 1; i++) {
      right_leaf_page->SetKeyAt(i, right_leaf_page->KeyAt(i + 1));
      right_leaf_page->SetValueAt(i, right_leaf_page->ValueAt(i + 1));
    }
    right_leaf_page->ChangeSizeBy(-1);
    // 3. 更新父结点的key为右兄弟的第一个key，注意这里的父节点为index + 1
    parent_internal_page->SetKeyAt(index + 1, right_leaf_page->KeyAt(0));
  } else {
    // internal node
    auto internal_page = static_cast<InternalPage *>(page);
    auto right_internal_page = static_cast<InternalPage *>(right_page);
    // 1. 借父节点的key放在当前节点最后一个位置
    internal_page->SetKeyAt(size, parent_internal_page->KeyAt(index + 1));
    // 2. 借右兄弟第一个key放在父节点被借走的key的位置
    parent_internal_page->SetKeyAt(index + 1, right_internal_page->KeyAt(1));
    // 3. 借右兄弟第一个value放在当前节点最后一个位置
    internal_page->SetValueAt(size, right_internal_page->ValueAt(0));
    internal_page->ChangeSizeBy(1);
    // 4. 右兄弟整体前移一个位置
    for (int i = 0; i < right_size - 1; i++) {
      if (i != 0) {
        right_internal_page->SetKeyAt(i, right_internal_page->KeyAt(i + 1));
      }
      right_internal_page->SetValueAt(i, right_internal_page->ValueAt(i + 1));
    }
    right_internal_page->ChangeSizeBy(-1);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeWithLeft(BPlusTreePage *page, BPlusTreePage *left_page, BPlusTreePage *parent_page,
                                   int index) {
  int left_size = left_page->GetSize();
  int size = page->GetSize();
  int parent_size = parent_page->GetSize();
  auto parent_internal_page = static_cast<InternalPage *>(parent_page);
  if (page->IsLeafPage()) {
    // leaf node
    auto leaf_page = static_cast<LeafPage *>(page);
    auto left_leaf_page = static_cast<LeafPage *>(left_page);
    // 把当前节点所有数据整体搬到左兄弟节点的后面
    for (int i = 0; i < size; i++) {
      left_leaf_page->SetKeyAt(left_size + i, leaf_page->KeyAt(i));
      left_leaf_page->SetValueAt(left_size + i, leaf_page->ValueAt(i));
    }
    left_leaf_page->ChangeSizeBy(size);
    // 更新左兄弟节点的next_page_id为当前节点的next_page_id
    left_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  } else {
    auto internal_page = static_cast<InternalPage *>(page);
    auto left_internal_page = static_cast<InternalPage *>(left_page);
    // 把父节点的key放在左兄弟节点的后面
    left_internal_page->SetKeyAt(left_size, parent_internal_page->KeyAt(index));
    // 把当前节点所有数据整体搬到左兄弟节点的后面
    for (int i = 0; i < size; i++) {
      if (i != 0) {
        left_internal_page->SetKeyAt(left_size + i, internal_page->KeyAt(i));
      }
      left_internal_page->SetValueAt(left_size + i, internal_page->ValueAt(i));
    }
    left_internal_page->ChangeSizeBy(size);
  }

  // 更新父节点，删除index位置的数据并相应地前移
  for (int i = index; i < parent_size - 1; i++) {
    parent_internal_page->SetKeyAt(i, parent_internal_page->KeyAt(i + 1));
    parent_internal_page->SetValueAt(i, parent_internal_page->ValueAt(i + 1));
  }
  parent_internal_page->ChangeSizeBy(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeWithRight(BPlusTreePage *page, BPlusTreePage *right_page, BPlusTreePage *parent_page,
                                    int index) {
  int right_size = right_page->GetSize();
  int size = page->GetSize();
  int parent_size = parent_page->GetSize();
  auto parent_internal_page = static_cast<InternalPage *>(parent_page);
  if (page->IsLeafPage()) {
    // leaf node
    auto leaf_page = static_cast<LeafPage *>(page);
    auto right_leaf_page = static_cast<LeafPage *>(right_page);
    // 把右兄弟所有数据整体搬到当前节点的后面
    for (int i = 0; i < right_size; i++) {
      leaf_page->SetKeyAt(size + i, right_leaf_page->KeyAt(i));
      leaf_page->SetValueAt(size + i, right_leaf_page->ValueAt(i));
    }
    leaf_page->ChangeSizeBy(right_size);
    // 更新左兄弟节点的next_page_id为当前节点的next_page_id
    leaf_page->SetNextPageId(right_leaf_page->GetNextPageId());
  } else {
    auto internal_page = static_cast<InternalPage *>(page);
    auto right_internal_page = static_cast<InternalPage *>(right_page);
    // 把父节点的key放在当前节点的后面
    internal_page->SetKeyAt(size, parent_internal_page->KeyAt(index + 1));
    // 把右兄弟的所有数据整体搬到当前节点的后面
    for (int i = 0; i < right_size; i++) {
      if (i != 0) {
        internal_page->SetKeyAt(size + i, right_internal_page->KeyAt(i));
      }
      internal_page->SetValueAt(size + i, right_internal_page->ValueAt(i));
    }
    internal_page->ChangeSizeBy(right_size);
  }

  // 更新父节点，删除index + 1位置的数据并相应地前移
  for (int i = index + 1; i < parent_size - 1; i++) {
    parent_internal_page->SetKeyAt(i, parent_internal_page->KeyAt(i + 1));
    parent_internal_page->SetValueAt(i, parent_internal_page->ValueAt(i + 1));
  }
  parent_internal_page->ChangeSizeBy(-1);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
 * @brief Return the only value that associated with input key
 *
 * This method is used for point query
 *
 * @param key input key
 * @param[out] result vector that stores the only value that associated with input key, if the value exists
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  // Declaration of context instance. Using the Context is not necessary but advised.
  Context ctx;
  auto guard = bpm_->ReadPage(header_page_id_);
  auto head_page = guard.As<BPlusTreeHeaderPage>();
  // 读入根节点后释放header的锁
  guard.Drop();
  ctx.root_page_id_ = head_page->root_page_id_;
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  // 从根节点开始查找
  ctx.read_set_.emplace_back(bpm_->ReadPage(ctx.root_page_id_));
  auto page = ctx.read_set_.back().As<BPlusTreePage>();
  // 当节点为内部节点时
  while (!page->IsLeafPage()) {
    int index = KeyBinarySearch(page, key);

    // 因为page是const指针，为了保持常量性，类型转换时需要加上const
    auto internal_page = static_cast<const InternalPage *>(page);
    // 根据下标读value得到下一页的page_id
    page_id_t child_page_id = internal_page->ValueAt(index);
    ctx.read_set_.emplace_back(bpm_->ReadPage(child_page_id));
    page = ctx.read_set_.back().As<BPlusTreePage>();
    ctx.read_set_.pop_front();
  }

  // 当节点为叶子节点时
  int index = KeyBinarySearch(page, key);
  if (index == -1) {
    return false;
  }
  auto leaf_page = static_cast<const LeafPage *>(page);
  // 根据下标读value得到对应的RID
  result->push_back(leaf_page->ValueAt(index));
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * @brief Insert constant key & value pair into b+ tree
 *
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 *
 * @param key the key to insert
 * @param value the value associated with key
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  // Declaration of context instance. Using the Context is not necessary but advised.
  Context ctx;

  // 为什么直接用guard不行 ？？
  auto guard = bpm_->WritePage(header_page_id_);
  // 这里可以直接写ctx.header_page_ = std::move(guard);
  ctx.header_page_ = std::make_optional(std::move(guard));
  ctx.root_page_id_ = ctx.header_page_->As<BPlusTreeHeaderPage>()->root_page_id_;

  // (1) 如果tree是空的
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    // 1. 分配一个新页作为根节点
    page_id_t new_page_id = bpm_->NewPage();
    auto root_guard = bpm_->WritePage(new_page_id);
    auto root_page = root_guard.AsMut<LeafPage>();
    root_page->Init(leaf_max_size_);
    // 2. 插入数据
    root_page->SetKeyAt(0, key);
    root_page->SetValueAt(0, value);
    root_page->SetSize(1);
    // 3. 更新header page
    ctx.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_page_id;
    return true;
  }

  // (2) 如果tree不是空的
  // (2.1) 乐观锁
  // (2.1.1) 找到要进行插入操作的叶子结点
  ctx.read_set_.emplace_back(bpm_->ReadPage(ctx.root_page_id_));
  auto page = ctx.read_set_.back().As<BPlusTreePage>();
  // 如果root结点为叶子结点，则将其升级为写锁。这里存在时间空窗，但有header结点锁未释放，提供了线程保护
  if (page->IsLeafPage()) {
    ctx.read_set_.pop_back();
    ctx.write_set_.emplace_back(bpm_->WritePage(ctx.root_page_id_));
    page = ctx.write_set_.back().As<BPlusTreePage>();
  }

  ctx.header_page_ = std::nullopt;

  while (!page->IsLeafPage()) {
    int index = KeyBinarySearch(page, key);
    auto internal_page = static_cast<const InternalPage *>(page);
    auto chlid_page_id = internal_page->ValueAt(index);
    ctx.read_set_.push_back(bpm_->ReadPage(chlid_page_id));
    page = ctx.read_set_.back().As<BPlusTreePage>();
    // 如果当前结点为叶子结点，则在其父结点锁未被释放的情况下，进行读锁向写锁的升级
    // 父结点锁未被释放，保证读写锁升级过程的线程安全
    if (page->IsLeafPage()) {
      ctx.read_set_.pop_back();
      ctx.write_set_.push_back(bpm_->WritePage(chlid_page_id));
      page = ctx.write_set_.back().As<BPlusTreePage>();
    }
    ctx.read_set_.pop_front();
  }

  auto op_write_page = ctx.write_set_.back().AsMut<BPlusTreePage>();
  if (op_write_page->GetSize() < op_write_page->GetMaxSize()) {
    auto leaf_page = static_cast<LeafPage *>(op_write_page);
    int insert_index = IndexBinarySearchLeaf(leaf_page, key);

    // 若即将插入的key已存在，则返回flase
    if (comparator_(leaf_page->KeyAt(insert_index), key) == 0) {
      return false;
    }

    // 插入数据
    int size = leaf_page->GetSize();
    for (int i = size - 1; i >= insert_index; i--) {
      leaf_page->SetKeyAt(i + 1, leaf_page->KeyAt(i));
      leaf_page->SetValueAt(i + 1, leaf_page->ValueAt(i));
    }
    leaf_page->SetKeyAt(insert_index, key);
    leaf_page->SetValueAt(insert_index, value);
    leaf_page->ChangeSizeBy(1);
    return true;
  }
  // 若不符合条件，则将WritePageGuard释放
  ctx.write_set_.clear();

  // (2.2) 悲观锁 latch crabbing
  // (2.2.1) 找到要插入的叶子结点

  // 这里需要重新根据header获得一次root_page_id
  // 之前在test中出现过多线程问题，主要问题在于其他线程创建了新root结点，header被修改了root_page_id，但是这里用的root_page_id依旧是函数最开始时获取的
  auto head_guard = bpm_->WritePage(header_page_id_);
  ctx.header_page_ = std::make_optional(std::move(head_guard));
  ctx.root_page_id_ = ctx.header_page_->As<BPlusTreeHeaderPage>()->root_page_id_;

  ctx.write_set_.push_back(bpm_->WritePage(ctx.root_page_id_));
  auto write_page = ctx.write_set_.back().AsMut<BPlusTreePage>();
  // 特判根节点为叶子节点的情况，因为这种情况不会进入后续while循环
  // 若key的个数小于maxsize则后续一定不会改变header_page, 因此直接释放header_page_的锁
  if (write_page->GetSize() < write_page->GetMaxSize()) {
    ctx.header_page_ = std::nullopt;
  }

  while (!write_page->IsLeafPage()) {
    int index = KeyBinarySearch(write_page, key);
    if (index == -1) {
      return false;
    }
    auto internal_page = static_cast<InternalPage *>(write_page);
    auto child_page_id = internal_page->ValueAt(index);
    ctx.write_set_.push_back(bpm_->WritePage(child_page_id));
    // 个人觉得需要在context类中加入存放内部结点搜索位置index的数组
    ctx.indexes_.push_back(index);
    write_page = ctx.write_set_.back().AsMut<BPlusTreePage>();
    if (write_page->GetSize() < write_page->GetMaxSize()) {
      if (ctx.header_page_.has_value()) {
        ctx.header_page_ = std::nullopt;
      }
      // 从上到下释放掉当前节点的所有祖先节点
      while (ctx.write_set_.size() > 1) {
        ctx.write_set_.pop_front();
      }
    }
  }

  // (2.2.2)找到要插入key的位置
  auto leaf_page = static_cast<LeafPage *>(write_page);
  int insert_index = IndexBinarySearchLeaf(leaf_page, key);

  if (comparator_(leaf_page->KeyAt(insert_index), key) == 0) {
    return false;
  }
  // (2.2.3)若插入的叶子节点未满，则直接插入
  if (leaf_page->GetSize() < leaf_page->GetMaxSize()) {
    int size = leaf_page->GetSize();
    for (int i = size - 1; i >= insert_index; i--) {
      leaf_page->SetKeyAt(i + 1, leaf_page->KeyAt(i));
      leaf_page->SetValueAt(i + 1, leaf_page->ValueAt(i));
    }
    leaf_page->SetKeyAt(insert_index, key);
    leaf_page->SetValueAt(insert_index, value);
    leaf_page->ChangeSizeBy(1);
    ctx.write_set_.pop_front();
    return true;
  }

  // (2.2.4)若插入的叶子节点已满，则需要分裂
  // 这里设置让分裂后第一个结点的键数量为(maxsize + 1) / 2 的向上取整，也就是分裂后第一个叶子有时会比第二个多一个键值对
  int first_size = (leaf_page->GetMaxSize() + 2) / 2;
  int second_size = leaf_page->GetMaxSize() + 1 - first_size;
  page_id_t new_leaf_id = bpm_->NewPage();
  ctx.write_set_.push_back(bpm_->WritePage(new_leaf_id));
  auto new_leaf_page = ctx.write_set_.back().AsMut<LeafPage>();
  new_leaf_page->Init(leaf_max_size_);
  new_leaf_page->SetSize(second_size);
  leaf_page->SetSize(first_size);
  // 修改原叶子节点和新叶子结点的next_page_id_
  new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_leaf_id);

  // 分插入位置在老叶子结点还是新叶子结点来分别处理
  if (insert_index < first_size) {
    for (int i = 0; i < second_size; i++) {
      new_leaf_page->SetKeyAt(i, leaf_page->KeyAt(first_size - 1 + i));
      new_leaf_page->SetValueAt(i, leaf_page->ValueAt(first_size - 1 + i));
    }
    for (int i = first_size - 1; i > insert_index; i--) {
      leaf_page->SetKeyAt(i, leaf_page->KeyAt(i - 1));
      leaf_page->SetValueAt(i, leaf_page->ValueAt(i - 1));
    }
    leaf_page->SetKeyAt(insert_index, key);
    leaf_page->SetValueAt(insert_index, value);
  } else {
    for (int i = 0; i < insert_index - first_size; i++) {
      new_leaf_page->SetKeyAt(i, leaf_page->KeyAt(first_size + i));
      new_leaf_page->SetValueAt(i, leaf_page->ValueAt(first_size + i));
    }
    new_leaf_page->SetKeyAt(insert_index - first_size, key);
    new_leaf_page->SetValueAt(insert_index - first_size, value);
    for (int i = insert_index - first_size + 1; i < second_size; i++) {
      new_leaf_page->SetKeyAt(i, leaf_page->KeyAt(i + first_size - 1));
      new_leaf_page->SetValueAt(i, leaf_page->ValueAt(i + first_size - 1));
    }
  }

  // (2.2.5) 处理完叶子结点split操作后，继续更新内部结点
  // 获取要插入上一级内部结点的key，即新叶子结点的第一个key
  KeyType insert_key = new_leaf_page->KeyAt(0);
  // 两个叶子结点都处理完了，释放guard
  ctx.write_set_.pop_back();
  ctx.write_set_.pop_back();
  // 用于保存分裂后两个结点的page id
  page_id_t second_split_page_id = new_leaf_id;
  // 用于判断是否需要创建新的根结点
  bool new_root_flag = true;

  while (!ctx.write_set_.empty()) {
    // 得到上一级内部节点
    auto internal_page = ctx.write_set_.back().AsMut<InternalPage>();
    // 得到上一级内部节点应该插入的位置
    int insert_index = ctx.indexes_.back() + 1;
    int size = internal_page->GetSize();

    if (size < internal_page->GetMaxSize()) {
      for (int i = size; i > insert_index; i--) {
        internal_page->SetKeyAt(i, internal_page->KeyAt(i - 1));
        internal_page->SetValueAt(i, internal_page->ValueAt(i - 1));
      }
      internal_page->SetKeyAt(insert_index, insert_key);
      internal_page->SetValueAt(insert_index, second_split_page_id);
      internal_page->ChangeSizeBy(1);
      new_root_flag = false;
      ctx.write_set_.clear();
      ctx.indexes_.clear();
      break;
    }

    // 当内部结点已满时，继续进行分裂
    // 这里的size是指value的数量，不是key的数量
    // 同样老结点size_向上取整，新结点向下取整
    int first_size = (internal_page->GetMaxSize() + 2) / 2;
    int second_size = internal_page->GetMaxSize() + 1 - first_size;
    page_id_t new_internal_id = bpm_->NewPage();
    ctx.write_set_.push_back(bpm_->WritePage(new_internal_id));
    auto new_internal_page = ctx.write_set_.back().AsMut<InternalPage>();
    new_internal_page->Init(internal_max_size_);
    new_internal_page->SetSize(second_size);
    internal_page->SetSize(first_size);

    // 这里要注意，最中间的key是不需要保留在两个结点中的，会作为insert_key向一层传递，插入到上一层的internal page中
    // 最中间的key反映了右侧结点所在子树的最小值水平，所以向上传递，插入上层结点
    // 依然分插入位置在老叶子结点还是新叶子结点来分别处理
    if (insert_index < first_size) {
      // 比较巧妙的一点，在老节点中插入insert_index后，第first_size -
      // 1位置的key一定位于上取整的中间位置，且insert_index不会占据该位置
      KeyType tmp_key = internal_page->KeyAt(first_size - 1);
      for (int i = 0; i < second_size; i++) {
        // index为0位置key不存在，要注意单独处理
        if (i > 0) {
          new_internal_page->SetKeyAt(i, internal_page->KeyAt(first_size - 1 + i));
        }
        new_internal_page->SetValueAt(i, internal_page->ValueAt(first_size - 1 + i));
      }
      for (int i = first_size - 1; i > insert_index; i--) {
        internal_page->SetKeyAt(i, internal_page->KeyAt(i - 1));
        internal_page->SetValueAt(i, internal_page->ValueAt(i - 1));
      }
      internal_page->SetKeyAt(insert_index, insert_key);
      internal_page->SetValueAt(insert_index, second_split_page_id);
      // 更新insert_key
      insert_key = tmp_key;
    } else {
      for (int i = 0; i < insert_index - first_size; i++) {
        if (i > 0) {
          new_internal_page->SetKeyAt(i, internal_page->KeyAt(i + first_size));
        }
        new_internal_page->SetValueAt(i, internal_page->ValueAt(i + first_size));
      }
      KeyType tmp_key;
      // 在新节点插入insert_index后，first_size位置的key一定位于上取整的中间位置，但要注意此时insert_index可能会占据该位置
      if (insert_index > first_size) {
        new_internal_page->SetKeyAt(insert_index - first_size, insert_key);
        tmp_key = internal_page->KeyAt(first_size);
      } else {
        // 如果insert_index等于first_size
        tmp_key = insert_key;
      }
      new_internal_page->SetValueAt(insert_index - first_size, second_split_page_id);
      for (int i = insert_index - first_size + 1; i < second_size; i++) {
        new_internal_page->SetKeyAt(i, internal_page->KeyAt(first_size - 1 + i));
        new_internal_page->SetValueAt(i, internal_page->ValueAt(first_size - 1 + i));
      }
      // 更新insert_key
      insert_key = tmp_key;
    }
    // 更新要向上传递的新结点page id
    second_split_page_id = new_internal_id;
    // 释放新分裂结点的page guard
    ctx.write_set_.pop_back();

    // 最后要记得更新ctx
    ctx.write_set_.pop_back();
    ctx.indexes_.pop_back();
  }

  // 当需要创建新的root结点时，创建新结点并更新 header page
  if (new_root_flag) {
    page_id_t new_root_id = bpm_->NewPage();
    ctx.write_set_.push_back(bpm_->WritePage(new_root_id));
    auto new_root_page = ctx.write_set_.back().AsMut<InternalPage>();

    new_root_page->Init(internal_max_size_);
    // 这里size_应该设置为2，因为internal page 的size_指的是value的数量，是key的数量加一
    new_root_page->SetSize(2);
    new_root_page->SetKeyAt(1, insert_key);
    new_root_page->SetValueAt(0, ctx.root_page_id_);
    new_root_page->SetValueAt(1, second_split_page_id);

    auto head_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    head_page->root_page_id_ = new_root_id;
    ctx.write_set_.pop_back();
  }

  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * @brief Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 *
 * @param key input key
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Declaration of context instance.
  Context ctx;
  /*
    在执行乐观锁(读操作)时，header page的锁在读入根节点的锁后便可以释放掉
    所以我们不必把它放到ctx中，而是直接用一个局部变量guard来表示持有锁并获取root_page_id
    在 *确认读入* 根节点锁后，便直接drop释放header page的锁
  */
  auto guard = bpm_->WritePage(header_page_id_);
  ctx.root_page_id_ = guard.As<BPlusTreeHeaderPage>()->root_page_id_;

  // (1) 如果tree是空的
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    return;
  }

  // (2) 如果tree不是空的
  // (2.1) 乐观锁
  // (2.1.1) 找到要进行删除操作的叶子结点
  ctx.read_set_.emplace_back(bpm_->ReadPage(ctx.root_page_id_));
  auto page = ctx.read_set_.back().As<BPlusTreePage>();
  // 如果root结点为叶子结点，则将其升级为写锁。这里存在时间空窗，但有header结点锁未释放，提供了线程保护
  if (page->IsLeafPage()) {
    ctx.read_set_.pop_back();
    ctx.write_set_.emplace_back(bpm_->WritePage(ctx.root_page_id_));
    page = ctx.write_set_.back().As<BPlusTreePage>();
  }
  // 为避免空窗期，我们在这里才对guard执行drop操作
  guard.Drop();
  while (!page->IsLeafPage()) {
    int index = KeyBinarySearch(page, key);
    auto internal_page = static_cast<const InternalPage *>(page);
    auto chlid_page_id = internal_page->ValueAt(index);
    ctx.read_set_.push_back(bpm_->ReadPage(chlid_page_id));
    page = ctx.read_set_.back().As<BPlusTreePage>();
    // 如果当前结点为叶子结点，则在其父结点锁未被释放的情况下，进行读锁向写锁的升级
    // 父结点锁未被释放，保证读写锁升级过程的线程安全
    if (page->IsLeafPage()) {
      ctx.read_set_.pop_back();
      ctx.write_set_.push_back(bpm_->WritePage(chlid_page_id));
      page = ctx.write_set_.back().As<BPlusTreePage>();
    }
    ctx.read_set_.pop_front();
  }
  // 得到可修改的write page
  auto op_write_page = ctx.write_set_.back().AsMut<BPlusTreePage>();
  if (op_write_page->GetSize() > op_write_page->GetMinSize()) {
    int delete_index = KeyBinarySearch(op_write_page, key);
    if (delete_index == -1) {
      return;
    }
    auto leaf_page = static_cast<LeafPage *>(op_write_page);
    if (comparator_(leaf_page->KeyAt(delete_index), key) != 0) {
      return;
    }

    // 删除数据
    int size = leaf_page->GetSize();
    for (int i = delete_index; i < size - 1; i++) {
      leaf_page->SetKeyAt(i, leaf_page->KeyAt(i + 1));
      leaf_page->SetValueAt(i, leaf_page->ValueAt(i + 1));
    }
    leaf_page->ChangeSizeBy(-1);
    return;
  }

  ctx.write_set_.clear();
  // (2.2) 悲观锁 latch crabbing
  // (2.2.1) 找到要删除的叶子结点
  // 同样的，这里需要重新根据header获得一次root_page_id
  /*
    在执行悲观锁的情况下，header page的锁是持续持有的
    尽管和读操作时的拥有head_guard变量表示持有锁，并在drop时表示释放锁也是可行的
    但更推荐放在 ctx，因为这样更安全（异常安全）、更直观（表达事务范围）、也更符合项目里的整体风格。
    如果函数中途抛异常或者 return 提前退出，挂在 ctx 的 guard 会自动析构释放；但手动 drop() 可能根本走不到 → 会死锁。
  */
  auto head_guard = bpm_->WritePage(header_page_id_);
  ctx.header_page_ = std::make_optional(std::move(head_guard));
  ctx.root_page_id_ = ctx.header_page_->As<BPlusTreeHeaderPage>()->root_page_id_;

  ctx.write_set_.push_back(bpm_->WritePage(ctx.root_page_id_));
  auto write_page = ctx.write_set_.back().AsMut<BPlusTreePage>();
  // 特判根节点为叶子节点的情况，因为这种情况不会进入后续while循环
  // 若key的个数大于2则后续一定不会改变header_page, 因此直接释放header_page_的锁
  if (write_page->GetSize() > 2) {
    ctx.header_page_ = std::nullopt;
  }
  while (!write_page->IsLeafPage()) {
    int index = KeyBinarySearch(write_page, key);
    if (index == -1) {
      return;
    }
    auto internal_page = static_cast<InternalPage *>(write_page);
    auto child_page_id = internal_page->ValueAt(index);
    ctx.write_set_.push_back(bpm_->WritePage(child_page_id));
    // 个人觉得需要在context类中加入存放内部结点搜索位置index的数组
    ctx.indexes_.push_back(index);
    write_page = ctx.write_set_.back().AsMut<BPlusTreePage>();
    if (write_page->GetSize() > write_page->GetMinSize()) {
      if (ctx.header_page_.has_value()) {
        ctx.header_page_ = std::nullopt;
      }
      while (ctx.write_set_.size() > 1) {
        ctx.write_set_.pop_front();
      }
    }
  }

  // (2.2.2) 删除叶子结点中的key
  int delete_index = KeyBinarySearch(write_page, key);
  if (delete_index == -1) {
    return;
  }
  int size = write_page->GetSize();
  auto leaf_page = static_cast<LeafPage *>(write_page);
  for (int i = delete_index; i < size - 1; i++) {
    leaf_page->SetKeyAt(i, leaf_page->KeyAt(i + 1));
    leaf_page->SetValueAt(i, leaf_page->ValueAt(i + 1));
  }
  leaf_page->ChangeSizeBy(-1);

  // 当前操作结点的page id，在原root变为空要被删除时，也是新root结点的page id
  page_id_t new_root_page_id = INVALID_PAGE_ID;

  // 与insert操作不同的是，我们这里并不释放叶子节点的guard，因为delete需要同时用到当前节点和父节点

  while (!ctx.write_set_.empty()) {
    // 特判根节点，考虑是否可以写在外面？
    if (ctx.write_set_.size() == 1) {
      auto root_page = ctx.write_set_.back().AsMut<BPlusTreePage>();
      // 如果root结点为叶子结点，则单独处理。如果root不为空，则直接return，如果为空，则重新设置root page id
      if (root_page->IsLeafPage()) {
        if (root_page->GetSize() == 0) {
          // 是否需要bpm_->DeletePage当前root结点 ？
          bpm_->DeletePage(ctx.root_page_id_);
          // ctx.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = INVALID_PAGE_ID;
          auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
          header_page->root_page_id_ = INVALID_PAGE_ID;
        }
        return;
      }
      // 如果此时root结点为内部结点且为空，则将原root结点删除，且修改root结点的page id
      // size为1时，没有key存在，只有一个value，此时root为不合法状态，同样需要删除
      if (root_page->GetSize() <= 1) {
        ctx.write_set_.pop_back();
        bpm_->DeletePage(ctx.root_page_id_);
        auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
        header_page->root_page_id_ = new_root_page_id;
      }

      // 若root结点不为空，则不用管minSize的约束，直接return
      return;
    }

    // 若删除后的结点大于等于半满，则完成删除操作。这里相当于递归的出口
    if (write_page->GetSize() >= write_page->GetMinSize()) {
      return;
    }

    // 使用反向迭代器获取deque倒数第二个元素，即当前处理元素的父结点
    auto it = ctx.write_set_.rbegin();
    ++it;
    auto parent_page = it->AsMut<InternalPage>();
    int index = ctx.indexes_.back();

    // 尝试从左兄弟结点借
    if (index > 0) {
      ctx.write_set_.push_back(bpm_->WritePage(parent_page->ValueAt(index - 1)));
      auto left_page = ctx.write_set_.back().AsMut<BPlusTreePage>();
      if (left_page->GetSize() > left_page->GetMinSize()) {
        // 最开始定义的write_page即为当前节点，不必再单独开一个变量
        BorrowFromLeft(write_page, left_page, parent_page, index);
        return;
      }
      ctx.write_set_.pop_back();
    }
    if (index < parent_page->GetSize() - 1) {
      ctx.write_set_.push_back(bpm_->WritePage(parent_page->ValueAt(index + 1)));
      auto right_page = ctx.write_set_.back().AsMut<BPlusTreePage>();
      if (right_page->GetSize() > right_page->GetMinSize()) {
        BorrowFromRight(write_page, right_page, parent_page, index);
        return;
      }
      ctx.write_set_.pop_back();
    }
    // 不可借用，尝试左合并
    if (index > 0) {
      ctx.write_set_.push_back(bpm_->WritePage(parent_page->ValueAt(index - 1)));
      auto left_page = ctx.write_set_.back().AsMut<BPlusTreePage>();
      MergeWithLeft(write_page, left_page, parent_page, index);
      // 合并到左边，可能的新根节点page id为左兄弟节点的page id
      new_root_page_id = ctx.write_set_.back().GetPageId();
      ctx.write_set_.pop_back();
      // 将被合并的页面delete
      page_id_t page_id = ctx.write_set_.back().GetPageId();
      ctx.write_set_.pop_back();
      bpm_->DeletePage(page_id);
    } else {
      // 不可左合并，尝试右合并
      ctx.write_set_.push_back(bpm_->WritePage(parent_page->ValueAt(index + 1)));
      auto right_page = ctx.write_set_.back().AsMut<BPlusTreePage>();
      MergeWithRight(write_page, right_page, parent_page, index);
      // 顺序反过来
      page_id_t page_id = ctx.write_set_.back().GetPageId();
      ctx.write_set_.pop_back();
      bpm_->DeletePage(page_id);
      // 合并到当前节点，可能的新根节点page id为当前节点的page id
      new_root_page_id = ctx.write_set_.back().GetPageId();
      ctx.write_set_.pop_back();
      // 将被合并的页面delete
    }

    ctx.indexes_.pop_back();
    write_page = ctx.write_set_.back().AsMut<BPlusTreePage>();
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/**
 * @brief Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 *
 * You may want to implement this while implementing Task #3.
 *
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  Context ctx;
  auto guard = bpm_->ReadPage(header_page_id_);
  auto head_page = guard.As<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = head_page->root_page_id_;
  // 在读入根节点后释放header的锁
  guard.Drop();

  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, -1);
  }

  ReadPageGuard page_guard = bpm_->ReadPage(ctx.root_page_id_);
  auto page = page_guard.As<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    auto internal_page = static_cast<const InternalPage *>(page);
    page_guard = bpm_->ReadPage(internal_page->ValueAt(0));
    page = page_guard.As<BPlusTreePage>();
  }
  return INDEXITERATOR_TYPE(bpm_, page_guard.GetPageId(), 0);
}

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  Context ctx;
  auto guard = bpm_->ReadPage(header_page_id_);
  auto head_page = guard.As<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = head_page->root_page_id_;
  // 在读入根节点后释放header的锁
  guard.Drop();

  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, -1);
  }

  ReadPageGuard page_guard = bpm_->ReadPage(ctx.root_page_id_);
  auto page = page_guard.As<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    int index = KeyBinarySearch(page, key);
    auto internal_page = static_cast<const InternalPage *>(page);
    page_guard = bpm_->ReadPage(internal_page->ValueAt(index));
    page = page_guard.As<BPlusTreePage>();
  }
  auto leaf_page = static_cast<const LeafPage *>(page);
  int index = KeyBinarySearch(leaf_page, key);
  if (index == -1) {
    return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, -1);
  }
  return INDEXITERATOR_TYPE(bpm_, page_guard.GetPageId(), index);
}

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, -1); }

/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  auto guard = bpm_->ReadPage(header_page_id_);
  auto head_page = guard.As<BPlusTreeHeaderPage>();
  return head_page->root_page_id_;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
