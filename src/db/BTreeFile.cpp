#include <cstring>
#include <db/BTreeFile.hpp>
#include <db/Database.hpp>
#include <db/IndexPage.hpp>
#include <db/LeafPage.hpp>
#include <stdexcept>

using namespace db;

BTreeFile::BTreeFile(const std::string &name, const TupleDesc &td, size_t key_index)
    : DbFile(name, td), key_index(key_index) {}

void BTreeFile::insertTuple(const Tuple &t) {
  // TODO pa2: implement

  if (!td.compatible(t)) {
    throw std::invalid_argument("Tuple is not compatible with Tuple Desc");
  }

  BufferPool &buffer_pool = getDatabase().getBufferPool();

  //get first page that is the root
  PageId pid{name, 0};
  Page &rootPage = buffer_pool.getPage(pid);

  //if root is empty, insert leaf page
  if (rootPage.empty()) {
    LeafPage leaf(rootPage, td, key_index);

    if (leaf.insertTuple(t)) {
      buffer_pool.markDirty(pid);
    }
  } else {
    //get the root indexpage
    IndexPage root(rootPage);

    // Check if the root page can accommodate the new tuple.
    if (insertTupleRecursive(root, t)) {
      PageId newPid{name, numPages++};
      Page &newPage = buffer_pool.getPage(newPid);
      IndexPage newRoot(newPage);

      int splitKey = root.split(newRoot);

      // Update new root with the original root and the new split page.
      newRoot.insert(splitKey, pid.page);
      buffer_pool.markDirty(newPid);
    }

    buffer_pool.markDirty(pid);
  }
}

bool BTreeFile::insertTupleRecursive(IndexPage &node, const Tuple &t) {
  BufferPool &buffer_pool = getDatabase().getBufferPool();

  if (node.header->index_children) {
    // if the next page is an indexpage
    size_t child_index = 0;

    while (child_index < node.header->size && t.get_field(key_index).index() >= node.keys[child_index]) {
      child_index++;
    }

    PageId newPid{name, node.children[child_index]};
    Page &newPage = buffer_pool.getPage(newPid);
    IndexPage newIndexPage(newPage);

    // recursively call function
    if (insertTupleRecursive(newIndexPage, t)) {
      // If the insertion into the child node resulted in a split
      if (node.insert(node.keys[child_index], newPid.page)) {
        // index page is full, need to split it
        buffer_pool.markDirty(newPid);
        return true;
      }
      // index page is not full
      return false;
    }
  } else {
    // child is a leaf node
    size_t child_index = 0;

    while (child_index < node.header->size && t.get_field(key_index).index() >= node.keys[child_index]) {
      child_index++;
    }

    PageId pid{name, node.children[child_index]};
    Page &page = buffer_pool.getPage(pid);
    LeafPage leaf(page, td, key_index);

    if (leaf.insertTuple(t)) {
      // if leaf is full we need to split it
      PageId newLeafPid{name, numPages++};
      Page &newPage = buffer_pool.getPage(newLeafPid);
      LeafPage newLeafPage(newPage, td, key_index);

      int splitKey = leaf.split(newLeafPage);
      node.insert(splitKey, newLeafPid.page);

      buffer_pool.markDirty(newLeafPid);
      buffer_pool.markDirty(*reinterpret_cast<PageId *>(&node));
      return true;
    }
    //leafpage is not full
    return false;
  }
}


void BTreeFile::deleteTuple(const Iterator &it) {
  // Do not implement
}

Tuple BTreeFile::getTuple(const Iterator &it) const {
  // TODO pa2: implement

  BufferPool &buffer_pool = getDatabase().getBufferPool();
  PageId pid{name, it.page};

  Page &page = buffer_pool.getPage(pid);

  LeafPage leafPage(page, td, key_index);

  return leafPage.getTuple(it.slot);
}

void BTreeFile::next(Iterator &it) const {
  // TODO pa2: implement

  BufferPool &buffer_pool = getDatabase().getBufferPool();

  PageId pid{name, it.page};
  Page &page = buffer_pool.getPage(pid);

  LeafPage leafPage(page, td, key_index);

  size_t next_slot = it.slot+1;

  if(next_slot >= leafPage.header->size) {
    //leaf is full, go to next leaf

    it.page = leafPage.header->next_leaf;
    it.slot = 0;
  }
  else {

    it.slot = next_slot;
  }


}

Iterator BTreeFile::begin() const {
  // TODO pa2: implement

  BufferPool &buffer_pool = getDatabase().getBufferPool();
  PageId pid{name, root_id};
  Page &rootPage = buffer_pool.getPage(pid);

  //iterate from root to leftmost child
  IndexPage root(rootPage);

  size_t page = root_id;

  while(root.header->index_children) {
    //traverse left most child till you encounter leaf

    auto leftchild = root.children[0];

    PageId leftChildPid{name, leftchild};

    Page &leftChildPage = buffer_pool.getPage(leftChildPid);

    //update root
    root = IndexPage(leftChildPage);
    page = leftChildPid.page;
  }

  //return the iterator to the first tuple of the leftmost leaf (head).
  return Iterator(*this, page, 0);

}

Iterator BTreeFile::end() const {
  // TODO pa2: implement

  BufferPool &buffer_pool = getDatabase().getBufferPool();
  PageId pid{name, root_id};
  Page &rootPage = buffer_pool.getPage(pid);

  //iterate to rightmost child
  IndexPage root(rootPage);
  size_t page = root_id;

  while(root.header->index_children) {

    //travesre rightmost child till you reach leaf

    auto rightchild = root.children[root.header->size];
    PageId rightChildPid{name, rightchild};
    Page &rightChildPage = buffer_pool.getPage(rightChildPid);

    //update root to rightmost child
    root = IndexPage(rightChildPage);
    page = rightChildPid.page;

  }

  auto rightchild = root.children[root.header->size];
  PageId rightChildPid{name, rightchild};
  Page &rightChildPage = buffer_pool.getPage(rightChildPid);

  LeafPage leaf(rightChildPage, td, key_index);

  return Iterator(*this, page, leaf.header->size);


}