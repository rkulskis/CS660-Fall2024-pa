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
  if(rootPage.empty()) {

    LeafPage leaf(rootPage,td,key_index);

    if(leaf.insertTuple(t)) {

      buffer_pool.markDirty(pid);
    }


  }
  else {

    //get the root indexpage

    IndexPage root(rootPage);

    if(insertTupleRecursive(root, t)) {

      PageId newPid{name, numPages++};
      Page &newPage = buffer_pool.getPage(newPid);
      IndexPage newRoot(newPage);

      int splitKey = root.split(newRoot);

      newRoot.insert(splitKey, root_id);
      buffer_pool.markDirty(newPid);

    }

    buffer_pool.markDirty(pid);
  }


}

bool BTreeFile::insertTupleRecursive(IndexPage &node, const Tuple &t) {

  BufferPool &buffer_pool = getDatabase().getBufferPool();

  if(node.header->index_children) {
    //if the next page is an indexpage

    size_t child_index = 0;

    while( child_index < node.header->size && t.get_field(key_index).index() >= node.keys[child_index]) {
      child_index++;
    }

    PageId newPid{name, node.children[child_index]};
    Page &newPage = buffer_pool.getPage(newPid);
    IndexPage newIndexPage(newPage);

    //recursively call function
    if(insertTupleRecursive(newIndexPage, t)) {

      if(node.insert(node.keys[child_index], node.children[child_index])) {
        //index page is full, need to split it
        buffer_pool.markDirty(newPid);
        return true;
      }

      //index page is not full
      return false;

    }
  }
  else {
    //child is a leaf node

    size_t child_index = 0;

    while( child_index < node.header->size && t.get_field(key_index).index() >= node.keys[child_index]) {
      child_index++;
    }

    PageId pid{name, node.children[child_index]};
    Page &page = buffer_pool.getPage(pid);
    LeafPage leaf(page,td, key_index);

    if(leaf.insertTuple(t)) {
        //if leaf is full we need to split it

      PageId newLeafPid{name, numPages++};
      Page &newPage = buffer_pool.getPage(newLeafPid);
      LeafPage newLeafPage(newPage, td, key_index);

      int splitKey = leaf.split(newLeafPage);
      node.insert(splitKey, newLeafPid.page);

      buffer_pool.markDirty(newLeafPid);
      buffer_pool.markDirty(*reinterpret_cast<PageId *>(&node));
      return true;


    }

    buffer_pool.markDirty(*reinterpret_cast<PageId *>(&node));
    return false;
  }
}

void BTreeFile::deleteTuple(const Iterator &it) {
  // Do not implement
}

Tuple BTreeFile::getTuple(const Iterator &it) const {
  // TODO pa2: implement
}

void BTreeFile::next(Iterator &it) const {
  // TODO pa2: implement
}

Iterator BTreeFile::begin() const {
  // TODO pa2: implement
}

Iterator BTreeFile::end() const {
  // TODO pa2: implement
}