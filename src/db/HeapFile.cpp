#include <db/Database.hpp>
#include <db/HeapFile.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>
#include <filesystem>
#include <fstream>
#include <iostream>

using namespace db;

HeapFile::HeapFile(const std::string &name, const TupleDesc &td) : DbFile(name, td) {

  // Check if the file exists
  if (std::filesystem::exists(name)) {
    // file exists
    std::ifstream infile(name, std::ios::binary | std::ios::ate);

    if (!infile) {
      throw std::runtime_error("HeapFile::HeapFile failed to open file");
    }
    std::streamsize fileSize = infile.tellg();

    if (fileSize == 0) {
      // file is empty
      this->numPages = 0;
    } else {

      // If the file is not empty, calculate the number of pages
      this->numPages = fileSize / DEFAULT_PAGE_SIZE;
    }
    infile.close();
  } else {
    // create a new file
    std::ofstream outfile(name, std::ios::binary);
    if (!outfile) {
      throw std::runtime_error("HeapFile::HeapFile failed to create file");
    }
    outfile.close();
    this->numPages = 0;
  }

}

void HeapFile::insertTuple(const Tuple &t) {
  // TODO pa2: implement

  if (this->numPages == 0) {
    // increment pages if no page exists
    this->numPages++;

    //new page
    std::unique_ptr<Page> np = std::make_unique<Page>();
    // Heap wrapper
    HeapPage hp(*np, this->td);

    // Insert the tuple into the page
    hp.insertTuple(t);
    this->writePage(*np, 0); // Write the page to disk at page 0
  }

  else {
    //insert tuple into last page

    std::unique_ptr<Page> lp = std::make_unique<Page>();
    this->readPage(*lp, this->numPages - 1);
    HeapPage heapPage(*lp, this->td);

    if (heapPage.insertTuple(t)) {
      this->writePage(*lp, this->numPages - 1);
    }
    else {
      // If page is full, create new page

      this->numPages++;

      std::unique_ptr<Page> np = std::make_unique<Page>();
      HeapPage newHeapPage(*np, this->td);

      if (!newHeapPage.insertTuple(t)) {
        throw std::runtime_error("HeapFile::insertTuple failed");
      }

      this->writePage(*np, this->numPages - 1);
    }
  }
}

void HeapFile::deleteTuple(const Iterator &it) {
  // TODO pa2: implement

  // Read the page from iterator
  std::unique_ptr<Page> page = std::make_unique<Page>();

  this->readPage(*page, it.page);
  HeapPage heapPage(*page, this->td);

  // Delete the tuple from the page
  heapPage.deleteTuple(it.slot);
  this->writePage(*page, it.page);



}

Tuple HeapFile::getTuple(const Iterator &it) const {
  // TODO pa2: implement

  std::unique_ptr<Page> page = std::make_unique<Page>();
  this->readPage(*page, it.page);
  HeapPage heapPage(*page, this->td);
  return heapPage.getTuple(it.slot);
}

void HeapFile::next(Iterator &it) const {
  // TODO pa2: implement

  // Read the current page
  std::unique_ptr<Page> page = std::make_unique<Page>();
  this->readPage(*page, it.page);
  HeapPage heapPage(*page, this->td);

  // point heap page to next slot
  heapPage.next(it.slot);

  //iterate through slots
  while (it.slot == heapPage.end() && it.page < this->numPages) {
    it.page++;
    if (it.page == this->numPages) { // No more pages left
      it.slot = 0; // End of the file
      return;
    }

    std::unique_ptr<Page> np = std::make_unique<Page>();
    this->readPage(*np, it.page);
    HeapPage nextHeapPage(*np, this->td);
    it.slot = nextHeapPage.begin();
  }
}

Iterator HeapFile::begin() const {
  // TODO pa2: implement

  // get first non-empty page
  for (size_t page = 0; page < this->numPages; page++) {

    std::unique_ptr<Page> p = std::make_unique<Page>();
    this->readPage(*p, page);
    HeapPage heapPage(*p, this->td);

    if (heapPage.begin() != heapPage.end()){
      return {*this, page, heapPage.begin()};
    }
  }
  // return iterator
  return {*this, numPages, 0};
}

Iterator HeapFile::end() const {
  // TODO pa2: implement
  // Return the end iterator
  return {*this, numPages, 0};
}

