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
    std::ifstream inputFile(name, std::ios::binary | std::ios::ate);

    if (!inputFile) {
      throw std::runtime_error("HeapFile::HeapFile failed to open file");
    }


    std::streamsize file_size = inputFile.tellg();

    if (file_size == 0) {
      // file is empty
      this->numPages = 0;
    }
    else {

      // If the file is not empty, calculate the number of pages
      this->numPages = file_size / DEFAULT_PAGE_SIZE;
    }
    inputFile.close();
  }
  else {
    // create a new file
    std::ofstream outputFile(name, std::ios::binary);

    if (!outputFile) {
      throw std::runtime_error("HeapFile::HeapFile failed to create file");
    }

    outputFile.close();
    this->numPages = 0;
  }

}

void HeapFile::insertTuple(const Tuple &t) {
  // TODO pa2: implement

  if (numPages == 0) {

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
    HeapPage hp(*lp, this->td);

    if (hp.insertTuple(t)) {
      this->writePage(*lp, this->numPages - 1);
    }
    else {
      // If page is full, create new page

      this->numPages++;

      std::unique_ptr<Page> np = std::make_unique<Page>();
      HeapPage nhp(*np, this->td);

      if (!nhp.insertTuple(t)) {
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
  HeapPage hp(*page, this->td);

  // Delete the tuple from the page
  hp.deleteTuple(it.slot);
  this->writePage(*page, it.page);



}

Tuple HeapFile::getTuple(const Iterator &it) const {
  // TODO pa2: implement

  std::unique_ptr<Page> page = std::make_unique<Page>();

  this->readPage(*page, it.page);

  HeapPage hp(*page, this->td);

  return hp.getTuple(it.slot);
}

void HeapFile::next(Iterator &it) const {
  // TODO pa2: implement

  // Read the current page
  std::unique_ptr<Page> page = std::make_unique<Page>();
  this->readPage(*page, it.page);
  HeapPage hp(*page, this->td);

  // point heap page to next slot
  hp.next(it.slot);

  //iterate through slots
  while (it.page < this->numPages && it.slot == hp.end()) {

    it.page++;

    if (it.page == this->numPages) {
      it.slot = 0;
      return;
    }

    std::unique_ptr<Page> np = std::make_unique<Page>();
    this->readPage(*np, it.page);
    HeapPage nhp(*np, this->td);
    it.slot = nhp.begin();
  }
}

Iterator HeapFile::begin() const {
  // TODO pa2: implement

  // get first non-empty page

  for (size_t page = 0; page < this->numPages; page++) {

    std::unique_ptr<Page> p = std::make_unique<Page>();
    this->readPage(*p, page);
    HeapPage hp(*p, this->td);

    if (hp.begin() != hp.end()){
      return {*this, page, hp.begin()};
    }
  }
  // return iterator
  Iterator it = {*this, numPages, 0};
  return it;
}

Iterator HeapFile::end() const {
  // TODO pa2: implement
  // Return the iterator
  Iterator it = {*this, numPages, 0};
  return it;
}