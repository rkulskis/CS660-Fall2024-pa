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
    // If it exists, open the file and check its size
    std::ifstream infile(name, std::ios::binary | std::ios::ate);
    if (!infile) {
      throw std::runtime_error("Failed to open existing file for reading");
    }
    std::streamsize fileSize = infile.tellg();

    if (fileSize == 0) {
      // If the file is empty, we treat it as a new file
      this->numPages = 0;
    } else {
      // If the file is not empty, calculate the number of pages
      this->numPages = fileSize / DEFAULT_PAGE_SIZE;
    }
    infile.close();
  } else {
    // If the file doesn't exist, create a new one
    std::ofstream outfile(name, std::ios::binary);
    if (!outfile) {
      throw std::runtime_error("Failed to create a new file");
    }
    outfile.close();
    this->numPages = 0;  // Start with 0 pages for a new file
  }

}

void HeapFile::insertTuple(const Tuple &t) {
  // TODO pa2: implement

  if(this->numPages ==0) {
    //If there are no current pages, create a new one
    this->numPages++;

    std::unique_ptr<Page> np = std::make_unique<Page>();
    HeapPage hp(*np,td);

    hp.insertTuple(t);
    this->writePage(*np,0);
  }
  else {

    std::unique_ptr<Page> np = std::make_unique<Page>();
    this->readPage(*np,numPages-1);
    HeapPage hp(*np,td);

    if(hp.insertTuple(t)) {

      this->writePage(*np,numPages-1);
    }
    else {

      this->numPages++;
      std::unique_ptr<Page> np = std::make_unique<Page>();
      HeapPage hp(*np,td);

      if(!hp.insertTuple(t)) {
        throw std::runtime_error("HeapFile::insertTuple: could not insert tuple");
      }

      this->writePage(*np,numPages-1);
    }

  }
}

void HeapFile::deleteTuple(const Iterator &it) {
  // TODO pa2: implement

  auto file = it.file;
  auto page = it.page;
  auto slot = it.slot;




}

Tuple HeapFile::getTuple(const Iterator &it) const {
  // TODO pa2: implement
}

void HeapFile::next(Iterator &it) const {
  // TODO pa2: implement
}

Iterator HeapFile::begin() const {
  // TODO pa2: implement
}

Iterator HeapFile::end() const {
  // TODO pa2: implement
}
