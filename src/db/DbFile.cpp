#include <db/DbFile.hpp>
#include <stdexcept>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace db;

const TupleDesc &DbFile::getTupleDesc() const { return td; }

DbFile::DbFile(const std::string &name, const TupleDesc &td)
  : name(name), td(td), fd(-1), numPages(0) {
  fd = open(name.c_str(), O_RDWR);

  if (fd < 0)
    throw std::runtime_error("Failed to open file " + name);

  struct stat f_info;
  
  if (fstat(fd, &f_info) < 0) {
    close(fd);
    throw std::runtime_error("Failed to get file status " + name);
  }

  numPages = (f_info.st_size + DEFAULT_PAGE_SIZE-1) / DEFAULT_PAGE_SIZE; // ceil
}

DbFile::~DbFile() {
  if (fd >= 0)
		close(fd);
}

const std::string &DbFile::getName() const { return name; }

void DbFile::readPage(Page &page, const size_t id) const {
  reads.push_back(id);  // track read page IDs
  
  size_t offset = id * DEFAULT_PAGE_SIZE;  // calculate offset based on page id
  ssize_t bytes_read = pread(fd, page.data(), DEFAULT_PAGE_SIZE, offset);
  
  if (bytes_read < 0)
    throw std::runtime_error("Failed to read page " + std::to_string(id));
  
  if (bytes_read != DEFAULT_PAGE_SIZE)
    throw std::runtime_error("Incomplete read for page " + std::to_string(id));
}

void DbFile::writePage(const Page &page, const size_t id) const {
  writes.push_back(id);  // track write page IDs
  
  size_t offset = id * DEFAULT_PAGE_SIZE;
  ssize_t bytes_written = pwrite(fd, page.data(), DEFAULT_PAGE_SIZE, offset);
  
  if (bytes_written < 0)
    throw std::runtime_error("Failed to write page " + std::to_string(id));
  
  if (bytes_written != DEFAULT_PAGE_SIZE)
    throw std::runtime_error("Incomplete write for page " + std::to_string(id));
}

const std::vector<size_t> &DbFile::getReads() const { return reads; }

const std::vector<size_t> &DbFile::getWrites() const { return writes; }

void DbFile::insertTuple(const Tuple &t) { throw std::runtime_error("Not implemented"); }

void DbFile::deleteTuple(const Iterator &it) { throw std::runtime_error("Not implemented"); }

Tuple DbFile::getTuple(const Iterator &it) const { throw std::runtime_error("Not implemented"); }

void DbFile::next(Iterator &it) const { throw std::runtime_error("Not implemented"); }

Iterator DbFile::begin() const { throw std::runtime_error("Not implemented"); }

Iterator DbFile::end() const { throw std::runtime_error("Not implemented"); }

size_t DbFile::getNumPages() const { return numPages; }
