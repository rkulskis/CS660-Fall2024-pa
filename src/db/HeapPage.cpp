#include <db/Database.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>

using namespace db;

bool get_bit(const uint8_t B, const uint8_t b) {
	return B & (1 << b);					// shift right and mask
}

void set_bit(uint8_t *B, const uint8_t b, bool val) {
	if (val)
		*B |= (1 << b);
	else
		*B &= ~(1 << b);
}

// HeapPage overview:
// [[H=bit array of populated flags for tuples], [padding], [T=tuple array]]
HeapPage::HeapPage(Page &page, const TupleDesc &td) : td(td) {
  size_t
		P = page.size(),
		T = td.length();

  this->capacity = (P*8)/(T*8 + 1); // +1 bit per tuple populated flag in header
  this->header = page.data();
  this->data = header + (P - capacity*T); // after header
}

size_t HeapPage::begin() const { // return index of first populated tuple
	size_t slot;

  for (slot=0; slot < capacity; ++slot) {
		if (this->empty(slot)) continue;
		
		return slot;
  }
	
  return capacity;							// end of page
}

size_t HeapPage::end() const {
  return capacity;
}

bool HeapPage::insertTuple(const Tuple &t) {
	size_t slot;
	
  for (slot=0; slot < capacity; ++slot) {
		if (!this->empty(slot)) continue;
		
		td.serialize(&data[slot*td.length()], t);
		set_bit(&header[slot/8], 7-slot%8, true); // little endian use 7-
		
		return true;
  }
	
  return false;  								// page full
}

void HeapPage::deleteTuple(size_t slot) {
  if (this->empty(slot))
    throw std::runtime_error("Slot is not empty.");

	set_bit(&header[slot/8], 7-slot%8, false);	
}

Tuple HeapPage::getTuple(size_t slot) const {
  if (this->empty(slot))
    throw std::runtime_error("Empty slot.");

  return td.deserialize(&data[slot*td.length()]);
}

void HeapPage::next(size_t &slot) const {
  while (++slot < capacity) {
    if (!this->empty(slot))
      return;
  }

  slot = capacity;
}

bool HeapPage::empty(size_t slot) const {
	return !get_bit(header[slot/8], 7-slot%8);
}
