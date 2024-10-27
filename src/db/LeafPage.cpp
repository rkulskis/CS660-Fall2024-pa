#include <db/LeafPage.hpp>
#include <algorithm>
#include <cstring> 
#include <iterator>
#include <stdexcept>
#include <iostream>

namespace db {
	LeafPage::LeafPage(Page &page, const TupleDesc &td, size_t key_index)
    : td(td), key_index(key_index) {
    header = reinterpret_cast<LeafPageHeader *>(page.data());
		header->size = 0;
		header->next_leaf = 0;

    data = page.data() + sizeof(LeafPageHeader); // after header
    capacity = (DEFAULT_PAGE_SIZE - sizeof(LeafPageHeader)) / td.length();
	}

	bool LeafPage::insertTuple(const Tuple &t) {
		int k1, k2;
		size_t pos, i;
		uint8_t *slot_data;

    k1 = std::get<int>(t.get_field(key_index));
    pos = findInsertPosition(k1);
		
		slot_data = data + pos * td.length();

		if (pos == header->size)
			goto serialize;
		
		k2 = std::get<int>(getTuple(pos).get_field(key_index));
		
    if (k1 == k2) { // overwrite tuple
			td.serialize(slot_data, t);
			goto done;
    }

		std::memcpy(slot_data + td.length(), slot_data, // shift tuples
								td.length() * (header->size - pos));

	serialize:
    td.serialize(slot_data, t);
    header->size++;

	done:
    return (header->size == capacity);
	}

	size_t LeafPage::findInsertPosition(int key) const {
    size_t l = 0, r = header->size, mid;
		int mid_key;

		while (l < r) {
			mid = l + (r-l)/2;
			mid_key = std::get<int>(getTuple(mid).get_field(key_index));

			if (key > mid_key)
				l = mid + 1;  // search in right half
			else
				r = mid;  // search in left half (including mid)
    }

    return l;
	}
	// helper: Copy a tuple from one slot to another
	void LeafPage::copyTuple(size_t from, size_t to) {
    uint8_t
			*fromData = data + from * td.length(),
			*toData = data + to * td.length();
		
    std::memcpy(toData, fromData, td.length());
	}

	int LeafPage::split(LeafPage &new_page) {
    size_t midpt = header->size / 2, i;

    for (i=midpt; i < header->size; ++i)
			new_page.insertTuple(getTuple(i));

		new_page.header->size = header->size - midpt;
    header->size = midpt;
		
		new_page.header->next_leaf = header->next_leaf; // logic not right here

		// return 1st key of new page
    return std::get<int>(new_page.getTuple(0).get_field(key_index));
	}

	Tuple LeafPage::getTuple(size_t slot) const {
    if (slot >= header->size) {
			std::cout << "Slot " << slot << " is out of bounds!";
			throw std::runtime_error("Slot out of bounds");
		}

    uint8_t *slot_data = data + slot * td.length();
    return td.deserialize(slot_data);
	}
}
