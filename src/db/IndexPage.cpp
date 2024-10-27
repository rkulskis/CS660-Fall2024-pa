#include <db/IndexPage.hpp>
#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <iostream>

using namespace db;

IndexPage::IndexPage(Page &page) {
	header = reinterpret_cast<IndexPageHeader *>(page.data());
	header->size = 0;
	header->index_children = false;
		
	capacity = (DEFAULT_PAGE_SIZE - sizeof(IndexPageHeader))
		/ (sizeof(int) + sizeof(size_t)) - 1; // -1 since |children|=|keys|+1

	keys = reinterpret_cast<int *>(page.data() + sizeof(IndexPageHeader));
	children = reinterpret_cast<size_t *>(keys + capacity);
}

size_t IndexPage::findInsertPosition(int key) const {
	size_t l = 0, r = header->size, mid;
	int mid_key;

	while (l < r) {
		mid = l + (r-l)/2;
		mid_key = keys[mid];

		if (key > mid_key)
			l = mid + 1;  // search in right half
		else
			r = mid;  // search in left half (including mid)
	}

	return l;
}

bool IndexPage::insert(int key, size_t child) {
	if (header->size == capacity)
		return true;

	size_t pos = findInsertPosition(key), i;

	if (pos == header->size) goto insert; // append
		
	// make space for new entry
	for (i=header->size; i>pos; --i) {
		keys[i] = keys[i-1];
		children[i] = children[i-1];
	}

 insert:
	keys[pos] = key;
	children[pos] = child;
    
	header->size++;

	return header->size == capacity;
}

int IndexPage::split(IndexPage &new_page) {
	size_t midpt = header->size / 2, i;
	int split_key = keys[midpt];

	for (i=midpt+1; i < header->size; ++i) // midpt key goes to parent
		new_page.insert(keys[i], children[i]);

	new_page.header->size = header->size - (midpt+1);
	header->size = midpt;

	return split_key;
}
