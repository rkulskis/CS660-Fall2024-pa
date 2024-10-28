# Design decisions


# Missing/incomplete elements of code
B-tree insert isn't complete, we ran out of time.

# Challenges Faced:

Some of the challenges we faced include:

1) Using recursion to iteratively split nodes in a tree when a particular IndexPage, or LeafPage gets full.
2) Working with iterators. E.g. for leaf and index page Ross just implemented his
own binary search after struggling to use std::lower_bound since this requires
the creation of an iterator.

# Design descisions:
We added last\_child\_index to IndexPageHeader since there is 1 more child slot than key slot (i.e. for < or >=). This is for use with the BTreeFile.cpp traversal.

Rather than using memmove or other functions for shifting for insertion, we did
simple array indexing to allow for more clarity of code and readability.

For LeafPage we implemented some helper functions (e.g. copyTuple(), findInsertPosition(), this is the binary search) to allow for better code legibility and
coherence.

# Time spent
Ross spent 4 hours on the following files: LeafPage.cpp and IndexPage.cpp.
Arun spent 4 hours on the writeup and on the Btree.cpp File.

# Collaboration
Arun Ramana Balasubramaniam (BU ID: U93836083)
Ross Mikulskis (BU ID: U25561029)
