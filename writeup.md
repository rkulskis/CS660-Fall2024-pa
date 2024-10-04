# Design decisions

# Missing/incomplete elements of code
Nothing is missing for this assignment and it passes all the test cases

# Analytical questions
Deleting tuples from a HeapFile might lead to having empty pages. How can we avoid having empty pages in a file? What is the cost of the solution you propose?

A: We can avoid having empty pages in a file by keeping track of empty pages or partially filled pages using a list. We can use this list to fill up the empty or partially filled pages before adding a new page. The cost of this solution includes additional memory for keeping track of pages as well as increased complexity in terms of updating of the list when pages are inserted or deleted.

In this assignment we have fixed size fields. How can we support variable size fields (e.g. VARCHAR)?

A: We can store the VARCHAR data in a separate region (heap / overflow pages) and store the pointer in the tuple, thereby supporting variable size fields.


# Time spent
Ross spent 4 hours on code, namely tuple, dbFile, and heappage implementation.
Arun spent 2 hours on the writeup and analytical questions and heapfile implementation.

# Collaboration
Arun Ramana Balasubramaniam (BU ID: U93836083)
Ross Mikulskis (BU ID: U25561029)

