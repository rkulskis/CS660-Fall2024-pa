# Design decisions


# Missing/incomplete elements of code
Nothing is missing for this assignment and it passes all the test cases

# Analytical Questions


1) Maintaining an index of a file can slow down insertions and deletions as the index needs to be updated. However, it can speed up lookups. What are some strategies to minimize the impact of index maintenance on bulk insertions and deletions? What do you need to change in the implementation to support these strategies?

A. Bulk loading into the B+ trees by sorting the entries first and then constructing the B+ tree as well as using a buffering mechanism which periodically flushes data in a sorted manner into the B+ tree can help minimize the impact of index maintenance on bulk insertions.
The effects of bulk deletions can be minimized by marking the nodes that are to be deleted and deleting them at one go when a certain threshold is reached, rather than deleting them one at a time.

2) A common workload for database tables is to insert new entries with an auto-incrementing key. How can you optimize the BTreeFile for this workload?

A. We can cache the rightmost leaf node because as the key auto increments, the rightmost leaf get populated, therefore by caching the values, we can reduce disk I/O. We can also delay the splitting of the right nodes, which will reduce the number of time we need to balance the tree, thereby making it more optimal.

3) A common strategy employed in production systems is to maintain the internal nodes of indexes to always exist in the bufferpool (or rather, pin them to memory). Discuss why this is a good idea and if there are any implications of this strategy.

A. By maintaining the internal nodes of indexes to always exist in the bufferpool, we can see an drastic reduction in disk I/O which reduces latency, we also see a improved search performance as we can quickly traverse through the b+ tree, we can also allow for bulk loading and bulk deletion and therefore this strategy is extremely beneficial.


# Challenges Faced:

Some of the challenges we faced include:

1) Using recursion to iteratively split nodes in a tree when a particular IndexPage, or LeafPage gets full.

# Time spent
Ross spent 4 hours on the following files: LeafPage.cpp and IndexPage.cpp.
Arun spent 4 hours on the writeup and on the Btree.cpp File.

# Collaboration
Arun Ramana Balasubramaniam (BU ID: U93836083)
Ross Mikulskis (BU ID: U25561029)