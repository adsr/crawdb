# crawdb

crawdb is a toy file-based read-write key-value store. It is inspired by the
original "Arizona" key-value store, which itself was written at Etsy as an
alternative to sqlite for write-once read-only workloads.

Similar to Arizona, a crawdb has two files, an index file and a data file. The
index file consists of entries in the following format:

    <key> <offset> <len> <cksum>

Sorted entries appear first, followed by unsorted entries.

All fields are fixed length, so the sorted entries are searchable via a binary
search. When a key is found, the value for the key is read from the dat file
using the `<offset>` and `<len>` fields. The data is run through a checksum
algorithm (CRC-16) and compared to `<cksum>` to ensure data integrity.

If a key is not found via binary search, a reverse linear search is performed
as a fallback on the unsorted records at the end of the index file.

crawdb supports writes by appending unsorted entries at the end of the index
file.

A crawdb may be indexed (sorted) to make unsorted keys searchable via binary
search. Indexing can happen "online" and should only hold a lock briefly to copy
the old index and swap in the new index. Writes that occur during an index
operation are preserved as unsorted entries in the new index.

Locking for writes and indexing is accomplished via `flock(2)`.

I wrote this in one sitting and there are probably many bugs. Thorough testing
is a TODO.
