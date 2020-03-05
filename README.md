automerge-go
--------------------------

`automerge-go` is an experiment in Go of implementing columnar automerge as described by Martin Kleppman ([columnar experiment writeup](https://github.com/automerge/automerge-perf/tree/master/columnar)).

*** THIS NON-FUNCTIONAL CODE ***

While the code will be functional at some point, the current intent of the code is to understand the performance characteristics of columnar automerge.

# Current Performance

Run from mac pro desktop.

```text
=== RUN   TestPerformance
applying edits ...
 25000:  62388 bytes, 3.1 µs/op
 50000: 126464 bytes, 3.1 µs/op
 75000: 189055 bytes, 4.2 µs/op
100000: 252513 bytes, 3.5 µs/op
125000: 314784 bytes, 2.9 µs/op
150000: 376359 bytes, 7.2 µs/op
175000: 439171 bytes, 3.5 µs/op
200000: 502771 bytes, 6.7 µs/op
225000: 569985 bytes, 7.5 µs/op
250000: 634307 bytes, 4.1 µs/op

edits -> 259778
bytes -> 659432
pages -> 5056
--- PASS: TestPerformance (1.52s)
``` 



# General Concepts

`automerge-go` organizes automerge operations into a series of pages. Each page currently consists of 6 fields, with data stored in columnar format:

* `op_counter` - together with `op_actor` form the lamport timestamp of the current operation (int64)
* `op_actor` - together with `op_id` form the lamport timestamp of the current operation ([]byte)
* `ref_counter` - counter for reference lamport timestamp (int64)
* `ref_actor` - actor for reference lamport timestamp ([]byte)
* `op_type` - identifies the operation to be performed (int64)
* `value` - the data associated with the operation (varies)

Each field is encoded using one of following encodings which are borrowed heavily from parquet:

* `rle` - run length encoding int64 
* `delta_rle` - delta encoding on top of run length encoding
* `dictionary_rle` - encodes []byte to an int64 and stores the int64 using run length encoding. Similar to parquet's DICTIONARY_RLE
* `plain` - records are stored in adjancent series based on parquet's PLAIN format

## Encodings

### rle

`rle` provides a simple run length encoding for int64 values.  Each sequence of int64 is stored as a pair of values, the repetition count along with the value.

For example, the sequence `[1]` would be stored as `[1,1]` which you can think of as 1 repetition of 1.  The sequence `[1,1,1]` would be stored as `[3,1]`; 3 repetitions of 1.  And sequence like `[1,1,1,2,2,1,1,1,1]` would be stored as `[3,1,2,2,4,1]`.

### delta_rle

`delta_rle` sits atop `rle` and first encodes int64 values as a series of deltas.  

For example, with the sequence, `[1,2,3]`, we could first delta encode this to `[1,1,1]` which can be though of as start with 1, add 1 to get to the second value, 2, and add 1 to that to get to the third value, 3. `[1,1,1]` can then be further reduced using `rle` to `[3,1]`.

As the changes of repetition get longer, `delta_rle` compression gets better.  For example, a sequence from 1 to 100 would encode as `[100,1]` or just two bytes.

### plain

`plain` encoding stores data in series, one record after the next.  `plain` currently supports the following logical data types:

* []byte
* string
* rune (utf8 character)
* int64
* key value pair

Underneath the logical data type is a raw data type, currently only varint and byte array.

### dictionary_rle

`dictionary_rle` combines `plain` and `rle` encodings to store []byte values.  `dictionary_rle` first encodes the []byte using `plain` encoding, ensuring there are no duplicates.  `dictionary_rle` then takes the order index from `plain` and uses that as an int64 which is then encoded using `rle`

## Fields

### op_counter, op_actor

Although stored as separate columns, these two fields together form the [lamport timestamp](https://en.wikipedia.org/wiki/Lamport_timestamps) of the operation.

`op_counter` is encoded using `delta_rle` to take advantage of the fact that sequences of text characters will be simple sequences e.g.

`op_actor` is encoded using `dictionary_rle`.  The expectation is that for any given page, there will likely be far fewer distinct `op_actor` values than there will be operations in the page.

### ref_counter, ref_actor

Also stored as separate columns, the two fields identify the causal reference for the operation.

`ref_counter` is encoded using `delta_rle`

`ref_actor` is encoded using `dictionary_rle`

To indicate start of document, the `ref_counter`, `ref_actor` pair of `0`, `nil` should be used.

### op_type

`op_type` identifies the operation to be performed.  The assumption is that each operation can be encoded into an int64 and that readers are responsible for interpreting the results.

`op_type` is encoded using `rle`.

### value

`value` contains the data associated with the operation and may store multiple types of data.  

`value` is encoded using `plain` encoding.

## Page

A `page` is a collection of operations.  To keep pages from growing too large (and slowing down the app), pages may be split into smaller pages.

Each page maintains a bloom filter of all the op_ids contained within the page to avoid users having to search all the records to find a given element.

## Node

A `node` (naming?) represents a collection of pages that share common logical and raw types.  The current intent is to allow logical portions of a document like automerge `Text` and `Table` elements to be stored under a single node.  May need to revisit this idea later.

When pages get too large, the node will split the pages into multiple smaller pages for performance reasons.

## Document

A `document` represents the top level entity that the user will interact with.  Documents contain a set of nodes.

