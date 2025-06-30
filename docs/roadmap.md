# The LineairDB project roadmap

### Goals

#### Support log-replication and online recovery

LineairDB is the embedded database and is intended to work as a part of storage engine of other DBMSs, so this library does not support distributed transaction.
However, online recovery is a helpful feature since it enables multiple LineairDB instances to synchronize the recorded information.
This is a desirable feature in a mobile environment.

#### Support setting of the upper bound (limit) of the response time of callback functions.

The current LineairDB executes the returns of commit callbacks an afterthought when the jobs in the thread pool is clogged.
In other words, LineairDB::Database::ExecuteTransaction is lock-free but is not starvation-free.
It is no worse in terms of cache efficiency, but it is better to forbid that a commit does not come back (eventually) forever.

#### Support CLI.

#### Read-only Multiversion (ROMV)

If a user gives a hint, like a read-only flag, to a transaction, it can be executed without coordination, such as locking or validation, while preserving its correctness. 

### Next Release

- Additional concurrency control protocols (MVTO, MVTO+NWR)
- TPC-C benchmark
- Secondary Index
- Multiple Table
