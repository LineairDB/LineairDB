# The LineairDB project roadmap

### Goals

#### Support log-replication and online recovery

LineairDB is the embedded database and is intended to work as a part of storage engine of other DBMSs, so this library does not support distributed transaction.
However, online recovery is a helpful feature since it enables multiple LineairDB instances to synchronize the recorded information.
This is a desirable feature in a mobile environment.

#### Support range index and scan query.

The current LineairDB has only a point index, and users cannot use range query.
This feature is very difficult to implement in a naïve manner; there are two major problems.
First, replacing a simple hash table with a concurrent and indexed data structure, such as concurrent skiplist or masstree, will likely result in poor performance.
Next, scan query raises Index Anomalies (e.g. phantom).
Several methods have already been proposed to prevent this anomalies, but the best one in terms of performance has to be discussed more and more.

#### Support setting of the upper bound (limit) of the response time of callback functions.

The current LineairDB executes the returns of commit callbacks an afterthought when the jobs in the thread pool is clogged.
In other words, LineairDB::Database::ExecuteTransaction is lock-free but is not starvation-free.
It is no worse in terms of cache efficiency, but it is better to forbid that a commit does not come back (eventually) forever.

#### Support CLI.

### Next Release

- Checkpointing
- Additional concurrency control protocols (MVTO, MVTO+NWR)
- TPC-C benchmark
