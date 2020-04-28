LineairDB is a C++ fast transactional key-value storage library.
It provides transaction processing for multiple keys with guarantees of both **strict serializability (linearizability)** and **recoverability** (see [Correctness](#Correctness) to further details of these properties).
LineairDB provides some **novel concurrency-control protocol** that promise scalability for many-core CPUs machines, especially in (or even if) write-intensive and contended workloads (see [NWR](#NWR) to the detail of the protocols or [Benchmark Results](#Benchmark)).

#### Notes

- LineairDB is not an SQL (Relational) database.
- There is no client-server support in the library (i.e., LineairDB is an embedded database).

# Design {#Design}

LineairDB fully adopts and benefits from the **Epoch Framework** [[Tu13], [Chandramouli18]].
Briefly, the epoch framework divides the wall-clock time into _epochs_ and assign each epoch into each transaction.
(i.e., epoch framework groups running transactions).
LineairDB uses the Epoch Framework in the followings:

1.  Epoch-based group commit of transactions
2.  RCU-QSBR-like garbage collection of storage

The Epoch Framework provides advantages in both correctness and performance.

## Correctness {#Correctness}

LineairDB assumes that the correctness properties of DBMS consist of [strict serializability] and [recoverability].

The Epoch framework is favorable for both property:

- For strict serializability, the number of "concurrent" transactions can increase because transactions in the same epoch commit together at the same time. This nature gives us some possibility of performance optimization. The details are described later in [NWR](#NWR).
- For recoverability, we no longer have to worry about violating recoverability. Notably, epoch-based group commit has the constraint that all the logs of transactions in an epoch have to be persisted before the commit is returned. Hence, if a transaction reads a dirty value written by another running transaction, it does not commit before the writer commits because these two transactions are in the same epoch. This nature also enables [early lock release] of the exclusive lock.

#### Why "strict" serializability?

Why do we need "strict" serializability?
LineairDB is an embedded single-node database and thus is (not strict) serializability good enough?
Note that serializability theory permits to change orders among non-concurrent transactions.
For instance, a serializable database allows all transactions to read from and write into any arbitrary version.
More precisely, a transaction may always read the initial values, even if there exist some newer versions, and also can always write older versions than the initial values. Most users can not accept this behavior because
even if several years may have passed after the initialization of DBMS, not strictly serializable databases allow such unacceptable results.
Concurrency control protocols satisfying strict serializability _never_ change the order of transactions when they are not concurrent.

## Non-visible Write Rule (NWR) {#NWR}

LineairDB provides some novel extended concurrency control protocols, named _"NWR"_ (e.g., LineairDB provides Silo [[Tu13](http://db.csail.mit.edu/pubs/silo.pdf)] and its extended protocol, named SiloNWR).
NWR-protocols have great performance in write-intensive and contended workloads, that includes _"blind-writes"_.
The scalability of NWR extension is obtained by omitting unnecessary write operations.
Briefly, LineairDB **reorders concurrent transactions** and omits some write operations.
Remind that transactions in an epoch are grouped; the nature of epoch-based group commit is favorable for NWR;
Since transactions in the same epoch are committed at the same time, we can say that transactions in the same epoch are concurrent, and can be reordered.

NWR can omit blind write operations, which are insert or no-read update-only operations.
If you have blind write operations in your use case, choosing NWR-protocols is recommended strongly.
Because unnecessary write operations are omitted, LineairDB improves the processing speed of transactions dramatically.

The correctness of transaction processing in LineairDB is proved based on the multi-version serializability theory. See the research paper [at this link](https://arxiv.org/abs/1904.08119).

# Example

The following is a simple example code of how to use LineairDB.
Here we are not dealing with a multi-threaded environment; however, LineairDB is basically designed to be thread-safe.
That is, it is allowed to invoke LineairDB::Database::ExecuteTransaction by multiple threads in parallel.

\include example/example.cpp

# Roadmap

The LineairDB project roadmap is available at [Roadmap](docs/roadmap.md)

# Benchmark Results {#Benchmark}

The followings are our benchmark results.
This is a modified benchmark of [YCSB-A]; unlike official YCSB in which a transaction operates a single key, each transaction operates on four keys in our benchmark.

This benchmark is executed in the following environments:

|                              |                                                   |
| ---------------------------- | ------------------------------------------------- |
| CPU                          | four Intel Xeon E7-8870 (total 144 logical cores) |
| Memory                       | 1TB (no swap-out)                                 |
| YCSB Table size              | 100K                                              |
| YCSB Record size             | 8-bytes                                           |
| Epoch size                   | 1000ms                                            |
| Contention (θ)               | 0.9 (highly contended)                            |
| # of threads to process txns | 70                                                |

#### YCSB-A

<img src=./epoch1000.json.png width=400px/>

SiloNWR, our novel concurrency control protocol, achieves excellent performance by omitting transactions without exclusive lockings.
Note that YCSB-A has an operation ratio of Read 50% and (Blind) Write 50%; that is, this is a fairly favorable setting for NWR-protocols.
If your use case is such a blind write-intensive, then LineairDB can be a great solution.

<!-- References -->

[ycsb-a]: https://github.com/brianfrankcooper/YCSB
[strict serializability]: https://fauna.com/blog/serializability-vs-strict-serializability-the-dirty-secret-of-database-isolation-levels
[recoverability]: https://dl.acm.org/doi/abs/10.1145/42267.42272
[early lock release]: https://dl.acm.org/doi/pdf/10.14778/1920841.1920928
[tu13]: https://dl.acm.org/doi/10.1145/2517349.2522713
[chandramouli18]: https://www.microsoft.com/en-us/research/uploads/prod/2018/03/faster-sigmod18.pdf

\copyright
Copyright (c) 2020 Nippon Telegraph and Telephone Corporation.
Licensed under the Apache License, Version 2.0 (the "License").
