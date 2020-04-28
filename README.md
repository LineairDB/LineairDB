## LineairDB

<p>
  <img alt="Version" src="https://img.shields.io/badge/version-0.1.0-blue.svg?cacheSeconds=2592000" />
  <a href="#Documentation" target="_blank">
    <img alt="Documentation" src="https://img.shields.io/badge/documentation-yes-brightgreen.svg" />
  </a>
  <a href="https://www.apache.org/licenses/LICENSE-2.0" target="_blank">
    <img alt="License: Apache--2.0" src="https://img.shields.io/badge/License-Apache--2.0-yellow.svg" />
  </a>
</p>

**LineairDB is a fast transactional key-value storage library. It provides transaction processing for multiple keys with strict serializability.**

### Features

---

- Keys and values are arbitrary byte arrays.
- The basic operations are read(key), write(key, value), ExecuteTransaction(procedure, callback).
- Changes in a transaction for multiple key-value pairs are made with atomicity and durability.
- Concurrent transactions are processed with strict serializability.
- In contended write-heavy workloads, high scalability for many-core CPUs is provided.

### Notes

---

- LineairDB is not an SQL (Relational) database.
- There is no client-server support in the library (i.e., LineairDB is an embedded database).

### Usage

```c++

#include <lineairdb/lineairdb.h>

int main() {
  {
    LineairDB::Database db;
    LineairDB::TxStatus status;

    // Execute: enqueue a transaction with an expected callback
    db.ExecuteTransaction(
        [](LineairDB::Transaction& tx) {
          auto alice = tx.Read<int>("alice");
          if (alice.has_value()) {
            int alice = alice.value();
          }
          tx.Write<int>("bob", 1);
        },
        [&](LineairDB::TxStatus s) { status = s; });

    // Fence: Block-wait until all running transactions are terminated
    db.Fence();
    // status == LineairDB::TxStatus::Committed;
  }
}
```

### Getting the Source

```
git clone --recurse-submodules https://github.com/lineairdb/lineairdb.git
```

### Building

Quick start:

```
mkdir -p build && cd build
cmake -DBUILD_SHARED_LIBS=ON -DCMAKE_BUILD_TYPE=Release .. && make && sudo make install
```

Then you can use LineairDB by including the header `include/lineairdb/lineairdb.h`.

### Compatibility

We have been tested LineairDB in the following environments:

- Apple clang version 11.0.3
- Clang 6 and 9 on Linux

### Documentation

---

You can get the LineairDB library documentation by simply running

```
doxygen
```

and open `docs/build/html/index.html`.

Technical detail of concurrency control protocols such as SiloNWR is also available in the research paper [at this link](https://arxiv.org/abs/1904.08119).
The project roadmap is [here](./docs/roadmap.md).

### Contributing

This project welcomes contributions, issues, suggestions, and feature requests.
<br />Feel free to check [issues page](/issues).

### Question & Discussion

If you have any questions, please feel free to ask on slack.
[Join to slack](https://join.slack.com/t/lineairdb/shared_invite/zt-dvf52aoi-45skLlXcdi7IuQcIM8ARKw)
