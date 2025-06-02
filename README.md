# RindDB

RindDB is a simple [embedded] key-value buffer database written in Go.

It uses a B+ tree for indexing and a value log file as the data store.

The main goal is to provide a cyclic cache that stores data on SSD disk and supports fast index access.

A typical use case is to temporarily store large (over 1 MiB) binary blobs for a limited time, with random key access.
In this case, disk size is used as a limit instead of TTL for records.

## Features:

- [x] B+ tree index
- [x] Value log file
- [x] Read and write metrics

### TODO:

- [ ] Add flush and load index from disk
- [ ] Restore vlog offsets on startup
- [ ] Add tests
- [ ] Add benchmarks
- [ ] Add example usage and documentation
- [ ] Read cache for recently accessed keys
- [ ] Build server and protocol for remote access (not embedded use case)
