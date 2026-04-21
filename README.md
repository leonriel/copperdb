# CopperDB

CopperDB is a persistent key-value store built in Rust, powered by an
LSM-tree storage engine. It exposes a simple HTTP API for reading, writing,
and deleting keys.

Internally, writes are first appended to a write-ahead log (WAL) for
durability, then inserted into an in-memory memtable backed by a concurrent
skip list. When a memtable is full it is frozen and flushed to disk as a
sorted string table (SSTable). A background compaction worker merges
SSTables across levels to reclaim space and keep read performance stable.

## Running the server

```sh
cargo run
```

By default the server listens on `127.0.0.1:8080` and stores data in
`./copperdb-data`. Both are configurable via environment variables:

```sh
COPPERDB_DIR=/tmp/mydata COPPERDB_ADDR=0.0.0.0:9000 cargo run
```

Press Ctrl-C to shut down gracefully.

## API

All values are treated as raw bytes. The key is part of the URL path.

### Health check

```
GET /health
```

Returns `200 OK` with body `ok`.

### Put a key

```
PUT /kv/{key}
```

The request body becomes the value. Returns `204 No Content` on success.

```sh
curl -X PUT http://localhost:8080/kv/city -d 'london'
```

### Get a key

```
GET /kv/{key}
```

Returns `200 OK` with the value as the response body, or `404 Not Found` if
the key does not exist.

```sh
curl http://localhost:8080/kv/city
```

### Delete a key

```
DELETE /kv/{key}
```

Returns `204 No Content`. Deleting a non-existent key is a no-op.

```sh
curl -X DELETE http://localhost:8080/kv/city
```

## Running tests

```sh
cargo test
```
