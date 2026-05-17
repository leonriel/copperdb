# CopperDB

CopperDB is a persistent key-value store built in Rust, powered by an
LSM-tree storage engine. It exposes a simple HTTP API for reading, writing,
deleting, and range-scanning keys.

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

### Range scan

```
GET /kv?start=<key>&end=<key>&limit=<N>
```

Returns up to `limit` key-value pairs whose keys fall in the half-open
range `[start, end)`, in ascending key order. All three query parameters
are optional:

| Parameter | Default     | Meaning                                |
|-----------|-------------|----------------------------------------|
| `start`   | unbounded   | Inclusive lower bound on the key range |
| `end`     | unbounded   | Exclusive upper bound                  |
| `limit`   | `1000`      | Maximum results returned               |

The response is JSON. Values are base64-encoded because the engine stores
arbitrary bytes and JSON has no native binary type:

```json
{
  "results": [
    {"key": "apple",  "value": "cmVk"},
    {"key": "banana", "value": "eWVsbG93"}
  ]
}
```

Returns `200 OK` for any well-formed request (including empty results),
`400 Bad Request` if a query parameter fails to parse, or `500 Internal
Server Error` if the engine surfaces an I/O error.

```sh
# Full scan (capped at 1000 entries).
curl http://localhost:8080/kv

# Bounded scan: keys in [banana, fig), at most 50 results.
curl 'http://localhost:8080/kv?start=banana&end=fig&limit=50'

# Decode a value back to raw bytes.
curl 'http://localhost:8080/kv?limit=1' | jq -r '.results[0].value' | base64 -d
```

Note the encoding asymmetry: single-key `GET /kv/{key}` returns raw bytes
as the response body, whereas the scan endpoint base64-encodes each value
inside a JSON wrapper. The scan needs a serialisation format that can pack
multiple values into one response; JSON forces an encoding step that the
single-key endpoint doesn't have to make.

## Running tests

```sh
cargo test
```
