# SSTable Binary Layout

## SSTable File

An SSTable file is composed of four regions written sequentially:

```
+--------------------+
|   Data Block 0     |
+--------------------+
|   Data Block 1     |
+--------------------+
|       ...          |
+--------------------+
|   Data Block N     |
+--------------------+
|   Meta Block       |
+--------------------+
|   Index Block      |
+--------------------+
|   Footer (24 B)    |
+--------------------+
```

### Data Blocks

Each data block holds a sorted run of key-record entries and targets 4 KB
(`TARGET_BLOCK_SIZE = 4096`). A block that would exceed 4 KB after adding an
entry is flushed and a new block is started. The first entry in a block is
always accepted regardless of size, so a single large record can produce an
oversized block.

Entries are written in the same order they arrive from the memtable iterator:
`user_key` ascending, then `seq_num` descending (newest version first).

### Meta Block (Bloom Filter)

The meta block contains a serialized bloom filter (via `bloomfilter::Bloom::to_bytes()`)
that tracks every `user_key` written to the SSTable. On read, the bloom filter
is loaded into memory by `SsTableReader::open` and checked before any data
block I/O in `search`. If the bloom filter reports that a key is absent, the
search short-circuits with `None`, avoiding disk reads entirely.

The bloom filter is configured with a 1% false-positive rate and sized for an
estimated 10,000 keys per SSTable. Under these parameters the bitmap is
11,982 bytes plus a 44-byte crate header, totalling approximately 12 KB per
SSTable.

### Index Block

The index block has the same binary format as a data block (see below), but
its entries map a block's first `user_key` to that block's byte offset in the
file. Each index entry is:

- **Key**: the first `user_key` of the data block (with `seq_num = 0`).
- **Value**: the data block's starting byte offset as a big-endian `u64` (8 bytes),
  stored as a `Record::Put`.

The index block is currently limited to a single block. If the index exceeds
`TARGET_BLOCK_SIZE`, the writer returns an error.

### Footer (24 bytes)

| Field         | Type        | Size    | Description                              |
|---------------|-------------|---------|------------------------------------------|
| Meta Offset   | `u64` (BE)  | 8 bytes | Byte offset where the meta block starts  |
| Index Offset  | `u64` (BE)  | 8 bytes | Byte offset where the index block starts |
| Magic Number  | `u64` (BE)  | 8 bytes | `0xDEADBEEFCAFEBABE` for validation      |

---

## Block

All blocks (data and index) share the same binary format:

```
+---------------------------+
|   Entry 0                 |
+---------------------------+
|   Entry 1                 |
+---------------------------+
|       ...                 |
+---------------------------+
|   Entry N                 |
+---------------------------+
|   Offset 0   (u16 BE)     |
|   Offset 1   (u16 BE)     |
|       ...                 |
|   Offset N   (u16 BE)     |
+---------------------------+
|   Num Offsets (u16 BE)    |
+---------------------------+
```

### Block Footer

The last 2 bytes of a block store the number of entries (`num_offsets`) as a
big-endian `u16`. Immediately before that is an array of `num_offsets`
big-endian `u16` values, each pointing to the byte offset of the corresponding
entry within the block's data section.

### Entry Layout

Each entry is a serialized `InternalKey` + `Record`:

**Put entry:**

| Field        | Type        | Size              |
|--------------|-------------|-------------------|
| Key Length   | `u16` (BE)  | 2 bytes           |
| User Key     | UTF-8 bytes | `key_length` bytes|
| Seq Num      | `u64` (BE)  | 8 bytes           |
| Record Tag   | `u8`        | 1 byte (`1`)      |
| Value Length  | `u32` (BE)  | 4 bytes           |
| Value        | raw bytes   | `value_length` bytes |

**Delete entry (tombstone):**

| Field        | Type        | Size              |
|--------------|-------------|-------------------|
| Key Length   | `u16` (BE)  | 2 bytes           |
| User Key     | UTF-8 bytes | `key_length` bytes|
| Seq Num      | `u64` (BE)  | 8 bytes           |
| Record Tag   | `u8`        | 1 byte (`0`)      |

Delete entries have no value length or value fields.
