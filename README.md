# Reading and Writing very large Protocol Buffers encoded files
Platform/language neutral Protocol Buffer based file format specification designed to be stream writable/readable.

### Problem Statement
Protocol Buffer library is [not designed](https://protobuf.dev/programming-guides/techniques/#large-data) for parsing large messages.

### Solution
 File format specification which allows to define custom dataset file formats based on length-prefixed, protobuf serialized payloads. This allows files to be read/written in a streaming fasion, thus not requiring to hold the entire dataset in physical memory of the process.

### File Byte Layout
* All integers are 4 byte, Signed, Big Endian encoded
* Magic Byte must be constant 0x1973
* Header and Payload are opaque, protobuf serialized byte arrays

```mermaid

---
title: "File Byte Layout"
---
packet-beta
0-3: "Magic Byte(Int32)"
4-8: "Header Length (Int32)"
9-18: "Header content (variable)"
19-23: "Payload length (Int32)"
24-39: "Payload (variable)"
40-44: "Payload length (Int32)"
45-62: "Payload (variable)"
63-67: "File Seal (Int32)"
```

---

## Indexing Support

This format supports efficient random access to records using a B+Tree index file. The index maps keys (such as entity names) to payload offsets in the data file. This enables fast lookups without scanning the entire file.

- **Index File**: A separate file (e.g., `name.index`) stores a B+Tree mapping keys to payload offsets.
- **Random Access**: Use the index to retrieve the offset, then seek directly to the payload in the data file.
- **Multi-language**: Both Java and Python implementations are provided for reading and searching the index.

See:
- [Java OnDiskBPlusTree usage](app/src/main/java/org/example/App.java)
- [Python OnDiskBPlusTree usage](python-app/readBTree.py)
---

## S3 Remote Read Support

The format and libraries support reading both data and index files directly from Amazon S3 using efficient range requests.

- **S3 Range Reads**: Only the required bytes are fetched from S3, minimizing bandwidth and memory usage.
- **Streaming**: Both header and payloads can be read without downloading the full file.
- **Index and Data**: Both the index and data files can be accessed remotely.

See:
- [Java S3StreamableProtoFileParser](app/src/main/java/org/example/S3StreamableProtoFileParser.java)
- [Python StreamableProtoFileS3Parser](python-app/proto_streamed_file/StreamableProtoFileS3Parser.py)

---

### Supported Languages
Any language which supports reading/writing files as a stream and for which protobuf bindings can be generated.

#### Tested Languages
* Java
* Python

#### Files to look at

* https://github.com/unclepaul84/streming-proto-spike/blob/main/app/src/main/java/org/example/App.java
* https://github.com/unclepaul84/streming-proto-spike/blob/main/python-app/write.py


