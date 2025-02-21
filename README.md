# streaming-proto-file-spike
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
0-4: "Magic Byte(Int32)[0x1973]"
5-9: "Header Length (Int32)"
10-31: "Header content (variable)"
32-36: "Payload length (Int32)"
37-63: "Payload (variable)"
64-68: "Payload length (Int32)"
69-95: "Payload (variable)"
```

### Supported Languages
Any language which supports reading/writing files as a stream and for which protobuf bindings can be generated.

#### Tested Languages
* Java
* Python

#### Files to look at

* https://github.com/unclepaul84/streming-proto-spike/blob/main/app/src/main/java/org/example/App.java
* https://github.com/unclepaul84/streming-proto-spike/blob/main/python-app/write.py


