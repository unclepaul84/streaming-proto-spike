# streaming-proto-file-spike
Platform/language neutral Protocol Buffer based file format specification which is stream writable/readable.

### Problem Statement
Protocol Buffers are [not designed](https://protobuf.dev/programming-guides/techniques/#large-data) for parsing large messages.
### Solution
 This file format specification allows to define custom file formats based on length-prefixed serialized protobuf payloads.

### File Byte Layout
* All integers are 4 byte, Big Endian encoded
* Magic Byte must be constant 0x1973

```mermaid

---
title: "File Byte Layout"
---
packet-beta
0-4: "Magic Byte(Int32)[0x1973]"
5-9: "Header Length (Int32)"
10-19: "Header content (variable)"
20-24: "Payload length (Int32)"
25-40: "Payload (variable)"
41-45: "Payload length (Int32)"
46-63: "Payload (variable)"
```

### Supported Languages
Any language which supports reading/writing files as stream and for which protobuf bindings can be generated.

#### Tested Languages
* Java
* Python