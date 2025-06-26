import struct
from pathlib import Path
from typing import Iterator

PAGE_SIZE = 4096
MAX_KEY_SIZE = 128
NODE_LEAF = 1
NODE_INTERNAL = 2
NODE_OVERFLOW = 3

class OnDiskBPlusTreeReader:
    def __init__(self, filepath: str):
        self.file = open(filepath, "rb")
        self.root_page_id = self._read_root_page_id()

    def close(self):
        self.file.close()

    def _read_page(self, page_id: int) -> bytes:
        self.file.seek(page_id * PAGE_SIZE)
        return self.file.read(PAGE_SIZE)

    def _read_root_page_id(self) -> int:
        self.file.seek(0)
        return struct.unpack(">Q", self.file.read(8))[0]

    def search(self, key: bytes) -> Iterator[bytes]:
        if len(key) > MAX_KEY_SIZE:
            raise ValueError("Key too long")
        overflow_page = self._find_overflow(self.root_page_id, key)
        return self._value_iterator(overflow_page) if overflow_page != -1 else iter(())

    def _find_overflow(self, page_id: int, key: bytes) -> int:
        page = self._read_page(page_id)
        node_type = page[0]
        count = struct.unpack(">I", page[1:5])[0]

        if node_type == NODE_LEAF:
            pos = 5
            for _ in range(count):
                klen = struct.unpack(">I", page[pos:pos+4])[0]
                pos += 4
                k = page[pos:pos+klen]
                pos += klen
                overflow = struct.unpack(">Q", page[pos:pos+8])[0]
                pos += 8
                if k == key:
                    return overflow
            return -1

        elif node_type == NODE_INTERNAL:
            pos = 5
            leftmost = struct.unpack(">Q", page[pos:pos+8])[0]
            pos += 8
            for _ in range(count):
                klen = struct.unpack(">I", page[pos:pos+4])[0]
                pos += 4
                k = page[pos:pos+klen]
                pos += klen
                right = struct.unpack(">Q", page[pos:pos+8])[0]
                pos += 8
                if key < k:
                    return self._find_overflow(leftmost, key)
                leftmost = right
            return self._find_overflow(leftmost, key)

        else:
            raise ValueError("Invalid page type")

    def _value_iterator(self, page_id: int) -> Iterator[bytes]:
        while page_id != -1:
            page = self._read_page(page_id)
            count = struct.unpack(">I", page[1:5])[0]
            pos = 5
            for _ in range(count):
                vlen = struct.unpack(">I", page[pos:pos+4])[0]
                pos += 4
                yield page[pos:pos+vlen]
                pos += vlen
            page_id = struct.unpack(">Q", page[PAGE_SIZE-8:])[0]
